"""Minimal GitHub REST client (stdlib) for alerts and release listings.

Uses GITHUB_TOKEN (Actions) or GH_TOKEN. Without a token, alert functions print
to stderr instead, so local runs never fail because GitHub is unreachable.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"


def repo() -> str:
    return os.environ.get("GITHUB_REPOSITORY", "amaya507/sitr-panama-archive")


def token() -> str | None:
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")


def request(method: str, path: str, data: dict | None = None, *, auth: bool = True):
    url = path if path.startswith("http") else f"{API}{path}"
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
               "User-Agent": "sitr-panama-archive"}
    if auth and token():
        headers["Authorization"] = f"Bearer {token()}"
    body = json.dumps(data).encode() if data is not None else None
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def list_releases(max_pages: int = 3) -> list[dict]:
    out = []
    for page in range(1, max_pages + 1):
        batch = request("GET", f"/repos/{repo()}/releases?per_page=100&page={page}")
        out += batch
        if len(batch) < 100:
            break
    return out


def release_assets(release: dict) -> list[dict]:
    """All assets of a release (the embedded list is capped at 100? paginate)."""
    assets = []
    page = 1
    while True:
        batch = request("GET", f"/repos/{repo()}/releases/{release['id']}/assets?per_page=100&page={page}")
        assets += batch
        if len(batch) < 100:
            return assets
        page += 1


# --------------------------------------------------------------------------
# Alerts = GitHub issues. GitHub emails the repo owner on every new issue and
# comment, which is the notification channel. One open issue per alert key.
# --------------------------------------------------------------------------
def _marker(key: str) -> str:
    return f"[sitr-alert:{key}]"


def _find_open(key: str) -> dict | None:
    marker = _marker(key)
    page = 1
    while True:
        issues = request("GET", f"/repos/{repo()}/issues?state=open&per_page=100&page={page}")
        for it in issues:
            if "pull_request" not in it and marker in it.get("title", ""):
                return it
        if len(issues) < 100:
            return None
        page += 1


def raise_alert(key: str, title: str, body: str, *, repeat_after_h: float = 12) -> None:
    run = os.environ.get("GITHUB_RUN_ID")
    if run:
        body += f"\n\nRun: https://github.com/{repo()}/actions/runs/{run}"
    print(f"::error title=ALERT {key}::{title}", file=sys.stderr, flush=True)
    if not token():
        print(f"ALERT (no GITHUB_TOKEN, not filed): {title}\n{body}", file=sys.stderr, flush=True)
        return
    try:
        existing = _find_open(key)
        if existing is None:
            request("POST", f"/repos/{repo()}/issues", {"title": f"{_marker(key)} {title}", "body": body})
            return
        updated = datetime.fromisoformat(existing["updated_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - updated > timedelta(hours=repeat_after_h):
            request("POST", f"/repos/{repo()}/issues/{existing['number']}/comments",
                    {"body": f"Still happening.\n\n{body}"})
    except Exception as e:  # noqa: BLE001 - alerting must never crash the caller
        print(f"::warning::could not file alert {key}: {e}", file=sys.stderr, flush=True)


def resolve_alert(key: str, note: str) -> None:
    if not token():
        return
    try:
        existing = _find_open(key)
        if existing is None:
            return
        n = existing["number"]
        request("POST", f"/repos/{repo()}/issues/{n}/comments", {"body": f"Resolved automatically: {note}"})
        request("PATCH", f"/repos/{repo()}/issues/{n}", {"state": "closed", "state_reason": "completed"})
    except Exception as e:  # noqa: BLE001
        print(f"::warning::could not resolve alert {key}: {e}", file=sys.stderr, flush=True)
