"""Polite HTTP client: stdlib only, so the 5-minute job needs no pip install.

One request per endpoint per cycle; retries only on transient failures, with
exponential backoff and jitter; honours Retry-After; identifies itself.
"""
from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime

from . import config
from .timeutil import now_utc

RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
KEEP_HEADERS = ("last-modified", "etag", "content-type", "cf-ray", "cf-cache-status", "retry-after", "server")


@dataclass
class FetchResult:
    name: str
    url: str
    status: int | None = None
    body: bytes | None = None
    error: str | None = None
    attempts: int = 0
    fetched_at_utc: datetime | None = None
    elapsed_s: float = 0.0
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == 200 and self.body is not None and self.error is None

    def meta(self) -> dict:
        snippet = None
        if self.body is not None and not self.ok:
            snippet = self.body[:300].decode("utf-8", "replace")
        return {
            "url": self.url,
            "status": self.status,
            "ok": self.ok,
            "error": self.error,
            "attempts": self.attempts,
            "fetched_at_utc": self.fetched_at_utc.isoformat() if self.fetched_at_utc else None,
            "elapsed_s": round(self.elapsed_s, 3),
            "bytes": len(self.body) if self.body is not None else 0,
            "headers": self.headers,
            "body_snippet": snippet,
        }


def endpoint_url(name: str) -> str:
    return f"{config.BASE_URL}{name}.json"


def fetch(
    name: str,
    *,
    attempts: int = config.HTTP_ATTEMPTS,
    timeout: float = config.HTTP_TIMEOUT_S,
    backoff_base: float = config.HTTP_BACKOFF_BASE_S,
    validate_json: bool = True,
    deadline: float = config.HTTP_DEADLINE_S,
    sleep=time.sleep,
    opener=urllib.request.urlopen,
) -> FetchResult:
    url = endpoint_url(name)
    res = FetchResult(name=name, url=url)
    req = urllib.request.Request(
        url, headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"}
    )
    for attempt in range(1, attempts + 1):
        res.attempts = attempt
        res.error = None
        retry_after = None
        t0 = time.monotonic()
        res.fetched_at_utc = now_utc()
        out = _with_deadline(lambda: _attempt(opener, req, timeout), deadline)
        if out is None:  # hung (DNS, stalled or trickling connection): treat as a timeout
            res.status, res.headers, res.body, res.error = None, {}, None, f"deadline exceeded ({deadline:.0f}s)"
        else:
            res.status, res.headers, res.body, res.error, retry_after = out
        res.elapsed_s = time.monotonic() - t0

        if res.status == 200 and res.body is not None and validate_json:
            try:
                json.loads(res.body.decode("utf-8-sig"))
            except Exception as e:  # noqa: BLE001 - truncated/mid-write file: retry
                res.error = f"invalid JSON: {type(e).__name__}: {e}"

        if res.error is None:
            return res
        transient = res.status is None or res.status in RETRY_STATUSES or res.status == 200
        if not transient or attempt == attempts:
            return res
        delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
        if retry_after is not None:
            delay = max(delay, min(retry_after, 120))
        sleep(delay)
    return res


def _attempt(opener, req, timeout):
    """One HTTP attempt -> (status, headers, body, error, retry_after)."""
    try:
        with opener(req, timeout=timeout) as resp:
            headers = {k: v for k, v in resp.headers.items() if k.lower() in KEEP_HEADERS}
            return resp.status, headers, resp.read(), None, None
    except urllib.error.HTTPError as e:
        headers = {k: v for k, v in (e.headers or {}).items() if k.lower() in KEEP_HEADERS}
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            body = None
        return e.code, headers, body, f"HTTP {e.code}", _retry_after(e.headers)
    except Exception as e:  # noqa: BLE001 - URLError, timeouts, resets
        return None, {}, None, f"{type(e).__name__}: {e}", None


def _with_deadline(fn, seconds: float):
    """Run fn in a daemon thread; return its result, or None if it is still
    running after `seconds`. urllib's timeout bounds each socket operation, not
    the whole request: a stalled DNS lookup or a server trickling bytes can hang
    far longer (a fetch hung >10 min on 2026-10-01). The abandoned thread is a
    daemon, so it never blocks process exit."""
    box: list = []
    t = threading.Thread(target=lambda: box.append(fn()), daemon=True)
    t.start()
    t.join(seconds)
    return box[0] if box else None


def _retry_after(headers) -> float | None:
    if not headers:
        return None
    value = headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None
