"""Raw SITR JSON -> long-format rows + drift events.

Design rules:
* Never raise out of an endpoint parser. Every problem becomes a DriftEvent and
  the rest of the file (and every other endpoint) is still parsed.
* Unknown fields inside known records are kept automatically (they become new
  `field` values) and reported. Unknown top-level lists of records are parsed
  generically and reported.
* Nothing wide is produced: one row per (timestamp, stream, entity, field).
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from . import config
from .timeutil import local_to_utc, parse_scada_ts, parse_spanish_ts, trend_label, utc_to_local

_NUM_RE = re.compile(r"^[+-]?(\d{1,3}(,\d{3})+|\d+)?(\.\d+)?$")
_PIE_RE = re.compile(r"^\s*(.*?)\s+(-?[\d.,]+)\s*\(\s*(-?[\d.,]+)\s*%\s*\)\s*$")


@dataclass(frozen=True)
class DriftEvent:
    endpoint: str
    path: str
    kind: str      # missing_key | new_key | missing_field | new_field | bad_value | parse_error | ...
    detail: str
    severity: str  # ERROR | WARN

    def as_dict(self) -> dict:
        return asdict(self)


def to_value(raw) -> tuple[float | None, str | None]:
    """Return (numeric value, original text if it is not a plain number)."""
    if raw is None:
        return None, None
    if isinstance(raw, bool):
        return None, str(raw)
    if isinstance(raw, (int, float)):
        return float(raw), None
    if not isinstance(raw, str):
        return None, json.dumps(raw, ensure_ascii=False)
    s = raw.strip()
    if s and _NUM_RE.match(s) and any(c.isdigit() for c in s):
        num = float(s.replace(",", ""))
        return num, (None if "," not in s else raw)
    if s.endswith("%") and _NUM_RE.match(s[:-1].strip()) and any(c.isdigit() for c in s):
        return float(s[:-1].replace(",", "")), raw
    return None, raw


class Parser:
    """Parses one endpoint body. Collects rows and drift events."""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.rows: list[dict] = []
        self.drift: list[DriftEvent] = []
        self.update_local: datetime | None = None

    # -- helpers -----------------------------------------------------------
    def warn(self, path, kind, detail, severity="WARN"):
        self.drift.append(DriftEvent(self.endpoint, path, kind, str(detail)[:500], severity))

    def emit(self, stream, entity, field, raw, *, unit=None, ts_local=None):
        value, text = to_value(raw)
        if value is None and text is None:
            return
        unit = unit or config.UNITS.get((stream, field))
        self.rows.append({
            "ts_local": ts_local, "stream": stream, "entity": str(entity), "field": field,
            "value": value, "value_text": text, "unit": unit,
        })

    def check_fields(self, stream, key, records, expected: set[str] | None):
        if expected is None:
            return
        seen: set[str] = set()
        missing_counts: dict[str, int] = {}
        for rec in records:
            if not isinstance(rec, dict) or _is_total_row(rec):
                continue
            seen |= rec.keys()
            for k in expected - rec.keys():
                missing_counts[k] = missing_counts.get(k, 0) + 1
        for k, n in sorted(missing_counts.items()):
            self.warn(stream, "missing_field", f"'{k}' missing in {n}/{len(records)} records")
        for k in sorted(seen - expected):
            self.warn(stream, "new_field", f"'{k}' (parsed as a new field)")

    # -- generic record list ----------------------------------------------
    def record_list(self, key, records, *, entity_key="name", unit_key="engunit",
                    ts_key=None, positional=False, pie=False):
        stream = f"{self.endpoint}.{key}"
        if not isinstance(records, list):
            self.warn(stream, "type_change", f"expected list, got {type(records).__name__}", "ERROR")
            return
        self.check_fields(stream, key, records, config.EXPECTED_KEYS.get(self.endpoint, {}).get(key))
        for i, rec in enumerate(records):
            try:
                self._one_record(stream, i, rec, entity_key, unit_key, ts_key, positional, pie)
            except Exception as e:  # noqa: BLE001
                self.warn(stream, "parse_error", f"record {i}: {type(e).__name__}: {e}", "ERROR")

    def _one_record(self, stream, i, rec, entity_key, unit_key, ts_key, positional, pie):
        if not isinstance(rec, dict):
            self.emit(stream, f"[{i}]", "value", rec)
            return
        if _is_total_row(rec):
            # vert.plant/solar/eolica end with {"Total": x}: the sum of mwactual.
            self.emit(stream, "Total", "value", rec["Total"], unit="MW")
            return
        rec = dict(rec)
        unit = rec.pop(unit_key, None) if unit_key else None
        ts_local = None
        if ts_key and ts_key in rec:
            ts_raw = rec.pop(ts_key)
            ts_local = parse_scada_ts(ts_raw)
            if ts_local is None:
                self.warn(stream, "bad_timestamp", f"record {i}: {ts_raw!r}")
        if positional:
            entity = f"{self.endpoint}[{i}]" if entity_key is None else f"{entity_key}[{i}]"
        elif callable(entity_key):
            entity = entity_key(rec)
        elif entity_key in rec:
            entity = rec.pop(entity_key)
        else:
            entity = f"#{i}"
            self.warn(stream, "missing_entity_key", f"record {i} has no '{entity_key}'; keyed by position", "ERROR")
        if pie:
            m = _PIE_RE.match(str(entity))
            if m:
                entity = m.group(1)
                self.emit(stream, entity, "share_pct", m.group(3))
            else:
                self.warn(stream, "label_format", f"pie label not recognised: {entity!r}")
        if unit is None and stream.startswith("diagram.") and "MW" in str(entity).upper():
            unit = "MW"  # SCADA analog tags named ...MW.AV
        for field, raw in rec.items():
            if isinstance(raw, dict):
                for sub, v in raw.items():
                    if sub != "name":
                        self.emit(stream, entity, f"{field}_{sub}", v, ts_local=ts_local)
                continue
            self.emit(stream, entity, field, raw,
                      unit=unit if field == "value" else None, ts_local=ts_local)


def _is_total_row(rec) -> bool:
    return isinstance(rec, dict) and set(rec.keys()) == {"Total"}


def _int_entity(rec: dict) -> str:
    f = rec.get("from", {}) if isinstance(rec.get("from"), dict) else {}
    t = rec.get("to", {}) if isinstance(rec.get("to"), dict) else {}
    return f"{str(f.get('name', '?')).strip()} -> {str(t.get('name', '?')).strip()}"


# --------------------------------------------------------------------------
# Endpoint dispatch
# --------------------------------------------------------------------------
HANDLERS = {
    "sin": {
        "data": dict(),
        "max": dict(entity_key=lambda r: "SIN"),
        "water": dict(),
    },
    "gen": {
        "pie": dict(pie=True),
        "pie2": dict(pie=True),
        "unit": dict(),
    },
    "vert": {
        "flow": dict(),
        "plant": dict(),
        "solar": dict(),
        "eolica": dict(),
    },
    "int": {"nodes": dict(entity_key=_int_entity)},
    "diagram": {
        "units": dict(entity_key="tag", ts_key="timestamp"),
        "tooltips": dict(entity_key="tag", ts_key="timestamp"),
    },
    "flow": {"occi": dict(entity_key="occi", positional=True)},
}


def parse_endpoint(endpoint: str, body: bytes, *, fetched_at_utc: datetime) -> tuple[list[dict], list[dict], list[DriftEvent], datetime | None]:
    """Parse one endpoint body.

    Returns (snapshot_rows, trend_rows, drift_events, source_update_utc).
    Rows carry timestamp_utc/timestamp_local already resolved.
    """
    p = Parser(endpoint)
    try:
        obj = json.loads(body.decode("utf-8-sig"))
    except Exception as e:  # noqa: BLE001
        p.warn("$", "invalid_json", f"{type(e).__name__}: {e}", "ERROR")
        return [], [], p.drift, None
    if not isinstance(obj, dict):
        p.warn("$", "type_change", f"top level is {type(obj).__name__}", "ERROR")
        return [], [], p.drift, None

    expected = config.EXPECTED_KEYS.get(endpoint, {})
    for k in expected:
        if k not in obj:
            p.warn(k, "missing_key", f"top-level key '{k}' missing", "ERROR")

    # Source time: the file's `update` field (Panama local).
    update_local = parse_spanish_ts(obj.get("update"))
    if update_local is None:
        p.warn("update", "bad_timestamp", f"update={obj.get('update')!r}; falling back to fetch time", "ERROR")
        update_local = utc_to_local(fetched_at_utc).replace(microsecond=0)
        update_utc = None
    else:
        update_utc = local_to_utc(update_local)
    p.update_local = update_local

    handlers = HANDLERS.get(endpoint, {})
    for key, val in obj.items():
        if key in ("update", "trend"):
            continue
        try:
            if key in handlers:
                p.record_list(key, val, **handlers[key])
            elif endpoint == "vert" and key == "final":
                p.emit("vert.final", "final", "value", val)
            elif endpoint == "flow" and key == "msg" and isinstance(val, dict):
                p.check_fields("flow.msg", key, [val], expected.get("msg"))
                for f, v in val.items():
                    p.emit("flow.msg", "msg", f, v)
            else:
                p.warn(key, "new_key", f"unexpected top-level key '{key}' ({type(val).__name__})")
                if isinstance(val, list) and val and all(isinstance(r, dict) for r in val):
                    ek = "name" if all("name" in r for r in val) else None
                    p.record_list(key, val, entity_key=ek, positional=ek is None)
                elif not isinstance(val, (list, dict)):
                    p.emit(f"{endpoint}.{key}", key, "value", val)
        except Exception as e:  # noqa: BLE001
            p.warn(key, "parse_error", f"{type(e).__name__}: {e}", "ERROR")

    fallback_utc = update_utc or fetched_at_utc
    rows = []
    for r in p.rows:
        ts_local = r.pop("ts_local")
        if ts_local is not None:
            ts_utc = local_to_utc(ts_local)
        else:
            ts_utc = fallback_utc
            ts_local = utc_to_local(ts_utc)
        r["timestamp_utc"] = ts_utc
        r["timestamp_local"] = ts_local
        rows.append(r)

    trend_rows: list[dict] = []
    if endpoint == "sin" and "trend" in obj:
        try:
            trend_rows = parse_trend(obj["trend"], update_local, p)
        except Exception as e:  # noqa: BLE001
            p.warn("trend", "parse_error", f"{type(e).__name__}: {e}", "ERROR")
            trend_rows = []
    return rows, trend_rows, p.drift, update_utc


def parse_trend(trend: dict, update_local: datetime, p: Parser) -> list[dict]:
    """Decode sin.trend.

    Verified 2026-09-29: sin.json is regenerated every 15 s. The point labelled
    HH:MM in a file whose `update` is ...:SS is an instantaneous sample taken at
    HH:MM:SS (Panama local). So the true sample instant is label + update.second,
    and the last point is the `update` instant itself. We store that instant as
    timestamp_* and keep the minute label as minute_label_local.

    Month indexing: `m` is zero-based. We verify this against `update` on every
    file (the last point must be within 2 minutes of update) and fall back to
    one-based decoding, loudly, if the source ever changes convention.
    """
    names = trend.get("names")
    series = trend.get("series")
    if not isinstance(series, list) or not series:
        p.warn("trend.series", "missing_key", "trend.series missing or empty", "ERROR")
        return []
    if names != config.TREND_EXPECTED_NAMES:
        p.warn("trend.names", "label_change", f"names={names!r}")
    if not isinstance(names, list):
        names = []

    # Decide month indexing using the last point vs update.
    last_time = series[-1].get("time", {})
    zero_based = True
    try:
        d0 = abs((trend_label(last_time, zero_indexed_month=True) - update_local).total_seconds())
    except (KeyError, ValueError, TypeError):
        d0 = float("inf")
    if d0 > 120:
        try:
            d1 = abs((trend_label(last_time, zero_indexed_month=False) - update_local).total_seconds())
        except (KeyError, ValueError, TypeError):
            d1 = float("inf")
        if d1 <= 120:
            zero_based = False
            p.warn("trend.series.time", "month_indexing_changed",
                   "last trend point only matches `update` with ONE-based months; decoding one-based", "ERROR")
        else:
            p.warn("trend.series.time", "label_mismatch",
                   f"last trend point {last_time} does not match update {update_local}; labels used as-is", "ERROR")

    offset = timedelta(seconds=update_local.second)
    n = len(series)
    rows: list[dict] = []
    bad_values = 0
    step_anomalies = 0
    prev_label = None
    for i, point in enumerate(series):
        try:
            label = trend_label(point["time"], zero_indexed_month=zero_based)
        except Exception:  # noqa: BLE001
            bad_values += 1
            continue
        if prev_label is not None and label - prev_label != timedelta(minutes=1):
            step_anomalies += 1
        prev_label = label
        sample_local = label + offset
        sample_utc = local_to_utc(sample_local)
        values = point.get("value") or []
        for j, raw in enumerate(values):
            val, _ = to_value(raw)
            if val is None:
                bad_values += 1
                continue
            entity = names[j] if j < len(names) else f"series{j}"
            rows.append({
                "timestamp_utc": sample_utc,
                "timestamp_local": sample_local,
                "stream": "sin.trend",
                "entity": entity,
                "field": "value",
                "value": val,
                "unit": "MW",
                "minute_label_local": label,
            })
    if bad_values:
        p.warn("trend.series", "bad_value", f"{bad_values} unparseable points/values of {n}")
    if step_anomalies:
        p.warn("trend.series", "irregular_step", f"{step_anomalies} steps != 1 minute")
    if n != 1441:
        p.warn("trend.series", "length_change", f"{n} points (was 1441)")
    return rows
