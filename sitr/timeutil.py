"""Time handling. All conversions use a fixed UTC-5 offset; the runner's local
timezone is never consulted (no naive `datetime.now()`, no `time.localtime`)."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from .config import PANAMA_TZ

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

_UPDATE_RE = re.compile(
    r"^\s*(\d{1,2})-([A-Za-zÁÉÍÓÚáéíóú]+)-(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$"
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_spanish_ts(text: str) -> datetime | None:
    """Parse '29-septiembre-2026 5:17:08' (or without seconds) to a naive
    Panama-local datetime. Returns None if the format is not recognised."""
    if not isinstance(text, str):
        return None
    m = _UPDATE_RE.match(text)
    if not m:
        return None
    day, month_name, year, hh, mm, ss = m.groups()
    month = SPANISH_MONTHS.get(month_name.lower())
    if month is None:
        return None
    try:
        return datetime(int(year), month, int(day), int(hh), int(mm), int(ss or 0))
    except ValueError:
        return None


def parse_scada_ts(text: str) -> datetime | None:
    """Parse diagram.json per-tag timestamps: '2026-09-29 05:23:54.117' (local)."""
    if not isinstance(text, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    return None


def local_to_utc(local_naive: datetime) -> datetime:
    return local_naive.replace(tzinfo=PANAMA_TZ).astimezone(timezone.utc)


def utc_to_local(utc: datetime) -> datetime:
    """Aware UTC -> naive Panama wall clock."""
    return utc.astimezone(PANAMA_TZ).replace(tzinfo=None)


def trend_label(time_obj: dict, *, zero_indexed_month: bool = True) -> datetime:
    """Decode a trend point's time object. The source's month is ZERO-INDEXED
    (JavaScript Date convention): {"m": "8"} is September."""
    m = int(time_obj["m"]) + (1 if zero_indexed_month else 0)
    return datetime(int(time_obj["y"]), m, int(time_obj["d"]), int(time_obj["h"]), int(time_obj["mi"]))


def fmt_compact(utc: datetime) -> str:
    return utc.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_compact(text: str) -> datetime:
    return datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def minutes(n: int) -> timedelta:
    return timedelta(minutes=n)
