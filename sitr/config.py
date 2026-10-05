"""Constants for the SITR capture pipeline.

Everything that describes the source (URLs, expected shapes, units) lives here so
that a change on the CND side is a one-file edit.
"""
from __future__ import annotations

from datetime import timedelta, timezone
from pathlib import Path

BASE_URL = "https://sitr.cnd.com.pa/m/pub/data/"

# Endpoints that returned 200 on 2026-09-29. Fetched sequentially, once per cycle.
LIVE_ENDPOINTS = ["sin", "gen", "vert", "int", "diagram", "flow"]

# Referenced in https://sitr.cnd.com.pa/m/js/scriptsnew.js but 404 on 2026-09-29.
# Probed once per day by the daily job; a 200 raises an alert.
DEAD_ENDPOINTS = ["carga", "comp", "pie", "volt", "lines115", "transformers", "cargaGatun"]

CONTACT_EMAIL = "rigoamaya23@gmail.com"
REPO_URL = "https://github.com/amaya507/sitr-panama-archive"
USER_AGENT = (
    f"sitr-panama-archive/1.0 (+{REPO_URL}; academic research, KTH MSc thesis; "
    f"contact: {CONTACT_EMAIL})"
)

# Panama is UTC-5 all year, no DST. A fixed offset is correct and avoids any
# dependence on the runner's timezone database or TZ variable.
PANAMA_TZ = timezone(timedelta(hours=-5), "America/Panama")

HTTP_TIMEOUT_S = 30          # per socket operation
HTTP_DEADLINE_S = 60         # hard cap per attempt, whole request (see fetch._with_deadline)
HTTP_ATTEMPTS = 4
HTTP_BACKOFF_BASE_S = 2.0
PAUSE_BETWEEN_REQUESTS_S = 1.0

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
SNAPSHOT_DIR = DATA_DIR / "snapshots"
TREND_DIR = DATA_DIR / "trend"
MANIFEST_DIR = DATA_DIR / "manifest"

RELEASE_PREFIX = "raw-"  # one GitHub release per UTC day: raw-YYYY-MM-DD

# Expected shape of each endpoint, as observed on 2026-09-29. Used only for
# drift detection: anything missing or new is logged, and parsing continues.
# None means "scalar / handled specially".
EXPECTED_KEYS: dict[str, dict[str, set[str] | None]] = {
    "sin": {
        "data": {"name", "value", "engunit"},
        "max": {"date", "value", "engunit"},
        "water": {"name", "value", "hi", "lo", "cu", "cl", "color"},
        "trend": None,
        "update": None,
    },
    "gen": {
        "pie": {"name", "value", "color"},
        "pie2": {"name", "value", "color"},
        "unit": {"name", "value", "color"},
        "update": None,
    },
    "vert": {
        "flow": {"name", "value", "engunit"},
        "plant": {"name", "color", "nactual", "cotamax", "vert", "cons", "mwactual", "agente", "contrato"},
        "solar": {"name", "lic", "mwactual", "agente"},
        "eolica": {"name", "lic", "mwactual", "agente"},
        "final": None,
        "update": None,
    },
    "int": {"nodes": {"from", "to"}, "update": None},
    "diagram": {
        "units": {"tag", "name", "value", "color", "timestamp"},
        "tooltips": {"tag", "name", "value", "color", "timestamp"},
        "update": None,
    },
    "flow": {"msg": {"name", "description", "style"}, "occi": {"value"}, "update": None},
}

# Drift kinds that are known source quirks: logged in drift_log.csv, never alerted.
# irregular_step: the source occasionally skips one minute label (seen 2026-09-30).
BENIGN_DRIFT_KINDS = {"irregular_step"}

TREND_EXPECTED_NAMES = ["Generación", "Demanda Real", "Demanda Pronosticada"]

# Units the source does not state explicitly but which are unambiguous from the
# site's own labelling. Where the source gives `engunit`, that wins. Anything not
# listed here and without engunit is stored with unit = null (see README).
UNITS: dict[tuple[str, str], str] = {
    ("gen.unit", "value"): "MW",
    ("gen.pie", "value"): "MW",
    ("gen.pie", "share_pct"): "%",
    ("gen.pie2", "value"): "MW",
    ("gen.pie2", "share_pct"): "%",
    ("vert.plant", "mwactual"): "MW",
    ("vert.plant", "contrato"): "MW",
    ("vert.solar", "mwactual"): "MW",
    ("vert.solar", "lic"): "MW",
    ("vert.eolica", "mwactual"): "MW",
    ("vert.eolica", "lic"): "MW",
    ("vert.final", "value"): "MW",
    ("int.nodes", "from_mw"): "MW",
    ("int.nodes", "to_mw"): "MW",
    ("int.nodes", "from_mvar"): "MVAr",
    ("int.nodes", "to_mvar"): "MVAr",
    ("flow.occi", "value"): "MW",
    ("sin.trend", "value"): "MW",
}
