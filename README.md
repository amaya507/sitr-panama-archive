# sitr-panama-archive

Unattended archive of the public real-time telemetry that Panama's national
system operator (Centro Nacional de Despacho, **CND**, part of ETESA) publishes
on **SITR** at <https://sitr.cnd.com.pa/m/>.

SITR has no history and no API documentation: it serves a rolling window, so
anything not captured is lost for good. This repository captures it every
5 minutes, parses it into long-format Parquet, and keeps every raw response so
history can be reparsed if the site's format changes.

Built for a KTH MSc thesis on storage and flexibility market design for Panama.
Maintainer contact (also sent in every request's User-Agent): rigoamaya23@gmail.com.

---

## Contents

1. [What is captured](#1-what-is-captured)
2. [The trend series: three things you must know](#2-the-trend-series-three-things-you-must-know)
3. [Where the data lives](#3-where-the-data-lives)
4. [Schema](#4-schema)
5. [Querying](#5-querying)
6. [How it runs](#6-how-it-runs)
7. [Completeness and verification](#7-completeness-and-verification)
8. [Alerts, and what to do about each](#8-alerts-and-what-to-do-about-each)
9. [Reparsing raw history](#9-reparsing-raw-history)
10. [Downloadable exports](#10-downloadable-exports)
11. [Citing this dataset](#11-citing-this-dataset)
12. [Being a polite client / if CND asks us to stop](#12-being-a-polite-client--if-cnd-asks-us-to-stop)
13. [Local development](#13-local-development)
14. [Facts verified on 2026-09-29](#14-facts-verified-on-2026-09-29)

---

## 1. What is captured

Base URL: `https://sitr.cnd.com.pa/m/pub/data/` (paths in the site's
`/m/js/scriptsnew.js` are relative and resolve here; `/m/data/` returns 404).

| Endpoint | Raw size | Contents | Captured as streams |
|---|---|---|---|
| `sin.json` | ~356 KB | System summary, reservoir levels, historical peak, **24 h trend at 1-minute resolution** | `sin.data`, `sin.water`, `sin.max`, `sin.trend` |
| `gen.json` | ~25 KB | Generation mix by technology, renewable share, **271 generating units** (MW) | `gen.pie`, `gen.pie2`, `gen.unit` |
| `vert.json` | ~21 KB | West flow and limit, **53 hydro plants** (level, max level, MW, agent, contract, spill flag), 55 solar, 8 wind | `vert.flow`, `vert.plant`, `vert.solar`, `vert.eolica`, `vert.final` |
| `int.json` | ~3 KB | 13 SIEPAC interconnection points, MW/MVAr at both ends | `int.nodes` |
| `diagram.json` | ~55 KB | SCADA analog tags (MW) with per-tag timestamps: 117 `units`, 200 `tooltips` | `diagram.units`, `diagram.tooltips` |
| `flow.json` | ~0.4 KB | 7 unnamed "occi" (west flow) values, and a message banner | `flow.occi`, `flow.msg` |

Dead endpoints referenced by the JS (404 on 2026-09-29): `carga`, `comp`,
`pie`, `volt`, `lines115`, `transformers`, `cargaGatun`. The daily job probes
each once a day and opens an issue if any starts returning 200 (its body is
saved under `probe/` in that chunk).

Every other stream is an **instantaneous snapshot with no history**. The
resolution at which we poll is the resolution we have forever.

## 2. The trend series: three things you must know

`sin.json → trend` holds 1,441 points (24 h + 1 minute) for three series:
`Generación`, `Demanda Real`, `Demanda Pronosticada`, all MW.

```json
{"trend": {"names": ["Generación", "Demanda Real", "Demanda Pronosticada"],
           "series": [{"value": ["1345","1351","1429"],
                       "time": {"y":"2026","m":"8","d":"28","h":"5","mi":"17"}}, ...]}}
```

### 2.1 The month is zero-indexed

`"m": "8"` is **September** (JavaScript `Date` convention). Day, hour and minute
are not shifted. A month-shifted timestamp does not crash anything: it
produces a plausible series that is quietly 30 days wrong. So the parser
**checks every file**: the last trend point must decode to within 2 minutes of
the file's human-readable `update` field (`"29-septiembre-2026 5:23:53"`). If
it only matches with one-based months, the parser switches convention and
raises an ERROR drift alert. If it matches neither, it raises an ERROR alert and
keeps the raw file. Tests: `tests/test_time.py`.

### 2.2 Each point is an instantaneous sample, not a minute value

`sin.json` is regenerated about every 15 seconds. The point labelled `05:12` in
a file whose `update` ends in `:23` is the value **at 05:12:23**. Every point in
that file shares the same seconds offset, and the last point *is* the `update`
instant. Consequences:

* Two fetches at different seconds offsets give different values for the same
  minute label. Both are real samples, so both are kept. Records are keyed by the
  **true sample instant** (`timestamp_utc`), with the label kept in
  `minute_label_local`.
* The regeneration offset drifts, so every fetch at a new offset adds another
  sample per minute. With a fetch every ~5 minutes, most minutes end up with
  several samples.
* The **canonical 1-minute series** (`sin.trend.minute` in the exports) is the
  mean of the samples held within each minute, with `n_samples`.

### 2.3 Real demand is revised after publication

Between fetches a few minutes apart, `Demanda Real` changes by 1–6 MW on most
of the 24 h window. `Generación` does not change, and `Demanda Pronosticada`
jitters by ±1 MW. So a sample can have several published values. The store
keeps **one row per distinct (series, instant, value)**, with
`first_seen_update_utc` and `last_seen_update_utc`, which record when each version
was published. The canonical value is the one seen most recently. Treat real
demand as a preliminary value that the operator revises.

## 3. Where the data lives

| Tier | What | Where | Lifetime |
|---|---|---|---|
| **Parsed record** | Long-format Parquet + manifests | `data/` in this git repo | forever, versioned by commit |
| **Raw archive** | Every response, byte-for-byte | GitHub **Releases** `raw-YYYY-MM-DD` (one per UTC day, one `.tar.gz` asset per fetch) | forever |
| Exports | Consolidated yearly trend (CSV + Parquet), monthly per-stream Parquet, gap report | GitHub release [`exports`](https://github.com/amaya507/sitr-panama-archive/releases/tag/exports), rebuilt weekly | convenience copy, always regenerable from `data/` |

Raw files are deliberately **not** in git: at about 7 MB/day of already-gzipped
data (2.6 GB/year) the repository would become unclonable, and deleting files
from the working tree does not remove them from history. Releases are durable
and public, and they don't make the repo bigger.

The exception is `backfill/`, which holds the handful of chunks captured by hand
on 2026-09-29 before the scheduled pipeline went live.

```
data/
  snapshots/<stream>/year=YYYY/month=MM/<stream>_YYYY-MM-DD.parquet   (UTC day)
  trend/year=YYYY/month=MM/trend_YYYY-MM-DD.parquet                   (UTC day of sample)
  manifest/
    ingested_chunks.csv          every raw chunk ingested (idempotency ledger)
    trend_held_intervals.csv     contiguous minutes held (Panama local + UTC)
    trend_missing_intervals.csv  gaps
    snapshot_cadence.csv         per-day count/spacing of snapshot observations
    drift_log.csv                every schema-drift kind ever seen, first/last chunk
    fetch_log/fetch_log_YYYY-MM.parquet   per-fetch HTTP status, retries, latency
    endpoint_probes.csv          daily status of the dead endpoints
    last_daily_run.txt
```

A chunk is `sitr_<UTC start>_<kind>_ok<k>of<n>[-x<failed endpoint>...].tar.gz`,
containing `meta.json` (UA, code version, per-endpoint status, headers,
timings) and `raw/<endpoint>.json`. `kind` is `snap` (5-minute job), `trend`
(twice-daily backstop) or `manual`.

## 4. Schema

**Long format everywhere.** One row = one value of one field of one entity at
one instant. Nothing wide is persisted.

### Snapshot streams (`data/snapshots/**`)

| column | type | meaning |
|---|---|---|
| `timestamp_utc` | timestamp[ms, UTC] | when the value was true, according to the source: the file's `update` field; for `diagram.*` the per-tag SCADA timestamp |
| `timestamp_local` | timestamp[ms] | same instant, Panama wall clock (always UTC−5; derived, never from the runner) |
| `stream` | string | `<endpoint>.<key>`, e.g. `gen.unit` |
| `entity` | string | unit/plant/tag/node name as published (see table) |
| `field` | string | source field name, e.g. `value`, `mwactual`, `nactual` |
| `value` | float64 | numeric value, or null if not numeric |
| `value_text` | string | original text when it is not a plain number (`"VIERTE"`, `"AES"`, `"100%"`, `"1,040.82"`, colours) |
| `unit` | string | `engunit` from the source when given, else only where unambiguous (see `sitr/config.py:UNITS`), else null |
| `source_update_utc` | timestamp[ms, UTC] | the file's `update` field |
| `fetched_at_utc` | timestamp[ms, UTC] | when our request completed |
| `chunk` | string | raw chunk this row came from (provenance) |

Dedupe key: `(stream, entity, field, timestamp_utc)`. Two fetches of the same
file generation collapse into one row. Stale SCADA tags whose timestamp did not move
collapse too, so `diagram.*` stores changes, not repeats.

| stream | entity | fields | notes |
|---|---|---|---|
| `sin.data` | `Generación total`, `Carga total`, `Intercambio neto`, `Intercambio programado`, `Reserva rodante`, `Frecuencia` | `value` | units from `engunit` (MW, Hz) |
| `sin.water` | reservoir name (38) | `value`, `hi`, `lo`, `cu`, `cl`, `color` | levels, probably m a.s.l. (not stated); `cu`/`cl` are display percentages |
| `sin.max` | `SIN` | `value` (MW), `date` (text) | historical peak |
| `gen.pie` | `Hídrica`, `Térmica`, `Solar`, `Eólica` | `value` (MW), `share_pct` (%), `color` | category parsed out of labels like `"Hídrica 1062.14 (75.84%)"` |
| `gen.pie2` | `Renovable`, `Térmica` | same | |
| `gen.unit` | unit name (271) | `value` (MW), `color` | |
| `vert.flow` | `Demanda Total`, `Flujo de Occidente`, `Límite de Flujo de Occidente`, … | `value` | units from `engunit` |
| `vert.plant` | hydro plant (52) + `Total` | `nactual`, `cotamax`, `vert`, `cons`, `mwactual` (MW), `agente`, `contrato` (MW), `color`; `Total`: `value` | `vert` = `"VIERTE"` when spilling, `""` otherwise; `nactual`/`cotamax` = current/max level (m, not stated) |
| `vert.solar`, `vert.eolica` | farm/park + `Total` | `lic` (licensed MW), `mwactual` (MW), `agente` | |
| `vert.final` | `final` | `value` (MW) | published with a thousands separator |
| `int.nodes` | `"<from> -> <to>"` (13) | `from_mw`, `from_mvar`, `to_mw`, `to_mvar` | one node has an empty `to` name (`"Brillantes -> "`) |
| `diagram.units`, `diagram.tooltips` | SCADA tag (117 / 200) | `value` (MW for `…MW…` tags), `name`, `color` | 63 tags appear in both lists; timestamp is per tag |
| `flow.occi` | `occi[0]`…`occi[6]` | `value` (MW) | **unnamed, positional**; `occi[6]` tracks "Flujo de Occidente". If the list length changes, check positions in raw |
| `flow.msg` | `msg` | `name`, `description`, `style` | operator banner text |

New fields and new top-level lists are captured automatically (and alerted).

### Trend (`data/trend/**`)

`timestamp_utc`, `timestamp_local` (true sample instant), `stream`
(`sin.trend`), `entity` (series name), `field` (`value`), `value`, `unit` (MW),
`minute_label_local` (the source's minute label, decoded), `first_seen_update_utc`,
`last_seen_update_utc`. Key: `(entity, timestamp_utc, value)`.

## 5. Querying

```python
import pandas as pd, glob
from sitr import store

samples = store.load_trend()                 # every sample and revision
canon   = store.canonical_trend(samples)     # latest published value per instant
minute  = store.minute_trend(samples)        # 1-minute long series (mean of samples)
hourly  = (minute.set_index("timestamp_utc").groupby("entity")["value"]
                 .resample("1h").mean().reset_index())      # join to hourly marginal cost

units = pd.concat(pd.read_parquet(f) for f in
                  glob.glob("data/snapshots/gen.unit/year=2026/month=10/*.parquet"))
```

DuckDB works directly on the Hive-style paths:
`SELECT * FROM 'data/snapshots/gen.unit/**/*.parquet' WHERE entity = 'Bayano 1'`.

## 6. How it runs

Four GitHub Actions workflows (public repo: Actions minutes are free):

| workflow | schedule (UTC) | does |
|---|---|---|
| `snapshot` | every ~5 min, **self-dispatching chain** (cron at :07/:37 only restarts it) | wait until ≥285 s after the previous chunk; fetch the 6 endpoints once each, sequentially; upload one chunk to today's release; dispatch the next run. Stdlib only |
| `daily` | 06:17 and 18:17 | backstop `sin.json` fetch + dead-endpoint probe; download un-ingested chunks (14-day look-back); parse into `data/`; manifests; gap report; alerts; watchdog; **commit**; re-enable schedules |
| `exports` | Mon 07:43 | rebuild consolidated exports and publish them to the `exports` release |
| `tests` | on push | the test suite, with `TZ=Asia/Kolkata` to show the runner timezone doesn't leak into the data |

Design choices, and why:

* **Daily partitions are written once the UTC day is complete** (the 06:17 run
  ingests yesterday). Each file is committed essentially once, so git history
  grows at about the data rate: ~0.7 MB/day of snapshot Parquet (≈0.26 GB/year) plus the trend.
* **The 24 h trend window is the safety net.** Every 5-minute fetch includes
  `sin.json`, so the trend is re-captured 288 times a day. The twice-daily backstop
  covers the case where the 5-minute job is down. A trend gap only opens if
  *every* fetch fails for more than 24 h.
* **GitHub's cron cannot hold a 5-minute cadence.** On 2026-09-29 the
  `*/5` schedule fired 2 times in 10 hours and `daily` ran 4 h late. So
  `snapshot` is a chain: each run dispatches the next (`workflow_dispatch` from
  `GITHUB_TOKEN` is allowed to start runs). The cron is only a restarter.
  Spacing is enforced in code (`sitr/pace.py`), which reads the previous chunk's time
  from the release and waits until 285 s have passed, so duplicate triggers can
  never poll CND faster. The optional **environment `pacer`** (Settings →
  Environments → `pacer` → Wait timer = 4 min) makes GitHub hold each queued run
  without occupying a runner; without it the pace step waits on the runner.
  The real cadence is recorded per day in `snapshot_cadence.csv`, printed by the
  gap report, and alerted on (`snapshot-cadence`).
* **The 60-day inactivity rule.** GitHub disables scheduled workflows in repos with no activity
  for 60 days. The daily job commits `last_daily_run.txt` on every run and
  calls `gh workflow enable` for all three.
* The HTTP client (`sitr/fetch.py`): 30 s timeout, 4 attempts with exponential
  backoff (2, 4, 8 s + jitter), honours `Retry-After`, retries on 5xx/429/timeouts
  and truncated JSON, never retries 4xx. A 1 s pause between endpoints.

## 7. Completeness and verification

```bash
python -m sitr gaps                      # completeness report (also in data/manifest/)
python -m sitr gaps --since "2026-10-01 00:00" --until "2026-12-31 23:59"
python -m sitr verify                    # compare the store with a fresh sin.json fetch
```

The gap report gives, for the trend, the number of minutes expected vs held
(a minute is held only if **all three** series have a sample), every missing
interval, samples per minute, and how many samples were revised. For the snapshots
it gives observations per UTC day and their median/p95/max spacing. Use these figures in
the methodology chapter.

`verify` fetches `sin.json` once and checks that every minute of the fresh 24 h
window (up to the newest stored sample) is held, that same-instant samples match
exactly, and that the minute-level series correlates above 0.98. It is the
"data has landed" test. A green workflow alone doesn't prove that.

## 8. Alerts, and what to do about each

Alerts are **GitHub issues** titled `[sitr-alert:<key>] …`. GitHub emails you
for each new issue (keep *Watch → Custom → Issues* on). One issue stays open per
key and gets a comment at most every 12 h. Issues close themselves when the
condition clears (except gaps and drift, which you close after noting them).

| key | meaning | what to do |
|---|---|---|
| `snapshot-stale` | no successful fetch in 90 min | Actions tab → `snapshot`. Disabled? Enable it. Failing? Read the log. GitHub outage? Wait. |
| `snapshot-cadence` | fewer than half the expected snapshots in 12 h | the chain has stopped: Actions → `snapshot` → *Run workflow* restarts it. Check the last run's log for why it didn't dispatch |
| `sin-stale` | no `sin.json` for 6 h | **Urgent**: trend data older than 24 h is lost for good if this lasts. Run `trend-capture` from any machine and upload the chunk (§13). |
| `fetch-failing` | every endpoint failed in one run | usually the site is down; check <https://sitr.cnd.com.pa/m/> in a browser |
| `blocked` | 401/403 or a Cloudflare challenge | **Do not work around it.** See §12. If only GitHub IPs are blocked, run the same code on a VPS (`python -m sitr snapshot` from cron + `scripts/upload_chunks.sh`). |
| `endpoint-failing-<ep>` | >20% failures for one endpoint in 12 h | check whether the endpoint moved or was renamed |
| `drift-YYYYMMDD` | new schema drift kinds | raw is safe. Fix `sitr/parse.py`/`config.py`, add a test with the new raw file, then reparse (§9) |
| `trend-gap` | new missing minutes in the trend | not recoverable; note it for the methodology and close the issue |
| `dead-endpoint-alive` | a 404 endpoint now returns 200 | inspect `probe/<name>.json` in that chunk; consider adding it to `LIVE_ENDPOINTS` |
| `snapshot-workflow`, `daily-workflow`, `backstop-upload` | pipeline errors | read the run log. A chunk that failed to upload is kept as a run artifact for 30 days |
| `exports` | weekly export publish failed | capture is unaffected; rerun `exports` from the Actions tab |

## 9. Reparsing raw history

When the source format changes, or the parser is improved:

```bash
# 1. get the raw chunks for the period (needs no auth: releases are public)
python - <<'EOF'
from pathlib import Path
from sitr.releases import download_month
for m in ["2026-10", "2026-11"]:
    download_month(m, Path("chunks"))
EOF
cp backfill/*.tar.gz chunks/                   # pre-pipeline chunks of 2026-09-29

# 2. rebuild into a FRESH directory, ignoring the ledger
python -m sitr consolidate --chunks chunks --data rebuilt --no-ledger --include-today

# 3. compare, then replace
python -m sitr gaps --data rebuilt
rm -rf data/snapshots data/trend && mv rebuilt/snapshots rebuilt/trend data/
git add -A data && git commit -m "data: reparse with parser vX (reason)"
```

To look at one raw response: `tar -xzf sitr_...tar.gz raw/sin.json`, or
`sitr.capture.read_chunk(path)` → `(meta, {endpoint: bytes}, probes)`.

Merges are idempotent, so reparsing into the existing `data/` (without
`--no-ledger`) only adds what is missing. A fresh directory is cleaner when the
parse logic itself changed.

## 10. Downloadable exports

The weekly `exports` workflow rebuilds these from `data/` and replaces the
assets of the release [`exports`](https://github.com/amaya507/sitr-panama-archive/releases/tag/exports):

| file | contents |
|---|---|
| `sitr_trend_minute_YYYY.csv` / `.parquet` | canonical 1-minute trend, long format (`timestamp_utc, timestamp_local, stream, entity, field, value, unit, n_samples`) |
| `sitr_trend_samples_YYYY.parquet` | every sample and every revision |
| `<stream>_YYYY-MM.parquet` | each snapshot stream, one file per month |
| `gap_report.txt` | the completeness report |

The same files can be built locally at any time: `python -m sitr export --out export`.
The exports are a convenience copy. Cite a repository commit (§11), not the release.

A Google Drive mirror was considered and deliberately dropped (2026-09-29).
Service accounts have no storage quota and cannot write into a personal My Drive
folder, and the only credential that could (a user OAuth token) was ruled out.
Git plus Releases already give two durable copies. If a Drive copy is ever
wanted, see commit `32b99dd` for an untested rclone mirror (`sitr/mirror.py`,
`.github/workflows/mirror.yml`) that can be restored.

## 11. Citing this dataset

Cite the repository at a specific commit, e.g.
`https://github.com/amaya507/sitr-panama-archive/tree/<commit-sha>`. The
commit fixes both the parsed data and the parser version, and every row names
the raw chunk it came from (`chunk` column). Every chunk records the code
version that fetched it (`meta.json → code_version`).

Source attribution: Centro Nacional de Despacho (CND), ETESA, Panama. SITR
public real-time data, <https://sitr.cnd.com.pa/m/>.

## 12. Being a polite client / if CND asks us to stop

* One request per endpoint per cycle, sequential, 1 s apart. For scale: the
  public SITR page itself re-requests every endpoint every **5 seconds** per open
  browser tab, so a 5-minute poll is 60× lighter than one visitor.
* The User-Agent names this repository and the contact email.
* Public, unauthenticated endpoints only. There is no login, no credential,
  and no workaround of any access control, and there must never be. On 401/403
  or a Cloudflare challenge the pipeline alerts and does **not** retry around it.

To stop immediately: Actions → `snapshot` → "…" → *Disable workflow* (and the
same for `daily`). To throttle: change the cron in
`.github/workflows/snapshot.yml` (e.g. `*/15`).

## 13. Local development

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt   # (bin/ on Linux/macOS)
python -m pytest -q
python -m sitr snapshot --out chunks        # one live fetch cycle
python -m sitr consolidate --chunks chunks --data /tmp/data --include-today
python -m sitr gaps --data /tmp/data
```

Uploading a chunk captured by hand (needs the GitHub CLI, `gh auth login`):
`bash scripts/upload_chunks.sh chunks`. It lands in the right daily release,
and the next `daily` run ingests it.

Code map: `fetch.py` (HTTP), `capture.py` (chunks), `parse.py` (raw → long rows,
drift), `store.py` (Parquet, dedupe), `consolidate.py` (ingest + manifests),
`gaps.py`, `verify.py`, `watchdog.py`, `github.py` (issues/releases),
`export.py`, `config.py` (everything source-specific).

## 14. Facts verified on 2026-09-29

These came from fetching the live site, not from documentation (there is none):

* 200: `sin`, `gen`, `vert`, `int`, `diagram`, `flow`. 404: the seven listed in §1. Served via Cloudflare.
* Trend month zero-indexed: the last point `{"m":"8","d":"29","h":"5","mi":"23"}` ↔ `update` `29-septiembre-2026 5:23:53`, fetched 10:24 UTC.
* Trend times are Panama local (UTC−5). The last point equals `sin.data` (Generación total / Carga total).
* `sin.json` regenerated every ~15 s (`Last-Modified` :08/:23/:38/:53 in one minute, with drifting offsets later). Files at the same offset one minute apart were identical on 1,423 of 1,440 points.
* `Demanda Real` revised on 930 of 1,436 points between two same-offset files 4 minutes apart (max 6 MW). `Generación` never changed.
* `vert.plant/solar/eolica` end with `{"Total": x}`. `flow.occi` is 7 unnamed values. 63 tags appear in both `diagram.units` and `diagram.tooltips`.
* Response bodies are valid UTF-8, with no BOM.
* Gzipped raw per snapshot cycle: 24.6 KB (26.4 KB as a chunk) ≈ 7.6 MB/day ≈ 2.7 GB/year.
