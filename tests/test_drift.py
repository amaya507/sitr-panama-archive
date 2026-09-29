"""Schema drift: log loudly, keep going, never lose the other streams."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from conftest import ALL_1024, make_chunk, obj, raw
from sitr.consolidate import consolidate
from sitr.parse import parse_endpoint

FETCHED = datetime(2026, 9, 29, 10, 24, 1, tzinfo=timezone.utc)


def _enc(o) -> bytes:
    return json.dumps(o, ensure_ascii=False).encode("utf-8")


def test_missing_top_level_key_is_error_but_rest_parses():
    o = obj("gen_20260929T1024Z")
    del o["unit"]
    rows, _, drift, _ = parse_endpoint("gen", _enc(o), fetched_at_utc=FETCHED)
    assert any(d.kind == "missing_key" and d.path == "unit" and d.severity == "ERROR" for d in drift)
    assert {r["stream"] for r in rows} == {"gen.pie", "gen.pie2"}  # pies still captured


def test_renamed_field_is_reported_and_kept():
    o = obj("gen_20260929T1024Z")
    for u in o["unit"]:
        u["valor"] = u.pop("value")
    rows, _, drift, _ = parse_endpoint("gen", _enc(o), fetched_at_utc=FETCHED)
    kinds = {(d.kind, d.path) for d in drift}
    assert ("missing_field", "gen.unit") in kinds and ("new_field", "gen.unit") in kinds
    assert sum(r["field"] == "valor" for r in rows) == 271  # data not lost


def test_record_without_entity_key_keyed_by_position():
    o = obj("gen_20260929T1024Z")
    del o["unit"][0]["name"]
    rows, _, drift, _ = parse_endpoint("gen", _enc(o), fetched_at_utc=FETCHED)
    assert any(d.kind == "missing_entity_key" and d.severity == "ERROR" for d in drift)
    assert any(r["entity"] == "#0" for r in rows)


def test_new_top_level_list_is_parsed_generically():
    o = obj("vert_20260929T1024Z")
    o["baterias"] = [{"name": "BESS 1", "mwactual": "12.5"}]
    rows, _, drift, _ = parse_endpoint("vert", _enc(o), fetched_at_utc=FETCHED)
    assert any(d.kind == "new_key" for d in drift)
    assert any(r["stream"] == "vert.baterias" and r["entity"] == "BESS 1" and r["value"] == 12.5 for r in rows)


def test_missing_update_falls_back_to_fetch_time_loudly():
    o = obj("int_20260929T1024Z")
    del o["update"]
    rows, _, drift, update = parse_endpoint("int", _enc(o), fetched_at_utc=FETCHED)
    assert update is None
    assert any(d.path == "update" and d.severity == "ERROR" for d in drift)
    assert rows and all(r["timestamp_utc"] == FETCHED for r in rows)


def test_missing_trend_does_not_block_sin_snapshot():
    o = obj("sin_20260929T1024Z")
    del o["trend"]
    rows, trend, drift, _ = parse_endpoint("sin", _enc(o), fetched_at_utc=FETCHED)
    assert trend == []
    assert any(d.path == "trend" and d.kind == "missing_key" for d in drift)
    assert {"sin.data", "sin.water", "sin.max"} <= {r["stream"] for r in rows}


def test_one_broken_endpoint_does_not_stop_the_others(chunk_dir, data_dir):
    bodies = {ep: raw(f) for ep, f in ALL_1024.items()}
    bodies["vert"] = b'{"flow": [ {"name": "Demanda Total", "value": "13'  # truncated JSON
    make_chunk(chunk_dir, "20260929T102401Z", bodies)
    res = consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    streams = {p.name for p in (data_dir / "snapshots").iterdir()}
    assert "gen.unit" in streams and "diagram.units" in streams and "int.nodes" in streams
    assert not any(s.startswith("vert.") for s in streams)
    assert any(d["kind"] == "invalid_json" for d in res.new_drift)
    log = pd.read_csv(data_dir / "manifest" / "drift_log.csv")
    assert (log["kind"] == "invalid_json").any()


def test_corrupt_chunk_is_logged_and_skipped(chunk_dir, data_dir):
    make_chunk(chunk_dir, "20260929T102401Z", {"gen": raw("gen_20260929T1024Z")})
    (chunk_dir / "sitr_20260929T102500Z_snap_ok1of1.tar.gz").write_bytes(b"not a tarball")
    res = consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    assert len(res.ingested) == 2
    assert any(d["kind"] == "chunk_unreadable" for d in res.new_drift)
    assert (data_dir / "snapshots" / "gen.unit").exists()


def test_real_fixtures_have_no_drift():
    for ep, f in ALL_1024.items():
        _, _, drift, _ = parse_endpoint(ep, raw(f), fetched_at_utc=FETCHED)
        assert drift == [], (ep, drift)


def test_special_shapes():
    rows, _, _, _ = parse_endpoint("vert", raw("vert_20260929T1024Z"), fetched_at_utc=FETCHED)
    totals = [r for r in rows if r["entity"] == "Total"]
    assert {r["stream"] for r in totals} == {"vert.plant", "vert.solar", "vert.eolica"}
    final = [r for r in rows if r["stream"] == "vert.final"][0]
    assert final["value"] == 1040.82 and final["value_text"] == "1,040.82"
    rows, _, _, _ = parse_endpoint("gen", raw("gen_20260929T1024Z"), fetched_at_utc=FETCHED)
    hyd = {r["field"]: r["value"] for r in rows if r["stream"] == "gen.pie" and r["entity"] == "Hídrica"}
    assert hyd["value"] == 1062.14 and hyd["share_pct"] == 75.84
    rows, _, _, _ = parse_endpoint("int", raw("int_20260929T1024Z"), fetched_at_utc=FETCHED)
    ch = {r["field"]: r["value"] for r in rows if r["entity"] == "Changuinola -> Cahuita"}
    assert ch == {"from_mw": -4.5, "from_mvar": -18.4, "to_mw": 4.21, "to_mvar": 8.18}
    rows, _, _, _ = parse_endpoint("diagram", raw("diagram_20260929T1024Z"), fetched_at_utc=FETCHED)
    r = next(r for r in rows if r["entity"] == "BAYANOLIN230-1AMW.AV" and r["field"] == "value")
    assert r["timestamp_local"] == datetime(2026, 9, 29, 5, 23, 54, 117000)  # per-tag SCADA time
    rows, _, _, _ = parse_endpoint("flow", raw("flow_20260929T1024Z"), fetched_at_utc=FETCHED)
    assert [r["entity"] for r in rows if r["stream"] == "flow.occi"] == [f"occi[{i}]" for i in range(7)]
