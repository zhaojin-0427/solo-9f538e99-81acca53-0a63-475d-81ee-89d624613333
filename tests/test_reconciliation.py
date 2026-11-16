"""Tests for the nightly manifest reconciliation API and service."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

import legacy.acquisition as legacy
from app.config import Settings
from app.db import connect
from app.main import create_app
from app.reconciliation import ManifestEntry, ReconciliationService


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def wait_terminal(client: TestClient, report_id: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    report = None
    while time.monotonic() < deadline:
        report = client.get(f"/reconciliations/{report_id}").json()["report"]
        if report["status"] in {"published", "aborted", "failed"}:
            return report
        time.sleep(0.01)
    raise AssertionError(f"report never reached a terminal state: {report}")


def submit(client: TestClient, entries: list[dict]) -> str:
    resp = client.post("/reconciliations", json={"entries": entries})
    assert resp.status_code == 202, resp.text
    return resp.json()["report"]["report_id"]


# --------------------------------------------------------------- happy paths
def test_full_reconciliation_classifies_all_three_diff_types(client):
    # Seed has 18 rows (ids 1..42 with gaps).  Manifest:
    #  - id 1 with a wrong temperature -> value_mismatch
    #  - id 3 with explicit null temperature (matches DB NULL) -> no diff
    #  - id 999 not in the DB -> manifest_only
    #  - every other DB id -> database_only
    entries = [
        {"sample_id": 1, "temperature": 1.0, "ph": 7.2},
        {"sample_id": 3, "temperature": None, "ph": 7.05},
        {"sample_id": 999, "temperature": 36.0},
    ]
    report_id = submit(client, entries)
    report = wait_terminal(client, report_id)
    assert report["status"] == "published"
    assert report["stale"] is False
    assert report["source"] == "wide_table"
    assert report["diff_counts"] == {
        "manifest_only": 1,
        "database_only": 16,
        "value_mismatch": 1,
        "total": 18,
    }

    diffs = client.get(
        f"/reconciliations/{report_id}/diffs", params={"limit": 100}
    ).json()
    assert diffs["count"] == 18
    assert diffs["has_more"] is False
    by_id = {d["sample_id"]: d for d in diffs["items"]}

    mismatch = by_id[1]
    assert mismatch["diff_type"] == "value_mismatch"
    assert mismatch["manifest"] == {
        "temperature": 1.0,
        "ph": 7.2,
        "temperature_present": True,
        "ph_present": True,
    }
    assert mismatch["database"] == {"temperature": 36.5, "ph": 7.2}

    only_manifest = by_id[999]
    assert only_manifest["diff_type"] == "manifest_only"
    assert only_manifest["manifest"]["temperature"] == 36.0
    assert only_manifest["manifest"]["temperature_present"] is True
    assert only_manifest["manifest"]["ph_present"] is False
    assert only_manifest["database"] == {"temperature": None, "ph": None}

    only_db = by_id[7]
    assert only_db["diff_type"] == "database_only"
    assert only_db["manifest"]["temperature_present"] is False
    assert only_db["manifest"]["ph_present"] is False
    # raw DB values preserved, including NULLs
    assert only_db["database"] == {"temperature": None, "ph": None}

    # id 3 matched (explicit null == SQL NULL) and must not appear
    assert 3 not in by_id


def test_explicit_null_distinct_from_missing_field(client):
    # id 4 in seed: temperature=36.9, ph=NULL
    entries = [
        # explicit null on ph equals DB NULL -> no mismatch
        {"sample_id": 4, "temperature": 36.9, "ph": None},
        # omitted temperature cannot mismatch; provided ph mismatches
        {"sample_id": 5, "ph": 1.23},
    ]
    report_id = submit(client, entries)
    report = wait_terminal(client, report_id)
    assert report["status"] == "published"
    assert report["diff_counts"]["value_mismatch"] == 1
    body = client.get(
        f"/reconciliations/{report_id}/diffs",
        params={"diff_type": "value_mismatch"},
    ).json()
    diffs = body["items"]
    assert [d["sample_id"] for d in diffs] == [5]
    d5 = diffs[0]
    assert d5["manifest"]["ph_present"] is True
    assert d5["manifest"]["temperature_present"] is False
    assert d5["database"]["ph"] == 7.41
    # id 4 is not a diff of any kind
    all_ids = {
        d["sample_id"]
        for d in client.get(
            f"/reconciliations/{report_id}/diffs", params={"limit": 100}
        ).json()["items"]
    }
    assert 4 not in all_ids


def test_identical_manifest_publishes_empty_report(client):
    entries = [
        {"sample_id": 1, "temperature": 36.5, "ph": 7.2},
        {"sample_id": 7, "temperature": None, "ph": None},
        {"sample_id": 3, "ph": 7.05, "temperature": None},
    ]
    report_id = submit(client, entries)
    report = wait_terminal(client, report_id)
    assert report["status"] == "published"
    assert report["diff_counts"]["total"] == 15  # only database-only rows
    body = client.get(f"/reconciliations/{report_id}/diffs").json()
    assert {d["diff_type"] for d in body["items"]} == {"database_only"}


def test_diff_pagination_keyset_and_type_filter(client):
    entries = [{"sample_id": 1, "temperature": 0.1}]
    report_id = submit(client, entries)
    wait_terminal(client, report_id)

    pages: list[int] = []
    cursor = None
    while True:
        params = {"limit": 4}
        if cursor is not None:
            params["after_id"] = cursor
        body = client.get(
            f"/reconciliations/{report_id}/diffs", params=params
        ).json()
        ids = [i["sample_id"] for i in body["items"]]
        assert ids == sorted(ids)
        pages.extend(ids)
        cursor = body["next_after_id"]
        if not body["has_more"]:
            assert body["next_after_id"] is None
            break
    assert pages == sorted(pages)
    assert len(pages) == len(set(pages)) == 18

    only_db = client.get(
        f"/reconciliations/{report_id}/diffs",
        params={"diff_type": "database_only", "limit": 100},
    ).json()
    assert only_db["count"] == 17
    assert {i["diff_type"] for i in only_db["items"]} == {"database_only"}

    bad = client.get(
        f"/reconciliations/{report_id}/diffs", params={"diff_type": "nope"}
    )
    assert bad.status_code == 422
    assert client.get(
        f"/reconciliations/{report_id}/diffs", params={"limit": 0}
    ).status_code == 422
    assert client.get(
        f"/reconciliations/{report_id}/diffs", params={"limit": 1001}
    ).status_code == 422


# --------------------------------------------------------------- validation
def test_manifest_validation_failures(client):
    cases = [
        {"entries": []},  # empty
        {"entries": [{"sample_id": 1}, {"sample_id": 1}]},  # duplicate
        {"entries": [{"sample_id": True}]},  # bool id
        {"entries": [{"sample_id": 1.5}]},  # float id
        {"entries": [{"sample_id": "1"}]},  # string id
        {"entries": [{"sample_id": 1, "temperature": True}]},  # bool reading
        {"entries": [{"sample_id": 1, "ph": "x"}]},  # string reading
    ]
    for payload in cases:
        resp = client.post("/reconciliations", json=payload)
        assert resp.status_code == 422, payload

    for raw in (
        b'{"entries":[{"sample_id":1,"temperature": NaN}]}',
        b'{"entries":[{"sample_id":1,"ph": Infinity}]}',
    ):
        resp = client.post(
            "/reconciliations",
            content=raw,
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 422


def test_manifest_over_5000_entries_rejected(client):
    payload = {"entries": [{"sample_id": i} for i in range(5001)]}
    resp = client.post("/reconciliations", json=payload)
    assert resp.status_code == 422
    assert "5000" in resp.json()["detail"]


def test_manifest_exactly_5000_entries_accepted(client, settings):
    # Manifest ids high enough that none collide with the 18 seeded rows.
    entries = [{"sample_id": 10_000 + i} for i in range(5000)]
    report_id = submit(client, entries)
    report = wait_terminal(client, report_id, timeout=60)
    assert report["status"] == "published"
    assert report["manifest_count"] == 5000
    assert report["diff_counts"]["manifest_only"] == 5000
    assert report["diff_counts"]["database_only"] == 18


def test_unknown_report_returns_404(client):
    assert client.get("/reconciliations/does-not-exist").status_code == 404
    assert (
        client.get("/reconciliations/does-not-exist/diffs").status_code == 404
    )


# ------------------------------------------------- concurrency / staleness
def test_concurrent_write_aborts_publication_and_cleans_up(client, settings):
    service: ReconciliationService = client.app.state.reconciliation

    def write_during_snapshot(run_id: int) -> None:
        conn = legacy.open_db(settings.database_path)
        try:
            legacy.update(conn, 1, temperature=40.0)
        finally:
            conn.close()

    service._tick = write_during_snapshot  # type: ignore[method-assign]
    try:
        report_id = submit(client, [{"sample_id": 1, "temperature": 36.5}])
        report = wait_terminal(client, report_id)
    finally:
        del service._tick

    assert report["status"] == "aborted"
    assert "revision" in (report["failure_reason"] or "")
    assert report["final_revision"] > report["base_revision"]
    # No half result: no diffs are readable and no staged rows remain.
    body = client.get(f"/reconciliations/{report_id}/diffs").json()
    assert body["items"] == []
    assert body["report"]["status"] == "aborted"
    db = connect(settings)
    try:
        run_row = db.execute(
            "SELECT id FROM reconciliation_runs WHERE run_uid=?", (report_id,)
        ).fetchone()
        leftover_diffs = db.execute(
            "SELECT COUNT(*) AS n FROM reconciliation_diffs WHERE run_id=?",
            (run_row["id"],),
        ).fetchone()["n"]
        leftover_manifest = db.execute(
            "SELECT COUNT(*) AS n FROM reconciliation_manifest WHERE run_id=?",
            (run_row["id"],),
        ).fetchone()["n"]
        assert leftover_diffs == 0
        assert leftover_manifest == 0
    finally:
        db.close()


def test_value_changed_back_still_aborts(client, settings):
    """A write that restores the original value still bumps the revision and
    must keep the report unpublished."""
    service: ReconciliationService = client.app.state.reconciliation

    def flip_and_flop(run_id: int) -> None:
        conn = legacy.open_db(settings.database_path)
        try:
            legacy.update(conn, 1, temperature=99.0)
            legacy.update(conn, 1, temperature=36.5)  # back to original
        finally:
            conn.close()

    service._tick = flip_and_flop  # type: ignore[method-assign]
    try:
        report_id = submit(
            client, [{"sample_id": 1, "temperature": 36.5, "ph": 7.2}]
        )
        report = wait_terminal(client, report_id)
    finally:
        del service._tick
    assert report["status"] == "aborted"
    assert report["final_revision"] > report["base_revision"]


def test_published_report_marked_stale_but_remains_readable(client, settings):
    report_id = submit(
        client, [{"sample_id": 1, "temperature": 0.1, "ph": 7.2}]
    )
    report = wait_terminal(client, report_id)
    assert report["stale"] is False

    conn = legacy.open_db(settings.database_path)
    try:
        legacy.insert(conn, sample_id=777, temperature=1.0, ph=2.0)
    finally:
        conn.close()

    again = client.get(f"/reconciliations/{report_id}").json()["report"]
    assert again["stale"] is True
    assert again["status"] == "published"
    # Content is frozen at publish time and still fully readable.
    diffs = client.get(
        f"/reconciliations/{report_id}/diffs", params={"limit": 100}
    ).json()
    assert diffs["report"]["stale"] is True
    assert diffs["count"] == report["diff_counts"]["total"]
    assert 777 not in {d["sample_id"] for d in diffs["items"]}

    db = connect(settings)
    try:
        assert db.execute(
            "SELECT stale FROM reconciliation_runs WHERE run_uid=?",
            (report_id,),
        ).fetchone()["stale"] == 1
    finally:
        db.close()


def test_reconciliation_does_not_block_legacy_writes(client, settings):
    """While a large reconciliation runs, legacy commits keep succeeding."""
    service: ReconciliationService = client.app.state.reconciliation
    gate = _Gate()

    def gate_tick(run_id: int) -> None:
        gate.reached.set()
        assert gate.proceed.wait(timeout=10)

    service._tick = gate_tick  # type: ignore[method-assign]
    entries = [{"sample_id": 100_000 + i} for i in range(3000)]
    try:
        report_id = submit(client, entries)
        assert gate.reached.wait(timeout=10)
        # Snapshot transaction is open: writes must not be blocked.
        conn = legacy.open_db(settings.database_path)
        try:
            new_id = legacy.insert(conn, temperature=3.0, ph=4.0)
            legacy.update(conn, new_id, ph=5.0)
            assert legacy.select(conn, new_id)["ph"] == 5.0
        finally:
            conn.close()
        gate.proceed.set()
        report = wait_terminal(client, report_id)
    finally:
        del service._tick
    # The concurrent write invalidates publication rather than blocking.
    assert report["status"] == "aborted"


class _Gate:
    def __init__(self) -> None:
        import threading

        self.reached = threading.Event()
        self.proceed = threading.Event()


# ------------------------------------------------- migration integration
def test_reconciliation_works_after_cutover_and_revision_keeps_bumping(client):
    client.post("/migrations")
    state = wait_for_migration(client)
    assert state["state"] == "completed"

    db = connect(client.app.state.settings)
    try:
        assert db.execute(
            "SELECT type FROM sqlite_master WHERE name='samples'"
        ).fetchone()["type"] == "view"
    finally:
        db.close()

    report_id = submit(
        client, [{"sample_id": 1, "temperature": 1.0, "ph": 7.2}]
    )
    report = wait_terminal(client, report_id)
    assert report["status"] == "published"
    assert report["source"] == "compatibility_view"
    assert report["diff_counts"]["value_mismatch"] == 1

    # A legacy write through the compatibility view bumps the revision and
    # stales the published report.
    conn = legacy.open_db(client.app.state.settings.database_path)
    try:
        legacy.update(conn, 2, temperature=33.3)
    finally:
        conn.close()
    stale = client.get(f"/reconciliations/{report_id}").json()["report"]
    assert stale["stale"] is True


def test_cutover_during_reconciliation_aborts_it(client, settings):
    from app.migration import MigrationEngine

    service: ReconciliationService = client.app.state.reconciliation
    gate = _Gate()
    service._tick = lambda run_id: (  # type: ignore[method-assign]
        gate.reached.set(),
        gate.proceed.wait(timeout=10),
    )
    try:
        report_id = submit(
            client, [{"sample_id": i} for i in range(1, 19)]
        )
        assert gate.reached.wait(timeout=10)
        # Kick off the migration while the reconciliation snapshot is open.
        engine: MigrationEngine = client.app.state.engine
        engine.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if engine.get_state()["state"] == "completed":
                break
            time.sleep(0.02)
        assert engine.get_state()["state"] == "completed"
        gate.proceed.set()
        report = wait_terminal(client, report_id)
    finally:
        del service._tick
    assert report["status"] == "aborted"


def wait_for_migration(client: TestClient, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = client.get("/migrations/current").json()["migration"]
        if state["state"] in {"completed", "failed"}:
            return state
        time.sleep(0.02)
    raise AssertionError(f"migration did not finish: {state}")


def test_revision_counter_bumps_on_every_legacy_write(client, settings):
    db = connect(settings)
    try:
        before = db.execute("SELECT revision FROM db_revision").fetchone()[
            "revision"
        ]
    finally:
        db.close()
    conn = legacy.open_db(settings.database_path)
    try:
        new_id = legacy.insert(conn, temperature=1.0, ph=2.0)
        legacy.update(conn, new_id, ph=None)
        legacy.delete(conn, new_id)
    finally:
        conn.close()
    db = connect(settings)
    try:
        after = db.execute("SELECT revision FROM db_revision").fetchone()[
            "revision"
        ]
    finally:
        db.close()
    assert after - before == 3


def test_failed_run_records_reason_and_leaves_no_half_result(client, settings, monkeypatch):
    def broken_flush(self, rows):
        raise RuntimeError("simulated staging failure")

    monkeypatch.setattr(ReconciliationService, "_flush_diffs", broken_flush)
    report_id = submit(client, [{"sample_id": 1, "temperature": 0.1}])
    report = wait_terminal(client, report_id)
    assert report["status"] == "failed"
    assert "simulated staging failure" in report["failure_reason"]

    body = client.get(f"/reconciliations/{report_id}/diffs").json()
    assert body["items"] == []
    db = connect(settings)
    try:
        run_row = db.execute(
            "SELECT id FROM reconciliation_runs WHERE run_uid=?", (report_id,)
        ).fetchone()
        assert (
            db.execute(
                "SELECT COUNT(*) AS n FROM reconciliation_diffs WHERE run_id=?",
                (run_row["id"],),
            ).fetchone()["n"]
            == 0
        )
    finally:
        db.close()


def test_concurrent_reconciliations_each_get_own_snapshot(client, settings):
    # Two jobs submitted back-to-back without intervening writes both publish.
    id_a = submit(client, [{"sample_id": 1, "temperature": 36.5, "ph": 7.2}])
    id_b = submit(client, [{"sample_id": 2, "temperature": 36.6, "ph": 7.18}])
    ra = wait_terminal(client, id_a)
    rb = wait_terminal(client, id_b)
    assert ra["status"] == rb["status"] == "published"
    assert ra["report_id"] != rb["report_id"]


def test_diffs_are_sorted_and_page_cursor_stable(client):
    entries = [{"sample_id": 50_000 - i} for i in range(50)]  # reversed input
    report_id = submit(client, entries)
    wait_terminal(client, report_id)
    seen: list[int] = []
    cursor = None
    for _ in range(100):
        params = {"limit": 7}
        if cursor is not None:
            params["after_id"] = cursor
        body = client.get(
            f"/reconciliations/{report_id}/diffs", params=params
        ).json()
        seen.extend(i["sample_id"] for i in body["items"])
        if not body["has_more"]:
            break
        cursor = body["next_after_id"]
    assert seen == sorted(seen)
    assert len(seen) == 68  # 50 manifest-only + 18 database-only


def test_standalone_service_on_fresh_database_does_not_crash(settings):
    """Using ReconciliationService without the migration engine must still
    find a readable (empty) samples table instead of failing the snapshot."""
    standalone = Settings(
        database_path=settings.database_path.parent / "standalone.db",
        seed_database_path=None,
        chunk_size=4,
        chunk_delay_ms=0,
        busy_timeout_ms=10_000,
        auto_resume=False,
    )
    svc = ReconciliationService(standalone)
    svc.initialize()
    run = svc.submit([ManifestEntry(sample_id=1, temperature=1.0, ph=None)])
    svc.wait_for_idle(30)
    final = svc.get_run(run["report_id"])
    assert final["status"] == "published"
    assert final["diff_counts"]["manifest_only"] == 1


def test_interrupted_pending_run_recovered_on_restart(client, settings):
    """A pending row plus staged diffs left by a dead process are reclaimed by
    initialize(): failed status and no half result."""
    from app.reconciliation import utcnow

    db = connect(settings)
    try:
        db.execute("BEGIN IMMEDIATE")
        cur = db.execute(
            "INSERT INTO reconciliation_runs"
            " (run_uid, status, requested_at, base_revision, manifest_count)"
            " VALUES ('orphan-run', 'pending', ?, 0, 1)",
            (utcnow(),),
        )
        orphan_id = cur.lastrowid
        db.execute(
            "INSERT INTO reconciliation_manifest"
            " (run_id, sample_id, temperature, ph, temp_present, ph_present)"
            " VALUES (?, 1, 0.1, NULL, 1, 0)",
            (orphan_id,),
        )
        db.execute(
            "INSERT INTO reconciliation_diffs"
            " (run_id, sample_id, diff_type, manifest_temperature, manifest_ph,"
            "  manifest_temp_present, manifest_ph_present,"
            "  database_temperature, database_ph)"
            " VALUES (?, 1, 'value_mismatch', 0.1, NULL, 1, 0, 36.5, 7.2)",
            (orphan_id,),
        )
        db.execute("COMMIT")
    finally:
        db.close()

    fresh = ReconciliationService(settings)
    fresh.initialize()

    report = client.get("/reconciliations/orphan-run").json()["report"]
    assert report["status"] == "failed"
    assert "shutdown" in (report["failure_reason"] or "")
    assert client.get("/reconciliations/orphan-run/diffs").json()["items"] == []
    db = connect(settings)
    try:
        assert (
            db.execute(
                "SELECT COUNT(*) AS n FROM reconciliation_diffs WHERE run_id=?",
                (orphan_id,),
            ).fetchone()["n"]
            == 0
        )
        assert (
            db.execute(
                "SELECT COUNT(*) AS n FROM reconciliation_manifest WHERE run_id=?",
                (orphan_id,),
            ).fetchone()["n"]
            == 0
        )
        assert (
            db.execute(
                "SELECT COUNT(*) AS n FROM reconciliation_runs WHERE status='pending'"
            ).fetchone()["n"]
            == 0
        )
    finally:
        db.close()
