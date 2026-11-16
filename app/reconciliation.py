"""Nightly manifest reconciliation service.

The analyzer hands in a manifest of up to ``reconciliation_max_entries``
distinct sample ids, each with a temperature / ph reading that may be an
explicit JSON ``null``.  The service compares the manifest against *every
currently committed* sample in the acquisition database and produces a report
with three disjoint diff classes, keeping both sides' raw values:

* ``manifest_only``  -- id present in the manifest, absent from the database
* ``database_only``  -- id present in the database, absent from the manifest
* ``value_mismatch`` -- id present on both sides but at least one provided
  reading differs; an explicit JSON ``null`` is a value and is distinct from a
  reading that the manifest simply omitted.

Concurrency contract
====================
A monotonic ``db_revision`` counter is bumped by every committed write to the
logical ``samples`` dataset (wide-table triggers before cutover, INSTEAD OF
view triggers afterwards, and the cutover transaction itself).  A job records
the revision at submission and re-reads it inside the short publish
transaction: if any write committed in the meantime -- even one that changes a
value back to what it was -- the report is **not** published.  The heavy
comparison runs on a read-only snapshot connection (WAL: never blocks writers);
write transactions are only used for short, chunk-sized inserts and the final
publish, so no write lock is held for the duration of a reconciliation.

Failure atomicity
=================
Diffs are staged in ``reconciliation_diffs`` while the job is ``pending`` and
only become readable when the job reaches ``published``.  Aborted and failed
runs have their staged rows deleted, so an unsuccessful reconciliation leaves
no half result behind.  Published reports are never deleted: later writes mark
them ``stale`` but their content stays queryable.
"""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    StrictInt,
    field_validator,
)
from typing_extensions import Annotated

from .config import Settings
from .db import connect, transaction
from .schema import (
    create_reconciliation_schema,
    ensure_revision_tracking,
    ensure_wide_table,
)

logger = logging.getLogger("reconciliation")

METRICS = ("temperature", "ph")
TERMINAL_STATUSES = ("published", "aborted", "failed")


def _coerce_reading(value: Any) -> Any:
    """Reject booleans and non-numeric JSON values before Pydantic coerces
    them; pass through ``None`` (explicit JSON null).  Strings are rejected so
    the API only accepts real JSON numbers.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, type(None))):
        raise ValueError("reading must be a number or null")
    return value


# Order matters: the before-validator runs first (bool rejection), then the
# float annotation, then the finite-check field validator.
NullableReading = Annotated[float | None, BeforeValidator(_coerce_reading)]


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class ReconciliationError(RuntimeError):
    """Domain error surfaced to API clients."""


class ReportNotFound(LookupError):
    pass


class ManifestEntry(BaseModel):
    """One analyzer reading.

    ``temperature`` / ``ph`` may be omitted or explicitly ``null``; the
    distinction is preserved through ``model_fields_set`` and stored as the
    ``*_present`` flags.
    """

    sample_id: StrictInt
    temperature: NullableReading = None
    ph: NullableReading = None

    @field_validator("sample_id")
    @classmethod
    def _id_is_real_integer(cls, value: Any) -> int:
        # StrictInt already rejects bools and non-integers; kept as a guard.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("sample_id must be an integer")
        return value

    @field_validator("temperature", "ph")
    @classmethod
    def _finite_or_none(cls, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("reading must be a number or null")
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("reading must be finite")
        return value


class ReconciliationRequest(BaseModel):
    entries: list[ManifestEntry] = Field(min_length=1)


class ReconciliationService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._threads: dict[str, threading.Thread] = {}
        self._threads_lock = threading.Lock()

    # ------------------------------------------------------------------ setup
    def initialize(self) -> None:
        """Create reconciliation tables and revision tracking if missing.

        A ``pending`` row left by a process that died mid-run can never
        finish (its worker thread is gone): mark it failed and delete its
        staged rows so no half result remains observable.
        """
        conn = connect(self.settings)
        try:
            with transaction(conn):
                create_reconciliation_schema(conn)
                ensure_revision_tracking(conn)
                # The migration engine normally creates / seeds the wide table
                # first; guarantee a readable ``samples`` exists even when this
                # service is used standalone on a fresh database.
                ensure_wide_table(conn)
                stale = conn.execute(
                    "SELECT id FROM reconciliation_runs WHERE status='pending'"
                ).fetchall()
                for row in stale:
                    run_id = row["id"]
                    self._delete_staged(conn, run_id)
                    conn.execute(
                        "UPDATE reconciliation_runs SET status='failed',"
                        " finished_at=?, final_revision=?, failure_reason=?"
                        " WHERE id=?",
                        (
                            utcnow(),
                            self._read_revision(conn),
                            "interrupted by process shutdown before publication",
                            run_id,
                        ),
                    )
        finally:
            conn.close()

    def wait_for_idle(self, timeout: float | None = None) -> None:
        with self._threads_lock:
            threads = list(self._threads.values())
        for thread in threads:
            thread.join(timeout)

    # ------------------------------------------------------------- validation
    def _validate_manifest(
        self, entries: list[ManifestEntry]
    ) -> list[ManifestEntry]:
        limit = self.settings.reconciliation_max_entries
        if len(entries) > limit:
            raise ReconciliationError(
                f"manifest has {len(entries)} entries; at most {limit} are allowed"
            )
        seen: set[int] = set()
        duplicates: list[int] = []
        for entry in entries:
            if entry.sample_id in seen:
                duplicates.append(entry.sample_id)
            seen.add(entry.sample_id)
        if duplicates:
            shown = ", ".join(str(i) for i in duplicates[:5])
            raise ReconciliationError(
                f"manifest sample ids must be distinct; duplicate(s): {shown}"
            )
        return entries

    # ------------------------------------------------------------ submission
    def submit(self, entries: list[ManifestEntry]) -> dict[str, Any]:
        """Validate and persist a manifest, then start the comparison worker."""
        entries = self._validate_manifest(entries)
        run_uid = str(uuid.uuid4())
        now = utcnow()
        conn = connect(self.settings)
        try:
            with transaction(conn):
                create_reconciliation_schema(conn)
                ensure_revision_tracking(conn)
                ensure_wide_table(conn)
                base_revision = self._read_revision(conn)
                cur = conn.execute(
                    "INSERT INTO reconciliation_runs"
                    " (run_uid, status, requested_at, base_revision,"
                    "  manifest_count) VALUES (?, 'pending', ?, ?, ?)",
                    (run_uid, now, base_revision, len(entries)),
                )
                run_id = int(cur.lastrowid)
                conn.executemany(
                    "INSERT INTO reconciliation_manifest"
                    " (run_id, sample_id, temperature, ph, temp_present,"
                    "  ph_present) VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            run_id,
                            e.sample_id,
                            e.temperature,
                            e.ph,
                            1 if "temperature" in e.model_fields_set else 0,
                            1 if "ph" in e.model_fields_set else 0,
                        )
                        for e in entries
                    ],
                )
        finally:
            conn.close()
        thread = threading.Thread(
            target=self._run_job,
            name=f"reconciliation-{run_uid[:8]}",
            args=(run_id,),
            daemon=True,
        )
        with self._threads_lock:
            self._threads[run_uid] = thread
        thread.start()
        return self.get_run(run_uid)

    # --------------------------------------------------------------- worker
    def _run_job(self, run_id: int) -> None:
        try:
            self._compare(run_id)
        except Exception as exc:  # noqa: BLE001 - reported on the run row
            logger.exception("reconciliation run %s failed", run_id)
            self._record_failure(run_id, str(exc))

    # Test seam: override to act (e.g. commit a legacy write) while the
    # read-only snapshot is open and before publish.
    def _tick(self, run_id: int) -> None:
        return None

    def _compare(self, run_id: int) -> None:
        """Snapshot-read the database, stage diffs, then publish or abort."""
        conn = connect(self.settings)
        try:
            conn.execute("BEGIN IMMEDIATE")
            run = self._fetch_run(conn, run_id)
            if run is None or run["status"] != "pending":
                conn.execute("ROLLBACK")
                return
            base_revision = run["base_revision"]
            manifest_rows = conn.execute(
                "SELECT sample_id, temperature, ph, temp_present, ph_present"
                " FROM reconciliation_manifest WHERE run_id = ?"
                " ORDER BY sample_id",
                (run_id,),
            ).fetchall()
            is_view = self._samples_is_view(conn)
            source = "compatibility_view" if is_view else "wide_table"
            conn.execute(
                "UPDATE reconciliation_runs SET started_at=?, source=?"
                " WHERE id=?",
                (utcnow(), source, run_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        manifest = {r["sample_id"]: r for r in manifest_rows}

        # One read-only transaction gives a single committed snapshot for the
        # whole comparison.  It never takes a write lock, so legacy writes
        # keep flowing; the revision check at publish decides whether they
        # invalidate this snapshot.
        reader = connect(self.settings, read_only=True)
        staged: list[tuple[Any, ...]] = []
        try:
            reader.execute("BEGIN")
            try:
                # Sorted merge walk of manifest ids vs database ids.
                db_cursor = reader.execute(
                    "SELECT id, temperature, ph FROM samples ORDER BY id"
                )
                chunk = self.settings.reconciliation_chunk_size
                manifest_only = mismatch = database_only = 0
                db_row = db_cursor.fetchone()
                for sample_id in sorted(manifest):
                    m = manifest[sample_id]
                    while db_row is not None and db_row["id"] < sample_id:
                        staged.append(
                            (
                                run_id,
                                db_row["id"],
                                "database_only",
                                None,
                                None,
                                0,
                                0,
                                db_row["temperature"],
                                db_row["ph"],
                            )
                        )
                        database_only += 1
                        db_row = db_cursor.fetchone()
                        if len(staged) >= chunk:
                            self._flush_diffs(staged)
                            staged.clear()
                    if db_row is not None and db_row["id"] == sample_id:
                        if self._values_differ(m, db_row):
                            staged.append(
                                (
                                    run_id,
                                    sample_id,
                                    "value_mismatch",
                                    m["temperature"],
                                    m["ph"],
                                    m["temp_present"],
                                    m["ph_present"],
                                    db_row["temperature"],
                                    db_row["ph"],
                                )
                            )
                            mismatch += 1
                        db_row = db_cursor.fetchone()
                    else:
                        staged.append(
                            (
                                run_id,
                                sample_id,
                                "manifest_only",
                                m["temperature"],
                                m["ph"],
                                m["temp_present"],
                                m["ph_present"],
                                None,
                                None,
                            )
                        )
                        manifest_only += 1
                    if len(staged) >= chunk:
                        self._flush_diffs(staged)
                        staged.clear()
                # Remaining database ids are all database-only.
                while db_row is not None:
                    staged.append(
                        (
                            run_id,
                            db_row["id"],
                            "database_only",
                            None,
                            None,
                            0,
                            0,
                            db_row["temperature"],
                            db_row["ph"],
                        )
                    )
                    database_only += 1
                    db_row = db_cursor.fetchone()
                    if len(staged) >= chunk:
                        self._flush_diffs(staged)
                        staged.clear()
                # Injection point for deterministic concurrency tests.
                self._tick(run_id)
            finally:
                if reader.in_transaction:
                    reader.execute("COMMIT")
        finally:
            reader.close()
        if staged:
            self._flush_diffs(staged)
            staged.clear()

        self._publish_or_abort(
            run_id,
            base_revision=base_revision,
            counts=(manifest_only, database_only, mismatch),
        )

    @staticmethod
    def _values_differ(manifest_row: sqlite3.Row, db_row: sqlite3.Row) -> bool:
        """Only readings the manifest actually provided participate in the
        comparison; an omitted reading can never mismatch.  Explicit null is a
        real value and differs from a non-null database value.
        """
        if manifest_row["temp_present"]:
            if not _sql_equal(manifest_row["temperature"], db_row["temperature"]):
                return True
        if manifest_row["ph_present"]:
            if not _sql_equal(manifest_row["ph"], db_row["ph"]):
                return True
        return False

    def _flush_diffs(self, rows: list[tuple[Any, ...]]) -> None:
        """Persist one chunk of staged diffs in a short write transaction."""
        conn = connect(self.settings)
        try:
            with transaction(conn):
                run = conn.execute(
                    "SELECT status FROM reconciliation_runs WHERE id=?",
                    (rows[0][0],),
                ).fetchone()
                if run is None or run["status"] != "pending":
                    raise ReconciliationError(
                        "reconciliation run is no longer active"
                    )
                conn.executemany(
                    "INSERT INTO reconciliation_diffs"
                    " (run_id, sample_id, diff_type, manifest_temperature,"
                    "  manifest_ph, manifest_temp_present, manifest_ph_present,"
                    "  database_temperature, database_ph)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
        finally:
            conn.close()

    def _publish_or_abort(
        self,
        run_id: int,
        *,
        base_revision: int,
        counts: tuple[int, int, int],
    ) -> None:
        """Atomic decision point.

        A single short write transaction re-reads the revision.  A changed
        revision means a committed write raced the snapshot: the run is marked
        ``aborted`` and every staged row is deleted inside the same
        transaction, so the failed reconciliation leaves nothing behind.
        """
        manifest_only, database_only, mismatch = counts
        conn = connect(self.settings)
        try:
            with transaction(conn):
                run = self._fetch_run(conn, run_id)
                if run is None or run["status"] != "pending":
                    return
                current_revision = self._read_revision(conn)
                if current_revision != base_revision:
                    self._delete_staged(conn, run_id)
                    conn.execute(
                        "UPDATE reconciliation_runs SET status='aborted',"
                        " finished_at=?, final_revision=?, failure_reason=?"
                        " WHERE id=?",
                        (
                            utcnow(),
                            current_revision,
                            "committed write detected during reconciliation"
                            f" (revision {base_revision} -> {current_revision})",
                            run_id,
                        ),
                    )
                    return
                conn.execute(
                    "UPDATE reconciliation_runs SET status='published',"
                    " finished_at=?, final_revision=?, published_revision=?,"
                    " diff_manifest_only=?, diff_database_only=?,"
                    " diff_value_mismatch=? WHERE id=?",
                    (
                        utcnow(),
                        current_revision,
                        current_revision,
                        manifest_only,
                        database_only,
                        mismatch,
                        run_id,
                    ),
                )
        finally:
            conn.close()

    def _record_failure(self, run_id: int, message: str) -> None:
        try:
            conn = connect(self.settings)
        except Exception:  # pragma: no cover - DB unavailable
            logger.exception("cannot connect to record reconciliation failure")
            return
        try:
            with transaction(conn):
                run = self._fetch_run(conn, run_id)
                if run is None or run["status"] in TERMINAL_STATUSES:
                    return
                self._delete_staged(conn, run_id)
                conn.execute(
                    "UPDATE reconciliation_runs SET status='failed',"
                    " finished_at=?, final_revision=?, failure_reason=?"
                    " WHERE id=?",
                    (
                        utcnow(),
                        self._read_revision(conn),
                        message[:2000],
                        run_id,
                    ),
                )
        except Exception:  # pragma: no cover - best effort
            logger.exception("cannot record reconciliation failure")
        finally:
            conn.close()

    # --------------------------------------------------------------- reads
    @contextmanager
    def _read_snapshot(self):
        """One read-only transaction: a consistent committed snapshot that can
        never take a write lock or block legacy writers (WAL)."""
        conn = connect(self.settings, read_only=True)
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            if conn.in_transaction:
                conn.execute("COMMIT")
            conn.close()

    def get_run(self, run_uid: str) -> dict[str, Any]:
        with self._read_snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM reconciliation_runs WHERE run_uid = ?",
                (run_uid,),
            ).fetchone()
            if row is None:
                raise ReportNotFound(f"reconciliation report {run_uid} not found")
            payload = self._run_payload(conn, row)
        self._persist_staleness_if_needed(row, payload["stale"])
        return payload

    def list_diffs(
        self,
        run_uid: str,
        *,
        diff_type: str | None = None,
        after_id: int | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Keyset-paginated diffs with both sides' raw values.

        Published reports are served as stored (``stale`` only flags them);
        pending / aborted / failed runs expose no diff rows at all.
        """
        max_page = self.settings.reconciliation_diff_page_max
        if isinstance(limit, bool) or not isinstance(limit, int) or not (
            1 <= limit <= max_page
        ):
            raise ReconciliationError(f"limit must be between 1 and {max_page}")
        if after_id is not None and (
            isinstance(after_id, bool) or not isinstance(after_id, int)
        ):
            raise ReconciliationError("after_id must be an integer")
        if diff_type is not None and diff_type not in (
            "manifest_only",
            "database_only",
            "value_mismatch",
        ):
            raise ReconciliationError(
                "diff_type must be one of manifest_only, database_only,"
                " value_mismatch"
            )
        with self._read_snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM reconciliation_runs WHERE run_uid = ?",
                (run_uid,),
            ).fetchone()
            if row is None:
                raise ReportNotFound(
                    f"reconciliation report {run_uid} not found"
                )
            if row["status"] != "published":
                payload = self._run_payload(conn, row)
                result = {
                    "report": payload,
                    "diff_type": diff_type,
                    "items": [],
                    "count": 0,
                    "next_after_id": None,
                    "has_more": False,
                }
            else:
                sql = (
                    "SELECT sample_id, diff_type, manifest_temperature,"
                    " manifest_ph, manifest_temp_present, manifest_ph_present,"
                    " database_temperature, database_ph"
                    " FROM reconciliation_diffs WHERE run_id = ?"
                )
                params: list[Any] = [row["id"]]
                if diff_type is not None:
                    sql += " AND diff_type = ?"
                    params.append(diff_type)
                if after_id is not None:
                    sql += " AND sample_id > ?"
                    params.append(after_id)
                sql += " ORDER BY sample_id LIMIT ?"
                params.append(limit + 1)
                rows = conn.execute(sql, params).fetchall()
                has_more = len(rows) > limit
                rows = rows[:limit]
                next_after = rows[-1]["sample_id"] if has_more and rows else None
                payload = self._run_payload(conn, row)
                result = {
                    "report": payload,
                    "diff_type": diff_type,
                    "items": [self._diff_payload(r) for r in rows],
                    "count": len(rows),
                    "next_after_id": next_after,
                    "has_more": has_more,
                }
        # Best-effort flag persistence outside the read snapshot.
        self._persist_staleness_if_needed(row, payload["stale"])
        return result

    def _persist_staleness_if_needed(
        self, row: sqlite3.Row, stale: bool
    ) -> None:
        """Persist the lazily-computed stale flag in a short write transaction.

        Kept out of the read path so report reads never upgrade to a write
        lock.  A failure here is invisible to clients (the flag is recomputed
        on every read anyway).
        """
        if not stale or row["stale"]:
            return
        try:
            conn = connect(self.settings)
        except Exception:  # pragma: no cover - DB unavailable
            return
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE reconciliation_runs SET stale=1"
                    " WHERE id=? AND stale=0",
                    (row["id"],),
                )
        except Exception:  # pragma: no cover - best effort
            logger.debug("could not persist stale flag", exc_info=True)
        finally:
            conn.close()

    def _run_payload(
        self, conn: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        current_revision = self._read_revision(conn)
        # Lazy staleness: a report published before the latest committed write
        # is stale, but its stored content remains fully queryable.
        stale = bool(row["stale"]) or (
            row["status"] == "published"
            and row["published_revision"] is not None
            and current_revision > row["published_revision"]
        )
        return {
            "report_id": row["run_uid"],
            "status": row["status"],
            "requested_at": row["requested_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "source": row["source"],
            "base_revision": row["base_revision"],
            "final_revision": row["final_revision"],
            "published_revision": row["published_revision"],
            "current_revision": current_revision,
            "stale": stale,
            "manifest_count": row["manifest_count"],
            "diff_counts": {
                "manifest_only": row["diff_manifest_only"],
                "database_only": row["diff_database_only"],
                "value_mismatch": row["diff_value_mismatch"],
                "total": row["diff_manifest_only"]
                + row["diff_database_only"]
                + row["diff_value_mismatch"],
            },
            "failure_reason": row["failure_reason"],
        }

    @staticmethod
    def _diff_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "sample_id": row["sample_id"],
            "diff_type": row["diff_type"],
            "manifest": {
                "temperature": row["manifest_temperature"],
                "ph": row["manifest_ph"],
                "temperature_present": bool(row["manifest_temp_present"]),
                "ph_present": bool(row["manifest_ph_present"]),
            },
            "database": {
                "temperature": row["database_temperature"],
                "ph": row["database_ph"],
            },
        }

    # -------------------------------------------------------------- helpers
    @staticmethod
    def _fetch_run(
        conn: sqlite3.Connection, run_id: int
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM reconciliation_runs WHERE id = ?", (run_id,)
        ).fetchone()

    @staticmethod
    def _read_revision(conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT revision FROM db_revision WHERE id = 1"
        ).fetchone()
        if row is None:  # pragma: no cover - ensure_revision_tracking seeds it
            raise ReconciliationError("db_revision counter is not initialized")
        return int(row["revision"])

    @staticmethod
    def _delete_staged(conn: sqlite3.Connection, run_id: int) -> None:
        conn.execute(
            "DELETE FROM reconciliation_diffs WHERE run_id = ?", (run_id,)
        )
        conn.execute(
            "DELETE FROM reconciliation_manifest WHERE run_id = ?", (run_id,)
        )

    @staticmethod
    def _samples_is_view(conn: sqlite3.Connection) -> bool:
        row = conn.execute(
            "SELECT type FROM sqlite_master WHERE name = 'samples'"
        ).fetchone()
        return row is not None and row["type"] == "view"


def _sql_equal(left: Any, right: Any) -> bool:
    """SQL-style equality: NULL = NULL is true here (both readings missing),
    NULL vs number is false.  REAL comparison uses Python float equality.
    """
    if left is None or right is None:
        return left is None and right is None
    return float(left) == float(right)
