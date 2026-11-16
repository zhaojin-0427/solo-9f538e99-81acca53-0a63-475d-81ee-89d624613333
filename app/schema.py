"""SQLite schema definitions for the wide-to-long migration.

The legacy acquisition program reads and writes ``samples(id, temperature, ph)``
directly.  While migration is running that table remains a real table.  The
cutover replaces it *inside one transaction* with a view over the long table,
backed by INSTEAD OF triggers, so unmodified legacy SQL keeps working.
"""

# Long (analysis) storage: exactly one row per sample and metric.
LONG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sample_metrics (
    sample_id INTEGER NOT NULL,
    metric    TEXT    NOT NULL CHECK (metric IN ('temperature', 'ph')),
    value     REAL,
    PRIMARY KEY (sample_id, metric)
) WITHOUT ROWID
"""

# Bookkeeping for the one logical migration.  Repeated POST requests must not
# rebuild a completed database, so this is the source of truth for state.
MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS migration_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    run_id         TEXT    NOT NULL,
    state          TEXT    NOT NULL
                   CHECK (state IN ('copying', 'completed', 'failed')),
    created_at     TEXT    NOT NULL,
    started_at     TEXT,
    completed_at   TEXT,
    last_error     TEXT,
    total_samples  INTEGER NOT NULL DEFAULT 0,
    samples_copied INTEGER NOT NULL DEFAULT 0,
    last_id        INTEGER,
    attempt        INTEGER NOT NULL DEFAULT 0
)
"""

MIGRATION_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS migration_events (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT    NOT NULL,
    at     TEXT    NOT NULL,
    event  TEXT    NOT NULL,
    detail TEXT
)
"""

# Helper for the compatibility INSERT trigger: remembers the id allocated when
# the legacy program inserts without an explicit id.  One row only.
INSERT_CONTEXT_SQL = """
CREATE TABLE IF NOT EXISTS _insert_context (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    sample_id INTEGER NOT NULL
)
"""

# Compatibility view: exactly the legacy column names, values and NULLs.
COMPAT_VIEW_SQL = """
CREATE VIEW samples (id, temperature, ph) AS
SELECT
    sample_id,
    MAX(CASE WHEN metric = 'temperature' THEN value END),
    MAX(CASE WHEN metric = 'ph'          THEN value END)
FROM sample_metrics
GROUP BY sample_id
"""

# INSERT: explicit duplicate ids are rejected like the legacy PRIMARY KEY did.
# An omitted / NULL id is allocated as MAX(id)+1, matching SQLite's legacy
# INTEGER PRIMARY KEY behaviour (no AUTOINCREMENT).
#
# Both metric rows always get the same effective id (given or allocated).  The
# one-row helper also lets a legacy program recover the id allocated for an
# INSERT without an id: SQLite restores ``last_insert_rowid()`` when a trigger
# finishes, so ``cursor.lastrowid`` cannot report it.
COMPAT_INSERT_TRIGGER_SQL = """
CREATE TRIGGER samples_insert INSTEAD OF INSERT ON samples
BEGIN
    SELECT RAISE(ABORT, 'UNIQUE constraint failed: samples.id')
    WHERE NEW.id IS NOT NULL
      AND EXISTS (SELECT 1 FROM sample_metrics WHERE sample_id = NEW.id);

    DELETE FROM _insert_context;
    INSERT INTO _insert_context (id, sample_id)
    VALUES (
        1,
        COALESCE(NEW.id, (SELECT COALESCE(MAX(sample_id), 0) + 1 FROM sample_metrics))
    );

    INSERT INTO sample_metrics (sample_id, metric, value)
    SELECT c.sample_id, m.metric,
           CASE m.metric
               WHEN 'temperature' THEN NEW.temperature
               WHEN 'ph'          THEN NEW.ph
           END
    FROM _insert_context AS c
    CROSS JOIN (SELECT 'temperature' AS metric UNION ALL SELECT 'ph') AS m;

    UPDATE db_revision SET revision = revision + 1,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = 1;
END
"""

# UPDATE: changing the id moves the two rows; changing a value overwrites only
# that metric.  Both rows are kept, even with NULL values.
COMPAT_UPDATE_TRIGGER_SQL = """
CREATE TRIGGER samples_update INSTEAD OF UPDATE ON samples
BEGIN
    -- Match the wide table: updating the INTEGER PRIMARY KEY to NULL was a
    -- "datatype mismatch" error there.
    SELECT RAISE(ABORT, 'datatype mismatch') WHERE NEW.id IS NULL;

    SELECT RAISE(ABORT, 'UNIQUE constraint failed: samples.id')
    WHERE NEW.id IS NOT OLD.id
      AND EXISTS (SELECT 1 FROM sample_metrics WHERE sample_id = NEW.id);

    DELETE FROM sample_metrics
    WHERE sample_id = OLD.id AND NEW.id IS NOT OLD.id;

    INSERT INTO sample_metrics (sample_id, metric, value)
    SELECT NEW.id, m.metric,
           CASE m.metric
               WHEN 'temperature' THEN NEW.temperature
               WHEN 'ph'          THEN NEW.ph
           END
    FROM (SELECT 'temperature' AS metric UNION ALL SELECT 'ph') AS m
    WHERE NEW.id IS NOT NULL
    ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;

    UPDATE db_revision SET revision = revision + 1,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = 1;
END
"""

COMPAT_DELETE_TRIGGER_SQL = """
CREATE TRIGGER samples_delete INSTEAD OF DELETE ON samples
BEGIN
    DELETE FROM sample_metrics WHERE sample_id = OLD.id;

    UPDATE db_revision SET revision = revision + 1,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = 1;
END
"""

_COMPAT_DROP_SQL = """
DROP TRIGGER IF EXISTS samples_insert;
DROP TRIGGER IF EXISTS samples_update;
DROP TRIGGER IF EXISTS samples_delete;
DROP VIEW IF EXISTS samples;
"""

# Triggers installed on the real wide table for the duration of the migration.
# Every committed legacy write is mirrored immediately, so a chunk that was
# copied earlier can never be rolled back to a stale value.
MIGRATION_TRIGGERS = (
    """
    CREATE TRIGGER _migration_samples_insert AFTER INSERT ON samples
    BEGIN
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'temperature', NEW.temperature)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'ph', NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
    END
    """,
    """
    CREATE TRIGGER _migration_samples_update AFTER UPDATE ON samples
    BEGIN
        DELETE FROM sample_metrics
        WHERE sample_id = OLD.id AND NEW.id IS NOT OLD.id;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'temperature', NEW.temperature)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'ph', NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
    END
    """,
    """
    CREATE TRIGGER _migration_samples_delete AFTER DELETE ON samples
    BEGIN
        DELETE FROM sample_metrics WHERE sample_id = OLD.id;
    END
    """,
)

_MIGRATION_DROP_SQL = """
DROP TRIGGER IF EXISTS _migration_samples_insert;
DROP TRIGGER IF EXISTS _migration_samples_update;
DROP TRIGGER IF EXISTS _migration_samples_delete;
"""

WIDE_TABLE_SQL = """
CREATE TABLE samples (
    id INTEGER PRIMARY KEY,
    temperature REAL,
    ph REAL
)
"""

# ------------------------------------------------------------------- revision
# Monotonic counter bumped by *every committed write* to the logical samples
# dataset.  Reconciliation jobs record the revision they started from and
# re-check it at publish time: any committed write in between (even one that
# changes a value back to the original) makes the report unpublishable.
DB_REVISION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS db_revision (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    revision   INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT
)
"""

# Timestamps inside trigger bodies cannot use bound parameters.
_BUMP_REVISION_SQL = (
    "UPDATE db_revision SET revision = revision + 1,"
    " updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = 1"
)

# Wide-table revision triggers.  Installed at startup (not only when a
# migration starts) so reconciliation is available before migration too.
# They are dropped automatically with the table at cutover; the compatibility
# view triggers below take over afterwards.
REVISION_TRIGGERS = (
    f"""
    CREATE TRIGGER IF NOT EXISTS _revision_samples_insert
    AFTER INSERT ON samples
    BEGIN
        {_BUMP_REVISION_SQL};
    END
    """,
    f"""
    CREATE TRIGGER IF NOT EXISTS _revision_samples_update
    AFTER UPDATE ON samples
    BEGIN
        {_BUMP_REVISION_SQL};
    END
    """,
    f"""
    CREATE TRIGGER IF NOT EXISTS _revision_samples_delete
    AFTER DELETE ON samples
    BEGIN
        {_BUMP_REVISION_SQL};
    END
    """,
)


# -------------------------------------------------------- reconciliation store
RECONCILIATION_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS reconciliation_runs (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uid            TEXT    NOT NULL UNIQUE,
        status             TEXT    NOT NULL
                           CHECK (status IN
                             ('pending', 'published', 'aborted', 'failed')),
        requested_at       TEXT    NOT NULL,
        started_at         TEXT,
        finished_at        TEXT,
        base_revision      INTEGER NOT NULL,
        final_revision     INTEGER,
        published_revision INTEGER,
        manifest_count     INTEGER NOT NULL,
        source             TEXT,
        stale              INTEGER NOT NULL DEFAULT 0,
        failure_reason     TEXT,
        diff_manifest_only INTEGER NOT NULL DEFAULT 0,
        diff_database_only INTEGER NOT NULL DEFAULT 0,
        diff_value_mismatch INTEGER NOT NULL DEFAULT 0
    )
    """,
    # Staging area for the incoming manifest.  Rows are kept for published
    # reports (audit) and deleted for aborted / failed runs, so no half result
    # ever remains observable.
    """
    CREATE TABLE IF NOT EXISTS reconciliation_manifest (
        run_id       INTEGER NOT NULL,
        sample_id    INTEGER NOT NULL,
        temperature  REAL,
        ph           REAL,
        temp_present INTEGER NOT NULL,
        ph_present   INTEGER NOT NULL,
        PRIMARY KEY (run_id, sample_id)
    ) WITHOUT ROWID
    """,
    # Persisted diff rows, one per disagreeing sample id.  The UNIQUE
    # constraint also provides the (run_id, sample_id) index used by the
    # keyset-paginated diff reader.
    """
    CREATE TABLE IF NOT EXISTS reconciliation_diffs (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id                   INTEGER NOT NULL,
        sample_id                INTEGER NOT NULL,
        diff_type                TEXT    NOT NULL
                                 CHECK (diff_type IN
                                   ('manifest_only', 'database_only',
                                    'value_mismatch')),
        manifest_temperature     REAL,
        manifest_ph              REAL,
        manifest_temp_present    INTEGER NOT NULL,
        manifest_ph_present      INTEGER NOT NULL,
        database_temperature     REAL,
        database_ph              REAL,
        UNIQUE (run_id, sample_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_recon_diffs_run_id"
    " ON reconciliation_diffs(run_id, sample_id)",
)


def bump_revision(conn) -> None:
    """Advance the committed-write counter (caller holds a write tx)."""
    conn.execute(_BUMP_REVISION_SQL)


def ensure_revision_tracking(conn) -> None:
    """Create the revision counter and wide-table triggers if needed.

    After cutover ``samples`` is a view: only the counter row is (re)created
    here -- the INSTEAD OF triggers installed with the view carry the bumps.
    """
    conn.execute(DB_REVISION_TABLE_SQL)
    conn.execute(
        "INSERT OR IGNORE INTO db_revision (id, revision, updated_at)"
        " VALUES (1, 0, strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
    )
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = 'samples'"
    ).fetchone()
    if row is not None and row["type"] == "table":
        for sql in REVISION_TRIGGERS:
            conn.execute(sql)


def create_reconciliation_schema(conn) -> None:
    """Create reconciliation bookkeeping tables (idempotent)."""
    for sql in RECONCILIATION_SCHEMA_SQL:
        conn.execute(sql)


def create_base_schema(conn) -> None:
    """Create long storage and migration bookkeeping (idempotent)."""
    conn.execute(LONG_TABLE_SQL)
    conn.execute(MIGRATION_TABLE_SQL)
    conn.execute(MIGRATION_EVENT_SQL)
    conn.execute(INSERT_CONTEXT_SQL)


def ensure_wide_table(conn) -> None:
    """Create the legacy wide table only when ``samples`` does not exist.

    After cutover ``samples`` is a view; ``CREATE TABLE IF NOT EXISTS`` is a
    no-op for views on SQLite, but checking sqlite_master keeps the intent
    explicit and works on older versions too.
    """
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = 'samples'"
    ).fetchone()
    if row is None:
        conn.execute(WIDE_TABLE_SQL)


def install_migration_triggers(conn) -> None:
    """Install mirroring triggers on the wide table (inside a transaction)."""
    drop_migration_triggers(conn)
    for sql in MIGRATION_TRIGGERS:
        conn.execute(sql)


def drop_migration_triggers(conn) -> None:
    for statement in _MIGRATION_DROP_SQL.split(";"):
        if statement.strip():
            conn.execute(statement)


def replace_wide_table_with_view(conn) -> None:
    """Drop the wide table and install the compatibility view.

    Caller must be inside a write transaction.  SQLite DDL is transactional,
    so a failure at any point rolls the whole switch back.
    """
    conn.execute("DROP TABLE IF EXISTS samples")
    for statement in _COMPAT_DROP_SQL.split(";"):
        if statement.strip():
            conn.execute(statement)
    conn.execute(COMPAT_VIEW_SQL)
    conn.execute(COMPAT_INSERT_TRIGGER_SQL)
    conn.execute(COMPAT_UPDATE_TRIGGER_SQL)
    conn.execute(COMPAT_DELETE_TRIGGER_SQL)
