"""DuckDB raw-layer warehouse: run bookkeeping, audit log, and landing tables.

Design notes
------------
The raw layer stays *append-only*. Every landed row carries `season`,
`ingested_at` and `ingest_run_id`, and staging models pick the view they need
with a window function. That is deliberate: the append history is what makes a
point-in-time (snapshot) view of player price, availability and set-piece order
possible at all.

Idempotency is by payload hash rather than by delete-and-reload, so re-running
an ingest the same day is a no-op while a genuine change still lands a new
version.
"""

import datetime
import hashlib
import json

import duckdb
import pandas as pd

META_COLUMNS = ("season", "ingested_at", "ingest_run_id")


def _hash_payload(payload):
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()


NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "DECIMAL",
}


def _jsonify_nested(df):
    """Make an incoming frame safe for DuckDB type inference.

    Two problems are handled here:

    1. Object columns holding lists/dicts have no inferable type, so they are
       serialised to JSON text and can be unnested downstream.
    2. All-null columns get inferred as INTEGER, which then rejects the first
       real value that turns up. FPL is full of fields that are null early in
       the season and populated later (`club_badge_src`, `last_deadline_value`,
       chip fields), so those are pinned to VARCHAR instead.
    """
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            sample = out[col].dropna()
            if len(sample) and isinstance(sample.iloc[0], (list, dict)):
                out[col] = out[col].map(
                    lambda v: None if v is None else json.dumps(v, default=str)
                )
        if out[col].isna().all():
            out[col] = out[col].astype("string")
    return out


def _reconcile_type(existing, incoming):
    """Pick a column type that can hold both the stored and incoming values.

    Returns None when the stored type is already fine.
    """
    if existing == incoming:
        return None
    base = lambda t: t.split("(")[0].upper()  # noqa: E731 - DECIMAL(5,2) -> DECIMAL
    a, b = base(existing), base(incoming)
    if a in NUMERIC_TYPES and b in NUMERIC_TYPES:
        return None if a == "DOUBLE" else "DOUBLE"
    return None if a == "VARCHAR" else "VARCHAR"


class Warehouse:
    def __init__(self, path):
        self.path = str(path)
        self.con = duckdb.connect(self.path)
        self.run_id = None
        self._ensure_meta()

    # -- setup ------------------------------------------------------------

    def _ensure_meta(self):
        self.con.execute("CREATE SEQUENCE IF NOT EXISTS raw_log_id_seq START 1")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_log(
                id INTEGER PRIMARY KEY DEFAULT nextval('raw_log_id_seq'),
                payload JSON,
                response_code VARCHAR(10),
                table_name VARCHAR(50),
                timestamp TIMESTAMP,
                status VARCHAR(30)
            )
            """
        )
        # Extend the original audit log in place rather than replacing it - it
        # holds the only surviving copy of the 2025-26 response bodies.
        for col, decl in [
            ("ingest_run_id", "VARCHAR"),
            ("season", "VARCHAR"),
            ("endpoint", "VARCHAR"),
            ("payload_hash", "VARCHAR"),
            ("row_count", "INTEGER"),
        ]:
            try:
                self.con.execute(f"ALTER TABLE raw_log ADD COLUMN {col} {decl}")
            except duckdb.CatalogException:
                pass  # column already present

        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_run(
                run_id VARCHAR PRIMARY KEY,
                season VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR,
                notes VARCHAR
            )
            """
        )

    # -- run bookkeeping --------------------------------------------------

    def start_run(self, season, notes=None):
        now = datetime.datetime.now()
        self.run_id = f"{season}_{now:%Y%m%dT%H%M%S}"
        self.con.execute(
            "INSERT INTO ingest_run(run_id, season, started_at, status, notes) VALUES (?,?,?,?,?)",
            [self.run_id, season, now, "running", notes],
        )
        return self.run_id

    def finish_run(self, status="success"):
        self.con.execute(
            "UPDATE ingest_run SET finished_at = ?, status = ? WHERE run_id = ?",
            [datetime.datetime.now(), status, self.run_id],
        )

    # -- audit log --------------------------------------------------------

    def log(self, table_name, endpoint, status, season, response_code=None,
            payload=None, row_count=None, payload_hash=None):
        self.con.execute(
            """
            INSERT INTO raw_log(payload, response_code, table_name, timestamp, status,
                                ingest_run_id, season, endpoint, payload_hash, row_count)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                json.dumps(payload, default=str) if payload is not None else None,
                str(response_code) if response_code is not None else None,
                table_name,
                datetime.datetime.now(),
                status,
                self.run_id,
                season,
                endpoint,
                payload_hash,
                row_count,
            ],
        )

    def last_hash(self, table_name, season):
        row = self.con.execute(
            """
            SELECT payload_hash FROM raw_log
            WHERE table_name = ? AND season = ? AND status = 'success'
              AND payload_hash IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
            """,
            [table_name, season],
        ).fetchone()
        return row[0] if row else None

    # -- landing ----------------------------------------------------------

    def land(self, df, table, season, keep_payload=None):
        """Append a dataframe to a raw table, evolving the schema if needed.

        Returns the number of rows written (0 when skipped as unchanged).
        """
        if df is None or len(df) == 0:
            return 0

        df = _jsonify_nested(pd.DataFrame(df))
        df["season"] = season
        df["ingested_at"] = datetime.datetime.now()
        df["ingest_run_id"] = self.run_id

        self.con.register("_landing_df", df)
        try:
            self.con.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" AS SELECT * FROM _landing_df WHERE 1=0'
            )
            self._evolve_schema(table)

            cols = ", ".join(f'"{c}"' for c in df.columns)
            self.con.execute(
                f'INSERT INTO "{table}" ({cols}) SELECT {cols} FROM _landing_df'
            )
        finally:
            self.con.unregister("_landing_df")

        return len(df)

    def _evolve_schema(self, table):
        """Reconcile `table` with the shape of `_landing_df`.

        Two kinds of drift show up in practice:

        * New columns - FPL adds fields mid-season (defensive_contribution
          appeared during 2025-26). The table is widened and older rows keep
          NULLs.
        * Changed types - a field that was null on first sight was inferred too
          narrowly, and a later payload carries a real value. The stored column
          is widened to something that holds both.

        Without this a blind `INSERT ... SELECT *` fails mid-season, which is
        exactly when a rerun is least convenient.
        """
        existing = {r[0]: r[1] for r in self.con.execute(f'DESCRIBE "{table}"').fetchall()}
        incoming = self.con.execute("DESCRIBE SELECT * FROM _landing_df").fetchall()

        for name, dtype, *_ in incoming:
            if name not in existing:
                self.con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {dtype}')
                continue
            widened = _reconcile_type(existing[name], dtype)
            if widened:
                self.con.execute(
                    f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE {widened}'
                )

    # -- convenience ------------------------------------------------------

    def land_if_changed(self, df, table, season, payload, endpoint, keep_payload=True):
        """Land a payload unless an identical one was already landed.

        Keeps the audit log honest either way: a skip is recorded as 'unchanged'
        so the run history still shows the endpoint was checked.
        """
        digest = _hash_payload(payload)
        if self.last_hash(table, season) == digest:
            self.log(table, endpoint, "unchanged", season, 200,
                     payload=None, row_count=0, payload_hash=digest)
            return 0

        n = self.land(df, table, season)
        self.log(table, endpoint, "success", season, 200,
                 payload=payload if keep_payload else None,
                 row_count=n, payload_hash=digest)
        return n

    def close(self):
        self.con.close()
