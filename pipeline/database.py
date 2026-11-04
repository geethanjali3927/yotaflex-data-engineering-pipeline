"""SQLite connection and schema helpers for the data pipeline."""

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterator


_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY NOT NULL
            CHECK (
                length(run_id) = 36
                AND length(run_id) - length(replace(run_id, '-', '')) = 4
                AND substr(run_id, 9, 1) = '-'
                AND substr(run_id, 14, 1) = '-'
                AND substr(run_id, 19, 1) = '-'
                AND substr(run_id, 24, 1) = '-'
                AND lower(replace(run_id, '-', '')) NOT GLOB '*[^0-9a-f]*'
            ),
        started_at TEXT NOT NULL CHECK (length(trim(started_at)) > 0),
        ended_at TEXT,
        status TEXT NOT NULL
            CHECK (
                status IN (
                    'running',
                    'succeeded',
                    'completed_with_rejects',
                    'failed',
                    'abandoned'
                )
            ),
        config_json TEXT,
        error_message TEXT,
        CHECK (
            (status = 'running' AND ended_at IS NULL)
            OR (status <> 'running' AND ended_at IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_dataset_metrics (
        run_id TEXT NOT NULL,
        dataset_name TEXT NOT NULL
            CHECK (dataset_name IN ('customers', 'orders')),
        source_file TEXT NOT NULL CHECK (length(trim(source_file)) > 0),
        schema_version TEXT,
        rows_read INTEGER CHECK (rows_read IS NULL OR rows_read >= 0),
        rows_parsed INTEGER CHECK (rows_parsed IS NULL OR rows_parsed >= 0),
        malformed_rows INTEGER
            CHECK (malformed_rows IS NULL OR malformed_rows >= 0),
        rows_valid_candidates INTEGER
            CHECK (
                rows_valid_candidates IS NULL
                OR rows_valid_candidates >= 0
            ),
        rows_rejected_parsed INTEGER
            CHECK (
                rows_rejected_parsed IS NULL
                OR rows_rejected_parsed >= 0
            ),
        rows_accepted INTEGER
            CHECK (rows_accepted IS NULL OR rows_accepted >= 0),
        rows_rejected INTEGER
            CHECK (rows_rejected IS NULL OR rows_rejected >= 0),
        rows_inserted INTEGER
            CHECK (rows_inserted IS NULL OR rows_inserted >= 0),
        rows_unchanged INTEGER
            CHECK (rows_unchanged IS NULL OR rows_unchanged >= 0),
        started_at TEXT NOT NULL CHECK (length(trim(started_at)) > 0),
        ended_at TEXT,
        status TEXT NOT NULL
            CHECK (
                status IN (
                    'pending',
                    'running',
                    'succeeded',
                    'completed_with_rejects',
                    'failed',
                    'abandoned'
                )
            ),
        reconciliation_status TEXT NOT NULL
            CHECK (
                reconciliation_status IN (
                    'pending',
                    'complete',
                    'incomplete_batch_failure',
                    'incomplete_publish_failure'
                )
            ),
        error_message TEXT,
        PRIMARY KEY (run_id, dataset_name),
        FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id),
        CHECK (
            rows_read IS NULL
            OR rows_parsed IS NULL
            OR malformed_rows IS NULL
            OR rows_read = rows_parsed + malformed_rows
        ),
        CHECK (
            rows_parsed IS NULL
            OR rows_valid_candidates IS NULL
            OR rows_rejected_parsed IS NULL
            OR rows_parsed = rows_valid_candidates + rows_rejected_parsed
        ),
        CHECK (
            rows_accepted IS NULL
            OR rows_inserted IS NULL
            OR rows_unchanged IS NULL
            OR rows_accepted = rows_inserted + rows_unchanged
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY NOT NULL
            CHECK (length(trim(customer_id)) > 0),
        first_name TEXT NOT NULL
            CHECK (length(trim(first_name)) BETWEEN 1 AND 100),
        last_name TEXT NOT NULL
            CHECK (length(trim(last_name)) BETWEEN 1 AND 100),
        email TEXT NOT NULL CHECK (length(trim(email)) > 0),
        phone TEXT,
        signup_date TEXT NOT NULL CHECK (length(signup_date) = 10),
        country_code TEXT NOT NULL
            CHECK (country_code GLOB '[A-Z][A-Z]'),
        status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
        source_file TEXT NOT NULL CHECK (length(trim(source_file)) > 0),
        source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
        first_seen_run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL CHECK (length(trim(loaded_at)) > 0),
        FOREIGN KEY (first_seen_run_id) REFERENCES pipeline_runs (run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY NOT NULL
            CHECK (length(trim(order_id)) > 0),
        customer_id TEXT NOT NULL,
        order_ts TEXT NOT NULL CHECK (length(trim(order_ts)) > 0),
        total_amount_cents INTEGER NOT NULL CHECK (total_amount_cents > 0),
        currency_code TEXT NOT NULL CHECK (currency_code = 'USD'),
        status TEXT NOT NULL
            CHECK (
                status IN (
                    'placed',
                    'paid',
                    'shipped',
                    'delivered',
                    'cancelled',
                    'returned'
                )
            ),
        source_file TEXT NOT NULL CHECK (length(trim(source_file)) > 0),
        source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
        first_seen_run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL CHECK (length(trim(loaded_at)) > 0),
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
        FOREIGN KEY (first_seen_run_id) REFERENCES pipeline_runs (run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS accepted_records (
        accepted_record_id INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL,
        dataset_name TEXT NOT NULL
            CHECK (dataset_name IN ('customers', 'orders')),
        source_file TEXT NOT NULL CHECK (length(trim(source_file)) > 0),
        source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
        record_key TEXT NOT NULL CHECK (length(trim(record_key)) > 0),
        outcome TEXT NOT NULL CHECK (outcome IN ('inserted', 'unchanged')),
        accepted_at TEXT NOT NULL CHECK (length(trim(accepted_at)) > 0),
        UNIQUE (run_id, dataset_name, source_file, source_row_number),
        FOREIGN KEY (run_id, dataset_name)
            REFERENCES run_dataset_metrics (run_id, dataset_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_records (
        reject_id INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL,
        dataset_name TEXT NOT NULL
            CHECK (dataset_name IN ('customers', 'orders')),
        source_file TEXT NOT NULL CHECK (length(trim(source_file)) > 0),
        source_row_number INTEGER CHECK (
            source_row_number IS NULL OR source_row_number > 0
        ),
        physical_line_start INTEGER CHECK (
            physical_line_start IS NULL OR physical_line_start > 0
        ),
        physical_line_end INTEGER CHECK (
            physical_line_end IS NULL OR physical_line_end > 0
        ),
        record_key TEXT,
        reason_code TEXT NOT NULL CHECK (length(trim(reason_code)) > 0),
        reason_detail TEXT NOT NULL CHECK (length(trim(reason_detail)) > 0),
        raw_record TEXT NOT NULL,
        rejected_at TEXT NOT NULL CHECK (length(trim(rejected_at)) > 0),
        UNIQUE (run_id, dataset_name, source_file, source_row_number),
        FOREIGN KEY (run_id, dataset_name)
            REFERENCES run_dataset_metrics (run_id, dataset_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_errors (
        batch_error_id INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL,
        dataset_name TEXT
            CHECK (dataset_name IS NULL OR dataset_name IN ('customers', 'orders')),
        source_file TEXT,
        stage TEXT NOT NULL
            CHECK (
                stage IN (
                    'input_check',
                    'schema_check',
                    'csv_parse',
                    'validation',
                    'transform',
                    'load',
                    'finalize'
                )
            ),
        error_code TEXT NOT NULL CHECK (length(trim(error_code)) > 0),
        error_message TEXT NOT NULL CHECK (length(trim(error_message)) > 0),
        physical_line INTEGER CHECK (
            physical_line IS NULL OR physical_line > 0
        ),
        created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
        FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id),
        FOREIGN KEY (run_id, dataset_name)
            REFERENCES run_dataset_metrics (run_id, dataset_name)
    )
    """,
)

_INDEX_STATEMENTS = (
    """
    CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders (customer_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_customers_first_seen_run_id
    ON customers (first_seen_run_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_orders_first_seen_run_id
    ON orders (first_seen_run_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_run_dataset_metrics_status_started_at
    ON run_dataset_metrics (status, started_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_accepted_records_run_dataset
    ON accepted_records (run_id, dataset_name)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rejected_records_run_dataset
    ON rejected_records (run_id, dataset_name)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rejected_records_record_key
    ON rejected_records (record_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_batch_errors_run_dataset
    ON batch_errors (run_id, dataset_name)
    """,
)


def connect_database(
    database_path: str | Path,
    *,
    timeout: float = 5.0,
) -> sqlite3.Connection:
    """Open a SQLite connection with row access by column name and FKs enabled."""
    connection = sqlite3.connect(database_path, timeout=timeout)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")

        # Fail immediately if the SQLite build does not support FK enforcement.
        foreign_keys_enabled = connection.execute(
            "PRAGMA foreign_keys"
        ).fetchone()[0]
        if foreign_keys_enabled != 1:
            raise sqlite3.DatabaseError(
                "SQLite foreign-key enforcement is unavailable"
            )
    except BaseException:
        connection.close()
        raise

    return connection


@contextmanager
def database_connection(
    database_path: str | Path,
    *,
    timeout: float = 5.0,
) -> Iterator[sqlite3.Connection]:
    """Open a connection for a with-block and always close it afterward."""
    connection = connect_database(database_path, timeout=timeout)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a group of database statements atomically, rolling back on errors."""
    if connection.in_transaction:
        raise sqlite3.ProgrammingError(
            "transaction() cannot start inside an existing transaction"
        )

    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
        connection.commit()
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise


def initialize_database(database_path: str | Path) -> None:
    """Create the pipeline tables and indexes if they do not already exist."""
    with database_connection(database_path) as connection:
        with transaction(connection):
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)

            for statement in _INDEX_STATEMENTS:
                connection.execute(statement)
