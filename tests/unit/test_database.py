import sqlite3

import pytest

from pipeline.database import (
    database_connection,
    initialize_database,
    transaction,
)


EXPECTED_TABLES = {
    "customers",
    "orders",
    "pipeline_runs",
    "run_dataset_metrics",
    "accepted_records",
    "rejected_records",
    "batch_errors",
}


def _insert_running_run(connection: sqlite3.Connection) -> str:
    """Insert a valid run row used by tests that need database references."""
    run_id = "12345678-1234-1234-1234-123456789abc"
    connection.execute(
        """
        INSERT INTO pipeline_runs (run_id, started_at, status)
        VALUES (?, ?, ?)
        """,
        (run_id, "2026-11-03T00:00:00Z", "running"),
    )
    return run_id


def test_initialization_creates_all_seven_tables(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"

    initialize_database(database_path)

    with database_connection(database_path) as connection:
        actual_tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert EXPECTED_TABLES <= actual_tables


def test_initialization_can_be_called_twice(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"

    initialize_database(database_path)
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        actual_tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert EXPECTED_TABLES <= actual_tables


def test_foreign_keys_are_enabled_on_each_connection(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_transaction_commits_when_block_succeeds(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        run_id = "12345678-1234-1234-1234-123456789abc"
        with transaction(connection):
            connection.execute(
                """
                INSERT INTO pipeline_runs (run_id, started_at, status)
                VALUES (?, ?, ?)
                """,
                (run_id, "2026-11-03T00:00:00Z", "running"),
            )

    with database_connection(database_path) as connection:
        result = connection.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

    assert result["status"] == "running"


def test_transaction_rolls_back_when_block_raises(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        with pytest.raises(RuntimeError, match="simulated failure"):
            with transaction(connection):
                _insert_running_run(connection)
                raise RuntimeError("simulated failure")

    with database_connection(database_path) as connection:
        run_count = connection.execute(
            "SELECT COUNT(*) FROM pipeline_runs"
        ).fetchone()[0]

    assert run_count == 0


def test_nested_transactions_are_rejected(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        with transaction(connection):
            with pytest.raises(sqlite3.ProgrammingError, match="existing transaction"):
                with transaction(connection):
                    pass


def test_rejection_total_can_include_loading_stage_rejections(tmp_path):
    database_path = tmp_path / "pipeline.sqlite"
    initialize_database(database_path)

    with database_connection(database_path) as connection:
        run_id = _insert_running_run(connection)
        connection.execute(
            """
            INSERT INTO run_dataset_metrics (
                run_id,
                dataset_name,
                source_file,
                rows_read,
                rows_parsed,
                malformed_rows,
                rows_valid_candidates,
                rows_rejected_parsed,
                rows_accepted,
                rows_rejected,
                rows_inserted,
                rows_unchanged,
                started_at,
                status,
                reconciliation_status
            )
            VALUES (?, 'orders', 'orders.csv', 10, 10, 0, 9, 1, 8, 2, 8, 0,
                    '2026-11-03T00:00:00Z', 'completed_with_rejects', 'complete')
            """,
            (run_id,),
        )

        rejected_count = connection.execute(
            """
            SELECT rows_rejected
            FROM run_dataset_metrics
            WHERE run_id = ? AND dataset_name = 'orders'
            """,
            (run_id,),
        ).fetchone()[0]

    assert rejected_count == 2
    assert rejected_count > 0 + 1
