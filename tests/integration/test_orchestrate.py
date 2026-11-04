import csv
import sqlite3

import pytest

import pipeline.orchestrate as orchestrate_module
from pipeline.database import database_connection, initialize_database
from pipeline.load import LoadBatchError
from pipeline.orchestrate import main, run_pipeline


CUSTOMER_HEADERS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "signup_date",
    "country_code",
    "status",
    "phone",
)
ORDER_HEADERS = (
    "order_id",
    "customer_id",
    "order_ts",
    "total_amount_cents",
    "currency_code",
    "status",
)


@pytest.fixture
def paths(tmp_path):
    return {
        "database": tmp_path / "pipeline.sqlite",
        "customers": tmp_path / "customers.csv",
        "orders": tmp_path / "orders.csv",
    }


def _write_csv(path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
    return path


def _write_inputs(
    paths,
    customers,
    orders,
    *,
    customer_headers=CUSTOMER_HEADERS,
):
    _write_csv(paths["customers"], customer_headers, customers)
    _write_csv(paths["orders"], ORDER_HEADERS, orders)


def _customer(
    customer_id="C-1",
    first_name="Ada",
    last_name="Lovelace",
    email="ada@example.com",
    signup_date="2026-01-01",
    country_code="US",
    status="active",
    phone="000123",
):
    return (
        customer_id,
        first_name,
        last_name,
        email,
        signup_date,
        country_code,
        status,
        phone,
    )


def _order(
    order_id="O-1",
    customer_id="C-1",
    order_ts="2026-01-02T12:00:00Z",
    total_amount_cents="1250",
    currency_code="USD",
    status="placed",
):
    return (
        order_id,
        customer_id,
        order_ts,
        total_amount_cents,
        currency_code,
        status,
    )


def _run(paths):
    return run_pipeline(
        paths["database"],
        paths["customers"],
        paths["orders"],
    )


def test_successful_end_to_end_processing_and_metrics(paths):
    _write_inputs(paths, [_customer()], [_order()])

    result = _run(paths)

    assert result.status == "succeeded"
    assert result.run_id
    assert result.customer_metrics.rows_read == 1
    assert result.customer_metrics.rows_parsed == 1
    assert result.customer_metrics.rows_valid_candidates == 1
    assert result.customer_metrics.rows_accepted == 1
    assert result.customer_metrics.rows_inserted == 1
    assert result.customer_metrics.rows_unchanged == 0
    assert result.customer_metrics.rows_rejected == 0
    assert result.order_metrics.rows_read == 1
    assert result.order_metrics.rows_parsed == 1
    assert result.order_metrics.rows_valid_candidates == 1
    assert result.order_metrics.rows_accepted == 1
    assert result.order_metrics.rows_inserted == 1
    assert result.order_metrics.rows_rejected == 0
    assert result.customer_metrics.reconciliation_status == "complete"
    assert result.order_metrics.reconciliation_status == "complete"

    with database_connection(paths["database"]) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 1


def test_same_input_twice_produces_unchanged_records(paths):
    _write_inputs(paths, [_customer()], [_order()])

    first = _run(paths)
    second = _run(paths)

    assert first.run_id != second.run_id
    assert first.customer_metrics.rows_inserted == 1
    assert first.order_metrics.rows_inserted == 1
    assert second.status == "succeeded"
    assert second.customer_metrics.rows_inserted == 0
    assert second.customer_metrics.rows_unchanged == 1
    assert second.order_metrics.rows_inserted == 0
    assert second.order_metrics.rows_unchanged == 1
    with database_connection(paths["database"]) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 1


def test_invalid_customer_and_order_rows_keep_rejection_traceability(paths):
    _write_inputs(
        paths,
        [
            _customer(),
            _customer(
                customer_id="C-BAD",
                email="not-an-email",
            ),
        ],
        [
            _order(),
            _order(
                order_id="O-BAD",
                total_amount_cents="not-an-integer",
            ),
        ],
    )

    result = _run(paths)

    assert result.status == "completed_with_rejects"
    assert result.customer_metrics.rows_rejected == 1
    assert result.order_metrics.rows_rejected == 1
    with database_connection(paths["database"]) as connection:
        rejections = connection.execute(
            """
            SELECT dataset_name, source_file, source_row_number, record_key,
                   reason_code, raw_record
            FROM rejected_records
            WHERE run_id = ?
            ORDER BY dataset_name
            """,
            (result.run_id,),
        ).fetchall()
    assert len(rejections) == 2
    by_dataset = {row["dataset_name"]: row for row in rejections}
    assert by_dataset["customers"]["source_file"] == "customers.csv"
    assert by_dataset["customers"]["source_row_number"] == 2
    assert by_dataset["customers"]["record_key"] == "C-BAD"
    assert by_dataset["customers"]["reason_code"] == "invalid_email_format"
    assert "not-an-email" in by_dataset["customers"]["raw_record"]
    assert by_dataset["orders"]["source_file"] == "orders.csv"
    assert by_dataset["orders"]["source_row_number"] == 2
    assert by_dataset["orders"]["record_key"] == "O-BAD"
    assert by_dataset["orders"]["reason_code"] == "invalid_order_amount"
    assert "not-an-integer" in by_dataset["orders"]["raw_record"]


def test_missing_input_fails_without_publishing_business_records(paths):
    _write_csv(paths["orders"], ORDER_HEADERS, [_order()])
    missing_customers = paths["customers"]

    with pytest.raises(LoadBatchError, match="not publishable"):
        _run(paths)
    with database_connection(paths["database"]) as connection:
        run = connection.execute(
            "SELECT run_id, status FROM pipeline_runs"
        ).fetchone()
        metrics = connection.execute(
            """
            SELECT status, reconciliation_status
            FROM run_dataset_metrics
            WHERE run_id = ?
            """,
            (run["run_id"],),
        ).fetchall()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 0
        errors = connection.execute(
            """
            SELECT dataset_name, source_file, error_code
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run["run_id"],),
        ).fetchall()
    assert run["status"] == "failed"
    assert all(row["status"] == "failed" for row in metrics)
    assert all(
        row["reconciliation_status"] == "incomplete_batch_failure"
        for row in metrics
    )
    assert any(
        row["dataset_name"] == "customers"
        and row["source_file"] == missing_customers.name
        and row["error_code"] == "missing_file"
        for row in errors
    )


def test_customer_schema_failure_does_not_reject_orders_as_unknown(paths):
    wrong_customer_headers = (
        "wrong_customer_id",
        "first_name",
        "last_name",
        "email",
        "signup_date",
        "country_code",
        "status",
        "phone",
    )
    _write_inputs(
        paths,
        [_customer()],
        [_order(customer_id="C-1")],
        customer_headers=wrong_customer_headers,
    )

    with pytest.raises(LoadBatchError, match="not publishable"):
        _run(paths)
    with database_connection(paths["database"]) as connection:
        run = connection.execute(
            "SELECT run_id, status FROM pipeline_runs"
        ).fetchone()
        order_rejections = connection.execute(
            """
            SELECT reason_code
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = 'orders'
            """,
            (run["run_id"],),
        ).fetchall()
        errors = connection.execute(
            """
            SELECT dataset_name, error_code
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run["run_id"],),
        ).fetchall()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 0
    assert order_rejections == []
    assert run["status"] == "failed"
    assert any(
        row["dataset_name"] == "orders"
        and row["error_code"] == "customer_reference_unavailable"
        for row in errors
    )
    assert any(
        row["dataset_name"] == "customers"
        and row["error_code"] in {
            "missing_required_columns",
            "unexpected_columns",
        }
        for row in errors
    )


def test_unexpected_error_before_publishing_is_recorded_and_reraised(
    paths,
    monkeypatch,
):
    _write_inputs(paths, [_customer()], [_order()])

    def unexpected_failure(_validated_customers):
        raise RuntimeError("simulated reference-map failure")

    monkeypatch.setattr(
        orchestrate_module,
        "build_customer_reference_map",
        unexpected_failure,
    )
    with pytest.raises(RuntimeError, match="simulated reference-map failure"):
        _run(paths)

    with database_connection(paths["database"]) as connection:
        run = connection.execute(
            "SELECT run_id, status, error_message FROM pipeline_runs"
        ).fetchone()
        errors = connection.execute(
            """
            SELECT stage, error_code, error_message
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run["run_id"],),
        ).fetchall()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 0
    assert run["status"] == "failed"
    assert "simulated reference-map failure" in run["error_message"]
    assert len(errors) == 1
    assert errors[0]["stage"] == "load"
    assert errors[0]["error_code"] == "load_failure"
    assert "simulated reference-map failure" in errors[0]["error_message"]


def test_publishing_failure_preserves_atomic_rollback(paths):
    _write_inputs(
        paths,
        [_customer(customer_id="C-ROLLBACK")],
        [_order(order_id="O-ROLLBACK", customer_id="C-ROLLBACK")],
    )
    initialize_database(paths["database"])
    with database_connection(paths["database"]) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_order_insert
            BEFORE INSERT ON orders
            BEGIN
                SELECT RAISE(ABORT, 'simulated order publish failure');
            END
            """
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="simulated order publish failure",
    ):
        _run(paths)

    with database_connection(paths["database"]) as connection:
        run = connection.execute(
            "SELECT run_id, status FROM pipeline_runs"
        ).fetchone()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers WHERE customer_id = 'C-ROLLBACK'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders WHERE order_id = 'O-ROLLBACK'"
        ).fetchone()[0] == 0
        errors = connection.execute(
            """
            SELECT stage, error_code, error_message
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run["run_id"],),
        ).fetchall()
    assert run["status"] == "failed"
    assert len(errors) == 1
    assert errors[0]["stage"] == "load"
    assert "simulated order publish failure" in errors[0]["error_message"]


def test_cli_success_and_failure_exit_codes(paths, capsys):
    _write_inputs(paths, [_customer()], [_order()])

    success_code = main(
        [
            "--database",
            str(paths["database"]),
            "--customers",
            str(paths["customers"]),
            "--orders",
            str(paths["orders"]),
        ]
    )

    success_output = capsys.readouterr()
    assert success_code == 0
    assert "Run ID:" in success_output.out
    assert "Status: succeeded" in success_output.out
    assert "customers: status=succeeded" in success_output.out
    assert "orders: status=succeeded" in success_output.out

    missing_orders = paths["orders"].with_name("missing_orders.csv")
    failure_code = main(
        [
            "--database",
            str(paths["database"]),
            "--customers",
            str(paths["customers"]),
            "--orders",
            str(missing_orders),
        ]
    )

    failure_output = capsys.readouterr()
    assert failure_code == 1
    assert failure_output.out == ""
    assert "Pipeline failed: Batch is not publishable" in failure_output.err


def test_cli_unexpected_failure_returns_nonzero_and_prints_stderr(
    paths,
    capsys,
    monkeypatch,
):
    def fail_before_run(*_args, **_kwargs):
        raise RuntimeError("simulated CLI exception")

    monkeypatch.setattr(orchestrate_module, "run_pipeline", fail_before_run)

    exit_code = main(
        [
            "--database",
            str(paths["database"]),
            "--customers",
            str(paths["customers"]),
            "--orders",
            str(paths["orders"]),
        ]
    )

    output = capsys.readouterr()
    assert exit_code == 1
    assert output.out == ""
    assert "Pipeline failed: simulated CLI exception" in output.err
