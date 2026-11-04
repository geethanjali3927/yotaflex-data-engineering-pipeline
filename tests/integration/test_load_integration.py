import csv
import sqlite3

import pytest

import pipeline.load as load_module
from pipeline.database import database_connection, initialize_database
from pipeline.extract import extract_required_csv_files
from pipeline.load import (
    LoadBatchError,
    publish_batch,
    record_processing_results,
    start_run,
)
from pipeline.transform import (
    TransformationResult,
    transform_customers,
    transform_orders,
)
from pipeline.validate import (
    build_customer_reference_map,
    validate_customers,
    validate_orders,
)


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
def database_path(tmp_path):
    path = tmp_path / "pipeline.sqlite"
    initialize_database(path)
    return path


def _write_csv(path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
    return path


def _build_pipeline_results(
    tmp_path,
    customers,
    orders,
    *,
    malformed_customer_rows=(),
    customer_references=None,
):
    customers_path = tmp_path / "customers.csv"
    orders_path = tmp_path / "orders.csv"
    _write_csv(
        customers_path,
        CUSTOMER_HEADERS,
        [*customers, *malformed_customer_rows],
    )
    _write_csv(orders_path, ORDER_HEADERS, orders)

    extraction_batch = extract_required_csv_files(
        customers_path,
        orders_path,
    )
    customer_validation = validate_customers(extraction_batch.customers)
    references = (
        customer_references
        if customer_references is not None
        else build_customer_reference_map(customer_validation)
    )
    order_validation = validate_orders(extraction_batch.orders, references)
    return (
        extraction_batch,
        transform_customers(customer_validation),
        transform_orders(order_validation),
    )


def _start_run(database_path, tmp_path):
    return start_run(
        database_path,
        {
            "customers": tmp_path / "customers.csv",
            "orders": tmp_path / "orders.csv",
        },
    )


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


def test_publishes_customers_and_orders_and_reconciles_metrics(
    tmp_path,
    database_path,
):
    extraction, customer_result, order_result = _build_pipeline_results(
        tmp_path,
        [_customer()],
        [_order()],
    )
    run = _start_run(database_path, tmp_path)

    result = publish_batch(
        database_path,
        run,
        extraction,
        customer_result,
        order_result,
    )

    assert result.status == "succeeded"
    assert result.run_id == run.run_id
    assert result.customer_metrics.rows_read == 1
    assert result.customer_metrics.rows_parsed == 1
    assert result.customer_metrics.malformed_rows == 0
    assert result.customer_metrics.rows_valid_candidates == 1
    assert result.customer_metrics.rows_rejected_parsed == 0
    assert result.customer_metrics.rows_accepted == 1
    assert result.customer_metrics.rows_rejected == 0
    assert result.customer_metrics.rows_inserted == 1
    assert result.customer_metrics.rows_unchanged == 0
    assert result.order_metrics.rows_accepted == 1
    assert result.order_metrics.rows_inserted == 1
    assert result.order_metrics.rows_unchanged == 0
    assert result.customer_metrics.reconciliation_status == "complete"
    assert result.order_metrics.reconciliation_status == "complete"

    with database_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 1
        accepted = connection.execute(
            """
            SELECT dataset_name, outcome
            FROM accepted_records
            WHERE run_id = ?
            ORDER BY dataset_name
            """,
            (run.run_id,),
        ).fetchall()
    assert [(row["dataset_name"], row["outcome"]) for row in accepted] == [
        ("customers", "inserted"),
        ("orders", "inserted"),
    ]


def test_identical_rerun_is_unchanged_and_repeated_runs_are_independent(
    tmp_path,
    database_path,
):
    initialize_database(database_path)
    batch_one, customers_one, orders_one = _build_pipeline_results(
        tmp_path,
        [_customer()],
        [_order()],
    )
    first_run = _start_run(database_path, tmp_path)
    first = publish_batch(
        database_path,
        first_run,
        batch_one,
        customers_one,
        orders_one,
    )

    batch_two, customers_two, orders_two = _build_pipeline_results(
        tmp_path,
        [_customer()],
        [_order()],
    )
    second_run = _start_run(database_path, tmp_path)
    second = publish_batch(
        database_path,
        second_run,
        batch_two,
        customers_two,
        orders_two,
    )

    assert first_run.run_id != second_run.run_id
    assert first.customer_metrics.rows_inserted == 1
    assert first.order_metrics.rows_inserted == 1
    assert second.customer_metrics.rows_inserted == 0
    assert second.customer_metrics.rows_unchanged == 1
    assert second.order_metrics.rows_inserted == 0
    assert second.order_metrics.rows_unchanged == 1
    with database_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM pipeline_runs"
        ).fetchone()[0] == 2


def test_changed_business_data_is_rejected_without_overwriting_existing_row(
    tmp_path,
    database_path,
):
    extraction, customers, orders = _build_pipeline_results(
        tmp_path,
        [_customer()],
        [_order()],
    )
    publish_batch(
        database_path,
        _start_run(database_path, tmp_path),
        extraction,
        customers,
        orders,
    )

    extraction, changed_customers, orders = _build_pipeline_results(
        tmp_path,
        [_customer(last_name="Byron")],
        [_order()],
    )
    run = _start_run(database_path, tmp_path)
    result = publish_batch(
        database_path,
        run,
        extraction,
        changed_customers,
        orders,
    )

    assert result.status == "completed_with_rejects"
    assert result.customer_metrics.rows_rejected == 1
    assert result.customer_metrics.rows_inserted == 0
    assert result.customer_metrics.rows_accepted == 0
    with database_connection(database_path) as connection:
        stored = connection.execute(
            "SELECT last_name FROM customers WHERE customer_id = 'C-1'"
        ).fetchone()
        rejection = connection.execute(
            """
            SELECT source_file, source_row_number, record_key, reason_code,
                   raw_record
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = 'customers'
            """,
            (run.run_id,),
        ).fetchone()
    assert stored["last_name"] == "Lovelace"
    assert rejection["source_file"] == "customers.csv"
    assert rejection["source_row_number"] == 1
    assert rejection["record_key"] == "C-1"
    assert rejection["reason_code"] == "existing_id_conflict"
    assert "Byron" in rejection["raw_record"]


def test_unknown_order_customer_is_rejected_with_source_traceability(
    tmp_path,
    database_path,
):
    extraction, customers, orders = _build_pipeline_results(
        tmp_path,
        [],
        [_order(order_id="O-UNKNOWN", customer_id="C-UNKNOWN")],
        customer_references={"C-UNKNOWN": "2026-01-01"},
    )
    assert len(orders.transformed_records) == 1
    run = _start_run(database_path, tmp_path)

    result = publish_batch(
        database_path,
        run,
        extraction,
        customers,
        orders,
    )

    assert result.order_metrics.rows_accepted == 0
    assert result.order_metrics.rows_rejected == 1
    assert result.order_metrics.rows_inserted == 0
    with database_connection(database_path) as connection:
        rejection = connection.execute(
            """
            SELECT source_file, source_row_number, physical_line_start,
                   physical_line_end, record_key, reason_code, raw_record
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = 'orders'
            """,
            (run.run_id,),
        ).fetchone()
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 0
    assert rejection["source_file"] == "orders.csv"
    assert rejection["source_row_number"] == 1
    assert rejection["physical_line_start"] == 2
    assert rejection["physical_line_end"] == 2
    assert rejection["record_key"] == "O-UNKNOWN"
    assert rejection["reason_code"] == "invalid_customer_reference"
    assert "O-UNKNOWN" in rejection["raw_record"]


def test_order_insert_failure_rolls_back_customers_and_orders(
    tmp_path,
    database_path,
):
    extraction, customers, orders = _build_pipeline_results(
        tmp_path,
        [_customer(customer_id="C-ROLLBACK")],
        [_order(order_id="O-ROLLBACK", customer_id="C-ROLLBACK")],
    )
    run = _start_run(database_path, tmp_path)
    with database_connection(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_order_insert
            BEFORE INSERT ON orders
            BEGIN
                SELECT RAISE(ABORT, 'simulated order insert failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="simulated order insert failure"):
        publish_batch(
            database_path,
            run,
            extraction,
            customers,
            orders,
        )

    with database_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM customers WHERE customer_id = 'C-ROLLBACK'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders WHERE order_id = 'O-ROLLBACK'"
        ).fetchone()[0] == 0
        run_status = connection.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()["status"]
        batch_errors = connection.execute(
            """
            SELECT stage, error_code, error_message
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run.run_id,),
        ).fetchall()
    assert run_status == "failed"
    assert len(batch_errors) == 1
    assert batch_errors[0]["stage"] == "load"
    assert batch_errors[0]["error_code"] == "load_failure"
    assert "simulated order insert failure" in batch_errors[0]["error_message"]


def test_incomplete_batch_is_failed_and_batch_error_is_audited(
    tmp_path,
    database_path,
):
    customers_path = _write_csv(
        tmp_path / "customers.csv",
        CUSTOMER_HEADERS,
        [_customer()],
    )
    orders_path = _write_csv(
        tmp_path / "orders.csv",
        ORDER_HEADERS,
        [_order()],
    )
    missing_customers = tmp_path / "missing_customers.csv"
    extraction = extract_required_csv_files(missing_customers, orders_path)
    run = start_run(
        database_path,
        {
            "customers": missing_customers,
            "orders": orders_path,
        },
    )

    with pytest.raises(LoadBatchError, match="not publishable"):
        publish_batch(
            database_path,
            run,
            extraction,
            TransformationResult("customers", (), (), ()),
            TransformationResult("orders", (), (), ()),
        )

    with database_connection(database_path) as connection:
        run_row = connection.execute(
            "SELECT status, ended_at FROM pipeline_runs WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()
        metrics = connection.execute(
            """
            SELECT dataset_name, status, reconciliation_status,
                   rows_accepted, rows_inserted, rows_unchanged
            FROM run_dataset_metrics
            WHERE run_id = ?
            ORDER BY dataset_name
            """,
            (run.run_id,),
        ).fetchall()
        errors = connection.execute(
            """
            SELECT dataset_name, source_file, stage, error_code
            FROM batch_errors
            WHERE run_id = ?
            """,
            (run.run_id,),
        ).fetchall()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0] == 0

    assert run_row["status"] == "failed"
    assert run_row["ended_at"] is not None
    assert len(metrics) == 2
    assert all(row["status"] == "failed" for row in metrics)
    assert all(
        row["reconciliation_status"] == "incomplete_batch_failure"
        for row in metrics
    )
    assert all(row["rows_accepted"] == 0 for row in metrics)
    assert all(row["rows_inserted"] == 0 for row in metrics)
    assert all(row["rows_unchanged"] == 0 for row in metrics)
    assert len(errors) == 2
    error_details = {
        (
            row["dataset_name"],
            row["source_file"],
            row["stage"],
            row["error_code"],
        )
        for row in errors
    }
    assert (
        "customers",
        "missing_customers.csv",
        "input_check",
        "missing_file",
    ) in error_details
    assert (None, None, "load", "load_failure") in error_details


def test_row_rejections_are_audited_once_and_metrics_reconcile(
    tmp_path,
    database_path,
):
    invalid_customer = _customer(
        customer_id="C-BAD",
        email="not-an-email",
    )
    extraction, customers, orders = _build_pipeline_results(
        tmp_path,
        [_customer(), invalid_customer],
        [_order()],
        malformed_customer_rows=[("MALFORMED",)],
    )
    run = _start_run(database_path, tmp_path)
    record_processing_results(
        database_path,
        run,
        extraction,
        customers,
        orders,
    )

    result = publish_batch(
        database_path,
        run,
        extraction,
        customers,
        orders,
    )

    assert result.status == "completed_with_rejects"
    assert result.customer_metrics.rows_read == 3
    assert result.customer_metrics.rows_parsed == 2
    assert result.customer_metrics.malformed_rows == 1
    assert result.customer_metrics.rows_valid_candidates == 1
    assert result.customer_metrics.rows_rejected_parsed == 1
    assert result.customer_metrics.rows_accepted == (
        result.customer_metrics.rows_inserted
        + result.customer_metrics.rows_unchanged
    )
    assert result.customer_metrics.rows_rejected == 2
    assert result.order_metrics.rows_read == (
        result.order_metrics.rows_parsed
        + result.order_metrics.malformed_rows
    )
    assert result.order_metrics.rows_parsed == (
        result.order_metrics.rows_valid_candidates
        + result.order_metrics.rows_rejected_parsed
    )

    with database_connection(database_path) as connection:
        rejects = connection.execute(
            """
            SELECT source_file, source_row_number, record_key, reason_code,
                   raw_record
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = 'customers'
            ORDER BY source_row_number
            """,
            (run.run_id,),
        ).fetchall()
        errors = connection.execute(
            "SELECT COUNT(*) FROM batch_errors WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()[0]
    assert len(rejects) == 2
    assert errors == 0
    assert rejects[0]["source_file"] == "customers.csv"
    assert rejects[0]["source_row_number"] == 2
    assert rejects[0]["record_key"] == "C-BAD"
    assert rejects[0]["reason_code"] == "invalid_email_format"
    assert "not-an-email" in rejects[0]["raw_record"]
    assert rejects[1]["source_row_number"] == 3
    assert rejects[1]["reason_code"] == "field_count_mismatch"
    assert rejects[1]["raw_record"] == "MALFORMED\n"


def test_post_commit_result_read_failure_does_not_fail_committed_run(
    tmp_path,
    database_path,
    monkeypatch,
):
    extraction, customers, orders = _build_pipeline_results(
        tmp_path,
        [_customer(customer_id="C-POST")],
        [_order(order_id="O-POST", customer_id="C-POST")],
    )
    run = _start_run(database_path, tmp_path)

    def fail_result_read(*args):
        raise RuntimeError("simulated result retrieval failure")

    monkeypatch.setattr(load_module, "_read_load_result", fail_result_read)
    with pytest.raises(RuntimeError, match="simulated result retrieval failure"):
        publish_batch(
            database_path,
            run,
            extraction,
            customers,
            orders,
        )

    with database_connection(database_path) as connection:
        run_row = connection.execute(
            "SELECT status, ended_at FROM pipeline_runs WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()
        assert connection.execute(
            "SELECT COUNT(*) FROM customers WHERE customer_id = 'C-POST'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM orders WHERE order_id = 'O-POST'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM batch_errors WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()[0] == 0
    assert run_row["status"] == "succeeded"
    assert run_row["ended_at"] is not None
