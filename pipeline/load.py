"""Atomic SQLite publishing and run/audit tracking for pipeline results."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Mapping

from pipeline.database import database_connection, transaction
from pipeline.extract import (
    BatchError,
    DatasetName,
    ExtractionBatchResult,
    ExtractionResult,
    ExtractedRecord,
    MalformedCsvRecord,
)
from pipeline.transform import (
    TransformedCustomer,
    TransformedOrder,
    TransformationResult,
)
from pipeline.validate import RowRejection, ValidationIssue


_DATASET_NAMES: tuple[DatasetName, ...] = ("customers", "orders")
_RECONCILIATION_STATUSES = frozenset(
    {
        "incomplete_batch_failure",
        "incomplete_publish_failure",
    }
)
_ALLOWED_ERROR_STAGES = frozenset(
    {
        "input_check",
        "schema_check",
        "csv_parse",
        "validation",
        "transform",
        "load",
        "finalize",
    }
)


@dataclass(frozen=True, slots=True)
class RunContext:
    """Identity and start time for one pipeline invocation."""

    run_id: str
    started_at: str


@dataclass(frozen=True, slots=True)
class DatasetLoadMetrics:
    """Persisted metrics for one dataset in a pipeline run."""

    dataset_name: DatasetName
    rows_read: int | None
    rows_parsed: int | None
    malformed_rows: int | None
    rows_valid_candidates: int | None
    rows_rejected_parsed: int | None
    rows_accepted: int | None
    rows_rejected: int | None
    rows_inserted: int | None
    rows_unchanged: int | None
    status: str
    reconciliation_status: str


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Final committed load status and customer/order metrics."""

    run_id: str
    status: str
    customer_metrics: DatasetLoadMetrics
    order_metrics: DatasetLoadMetrics


class LoadBatchError(RuntimeError):
    """A complete extraction/transform batch could not be published."""


def start_run(
    database_path: str | Path,
    source_files: Mapping[str, str | Path],
    config: Mapping[str, object] | None = None,
) -> RunContext:
    """Create a run and both dataset metric rows in one committed transaction."""
    missing_datasets = set(_DATASET_NAMES) - set(source_files)
    if missing_datasets:
        raise ValueError(
            "source_files must include customers and orders; missing: "
            + ", ".join(sorted(missing_datasets))
        )

    run_context = RunContext(
        run_id=str(uuid.uuid4()),
        started_at=_utc_now(),
    )
    serialized_config = (
        json.dumps(config, sort_keys=True, separators=(",", ":"))
        if config is not None
        else None
    )

    with database_connection(database_path) as connection:
        with transaction(connection):
            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, started_at, status, config_json
                )
                VALUES (?, ?, 'running', ?)
                """,
                (
                    run_context.run_id,
                    run_context.started_at,
                    serialized_config,
                ),
            )
            for dataset_name in _DATASET_NAMES:
                connection.execute(
                    """
                    INSERT INTO run_dataset_metrics (
                        run_id,
                        dataset_name,
                        source_file,
                        started_at,
                        status,
                        reconciliation_status
                    )
                    VALUES (?, ?, ?, ?, 'running', 'pending')
                    """,
                    (
                        run_context.run_id,
                        dataset_name,
                        _safe_filename(source_files[dataset_name]),
                        run_context.started_at,
                    ),
                )
    return run_context


def record_processing_results(
    database_path: str | Path,
    run_context: RunContext,
    extraction_batch: ExtractionBatchResult,
    customer_result: TransformationResult,
    order_result: TransformationResult,
) -> None:
    """Persist known row rejects, batch errors, and pre-publish metric counts."""
    extraction_by_dataset = _extractions_by_dataset(extraction_batch)
    transformed_by_dataset = _transformations_by_dataset(
        customer_result,
        order_result,
    )
    batch_errors = _collect_batch_errors(
        extraction_batch,
        customer_result,
        order_result,
    )
    rejections = _collect_rejections(
        extraction_by_dataset,
        transformed_by_dataset,
    )

    with database_connection(database_path) as connection:
        with transaction(connection):
            _require_running_run(connection, run_context.run_id)
            for rejection in rejections:
                _insert_rejection_once(connection, run_context.run_id, rejection)
            for error in batch_errors:
                _insert_batch_error_once(connection, run_context.run_id, error)

            for dataset_name in _DATASET_NAMES:
                extraction = extraction_by_dataset[dataset_name]
                transformation = transformed_by_dataset[dataset_name]
                _update_pre_publish_metrics(
                    connection,
                    run_context.run_id,
                    dataset_name,
                    extraction,
                    transformation,
                    rejections,
                    has_batch_errors=any(
                        error.dataset_name in (None, dataset_name)
                        for error in batch_errors
                    )
                    or extraction is None
                    or not extraction.complete
                    or bool(transformation.batch_errors),
                )


def publish_batch(
    database_path: str | Path,
    run_context: RunContext,
    extraction_batch: ExtractionBatchResult,
    customer_result: TransformationResult,
    order_result: TransformationResult,
) -> LoadResult:
    """Publish both datasets atomically, recording failure after any rollback."""
    load_rejections: list[RowRejection] = []
    try:
        # This operation is idempotent, so callers may also record results earlier.
        record_processing_results(
            database_path,
            run_context,
            extraction_batch,
            customer_result,
            order_result,
        )

        preflight_error = _preflight_error(
            extraction_batch,
            customer_result,
            order_result,
        )
        if preflight_error is not None:
            raise preflight_error

        with database_connection(database_path) as connection:
            with transaction(connection):
                _require_running_run(connection, run_context.run_id)
                inserted_by_dataset: dict[DatasetName, int] = {
                    "customers": 0,
                    "orders": 0,
                }
                unchanged_by_dataset: dict[DatasetName, int] = {
                    "customers": 0,
                    "orders": 0,
                }
                for customer in customer_result.transformed_records:
                    if not isinstance(customer, TransformedCustomer):
                        raise TypeError(
                            "customer result contains a non-customer record"
                        )
                    outcome, rejection = _publish_customer(
                        connection,
                        run_context,
                        customer,
                    )
                    if rejection is not None:
                        load_rejections.append(rejection)
                        _insert_rejection_once(
                            connection,
                            run_context.run_id,
                            rejection,
                        )
                    else:
                        _insert_accepted_record(
                            connection,
                            run_context,
                            "customers",
                            customer.source_record,
                            customer.customer_id,
                            outcome,
                        )
                        if outcome == "inserted":
                            inserted_by_dataset["customers"] += 1
                        else:
                            unchanged_by_dataset["customers"] += 1

                for order in order_result.transformed_records:
                    if not isinstance(order, TransformedOrder):
                        raise TypeError(
                            "order result contains a non-order record"
                        )
                    outcome, rejection = _publish_order(
                        connection,
                        run_context,
                        order,
                    )
                    if rejection is not None:
                        load_rejections.append(rejection)
                        _insert_rejection_once(
                            connection,
                            run_context.run_id,
                            rejection,
                        )
                    else:
                        _insert_accepted_record(
                            connection,
                            run_context,
                            "orders",
                            order.source_record,
                            order.order_id,
                            outcome,
                        )
                        if outcome == "inserted":
                            inserted_by_dataset["orders"] += 1
                        else:
                            unchanged_by_dataset["orders"] += 1

                for dataset_name in _DATASET_NAMES:
                    _finalize_dataset_metrics(
                        connection,
                        run_context.run_id,
                        dataset_name,
                        inserted_by_dataset[dataset_name],
                        unchanged_by_dataset[dataset_name],
                    )

                run_status = _final_run_status(connection, run_context.run_id)
                connection.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = ?, ended_at = ?, error_message = NULL
                    WHERE run_id = ?
                    """,
                    (run_status, _utc_now(), run_context.run_id),
                )

    except BaseException as publish_error:
        reconciliation_status = (
            "incomplete_batch_failure"
            if isinstance(publish_error, LoadBatchError)
            else "incomplete_publish_failure"
        )
        try:
            _record_failure(
                database_path,
                run_context,
                publish_error,
                reconciliation_status,
                load_rejections,
            )
        except BaseException as failure_recording_error:
            raise BaseExceptionGroup(
                "Publishing failed and failure details could not be recorded.",
                [publish_error, failure_recording_error],
            ) from None
        raise
    return _read_load_result(database_path, run_context.run_id)


def fail_run(
    database_path: str | Path,
    run_context: RunContext,
    error: Exception | BatchError,
    reconciliation_status: str,
) -> None:
    """Record a fatal run outcome independently of a rolled-back publish."""
    _record_failure(
        database_path,
        run_context,
        error,
        reconciliation_status,
        (),
    )


def _publish_customer(
    connection: sqlite3.Connection,
    run_context: RunContext,
    customer: TransformedCustomer,
) -> tuple[str, RowRejection | None]:
    existing = connection.execute(
        """
        SELECT first_name, last_name, email, phone, signup_date,
               country_code, status
        FROM customers
        WHERE customer_id = ?
        """,
        (customer.customer_id,),
    ).fetchone()
    business_values = (
        customer.first_name,
        customer.last_name,
        customer.email,
        customer.phone,
        customer.signup_date,
        customer.country_code,
        customer.status,
    )
    if existing is not None:
        if tuple(existing) == business_values:
            return "unchanged", None
        return (
            "rejected",
            _load_rejection(
                customer.source_record,
                customer.customer_id,
                "existing_id_conflict",
                "Customer ID already exists with different business data.",
            ),
        )

    _savepoint_insert(
        connection,
        "customer_insert",
        """
        INSERT INTO customers (
            customer_id, first_name, last_name, email, phone, signup_date,
            country_code, status, source_file, source_row_number,
            first_seen_run_id, loaded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer.customer_id,
            *business_values,
            customer.source_record.source_file,
            customer.source_record.source_row_number,
            run_context.run_id,
            _utc_now(),
        ),
    )
    return "inserted", None


def _publish_order(
    connection: sqlite3.Connection,
    run_context: RunContext,
    order: TransformedOrder,
) -> tuple[str, RowRejection | None]:
    existing = connection.execute(
        """
        SELECT customer_id, order_ts, total_amount_cents, currency_code, status
        FROM orders
        WHERE order_id = ?
        """,
        (order.order_id,),
    ).fetchone()
    business_values = (
        order.customer_id,
        order.order_ts,
        order.total_amount_cents,
        order.currency_code,
        order.status,
    )
    if existing is not None:
        if tuple(existing) == business_values:
            return "unchanged", None
        return (
            "rejected",
            _load_rejection(
                order.source_record,
                order.order_id,
                "existing_id_conflict",
                "Order ID already exists with different business data.",
            ),
        )

    connection.execute("SAVEPOINT order_insert")
    try:
        connection.execute(
            """
            INSERT INTO orders (
                order_id, customer_id, order_ts, total_amount_cents,
                currency_code, status, source_file, source_row_number,
                first_seen_run_id, loaded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.order_id,
                *business_values,
                order.source_record.source_file,
                order.source_record.source_row_number,
                run_context.run_id,
                _utc_now(),
            ),
        )
    except sqlite3.IntegrityError as error:
        connection.execute("ROLLBACK TO SAVEPOINT order_insert")
        connection.execute("RELEASE SAVEPOINT order_insert")
        if _is_foreign_key_error(error) and not _customer_exists(
            connection,
            order.customer_id,
        ):
            return (
                "rejected",
                _load_rejection(
                    order.source_record,
                    order.order_id,
                    "invalid_customer_reference",
                    "Order references a customer not present in SQLite.",
                ),
            )
        raise
    else:
        connection.execute("RELEASE SAVEPOINT order_insert")
    return "inserted", None


def _savepoint_insert(
    connection: sqlite3.Connection,
    savepoint: str,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        connection.execute(statement, parameters)
    except BaseException:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise
    else:
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def _insert_accepted_record(
    connection: sqlite3.Connection,
    run_context: RunContext,
    dataset_name: DatasetName,
    source_record: ExtractedRecord,
    record_key: str,
    outcome: str,
) -> None:
    connection.execute(
        """
        INSERT INTO accepted_records (
            run_id, dataset_name, source_file, source_row_number,
            record_key, outcome, accepted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_context.run_id,
            dataset_name,
            source_record.source_file,
            source_record.source_row_number,
            record_key,
            outcome,
            _utc_now(),
        ),
    )


def _load_rejection(
    source_record: ExtractedRecord,
    record_key: str | None,
    reason_code: str,
    reason_detail: str,
) -> RowRejection:
    return RowRejection(
        dataset_name=source_record.dataset_name,
        source_file=source_record.source_file,
        source_row_number=source_record.source_row_number,
        physical_line_start=source_record.physical_line_start,
        physical_line_end=source_record.physical_line_end,
        record_key=record_key,
        raw_record=source_record.raw_record,
        issues=(
            ValidationIssue(
                reason_code=reason_code,
                reason_detail=reason_detail,
            ),
        ),
    )


def _insert_rejection_once(
    connection: sqlite3.Connection,
    run_id: str,
    rejection: RowRejection,
) -> None:
    if rejection.dataset_name not in _DATASET_NAMES:
        raise ValueError("Rejected row has an unsupported dataset name")

    if rejection.source_row_number is None:
        existing = connection.execute(
            """
            SELECT 1
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = ? AND source_file = ?
              AND source_row_number IS NULL
              AND record_key IS ? AND raw_record = ?
            LIMIT 1
            """,
            (
                run_id,
                rejection.dataset_name,
                rejection.source_file,
                rejection.record_key,
                rejection.raw_record,
            ),
        ).fetchone()
    else:
        existing = connection.execute(
            """
            SELECT 1
            FROM rejected_records
            WHERE run_id = ? AND dataset_name = ? AND source_file = ?
              AND source_row_number = ?
            LIMIT 1
            """,
            (
                run_id,
                rejection.dataset_name,
                rejection.source_file,
                rejection.source_row_number,
            ),
        ).fetchone()
    if existing is not None:
        return

    connection.execute(
        """
        INSERT INTO rejected_records (
            run_id, dataset_name, source_file, source_row_number,
            physical_line_start, physical_line_end, record_key,
            reason_code, reason_detail, raw_record, rejected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            rejection.dataset_name,
            rejection.source_file,
            rejection.source_row_number,
            rejection.physical_line_start,
            rejection.physical_line_end,
            rejection.record_key,
            rejection.reason_code,
            rejection.reason_detail or rejection.reason_code,
            rejection.raw_record,
            _utc_now(),
        ),
    )


def _insert_batch_error_once(
    connection: sqlite3.Connection,
    run_id: str,
    error: BatchError,
) -> None:
    if error.stage not in _ALLOWED_ERROR_STAGES:
        raise ValueError(f"Unsupported batch-error stage: {error.stage}")
    dataset_name = error.dataset_name
    if dataset_name is not None and dataset_name not in _DATASET_NAMES:
        raise ValueError(f"Unsupported batch-error dataset: {dataset_name}")

    existing = connection.execute(
        """
        SELECT 1
        FROM batch_errors
        WHERE run_id = ?
          AND dataset_name IS ?
          AND source_file IS ?
          AND stage = ?
          AND error_code = ?
          AND error_message = ?
          AND physical_line IS ?
        LIMIT 1
        """,
        (
            run_id,
            dataset_name,
            error.source_file or None,
            error.stage,
            error.error_code,
            error.error_message,
            error.physical_line,
        ),
    ).fetchone()
    if existing is not None:
        return

    connection.execute(
        """
        INSERT INTO batch_errors (
            run_id, dataset_name, source_file, stage, error_code,
            error_message, physical_line, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            dataset_name,
            error.source_file or None,
            error.stage,
            error.error_code,
            error.error_message,
            error.physical_line,
            _utc_now(),
        ),
    )


def _collect_rejections(
    extractions: Mapping[DatasetName, ExtractionResult | None],
    transformations: Mapping[DatasetName, TransformationResult],
) -> tuple[RowRejection, ...]:
    collected: list[RowRejection] = []
    seen: set[tuple[object, ...]] = set()
    for dataset_name in _DATASET_NAMES:
        extraction = extractions[dataset_name]
        if extraction is not None:
            for malformed in extraction.malformed_rows:
                rejection = _malformed_rejection(extraction, malformed)
                _append_rejection_once(collected, seen, rejection)
        for rejection in transformations[dataset_name].row_rejections:
            _append_rejection_once(collected, seen, rejection)
    return tuple(collected)


def _append_rejection_once(
    collected: list[RowRejection],
    seen: set[tuple[object, ...]],
    rejection: RowRejection,
) -> None:
    occurrence = (
        rejection.dataset_name,
        rejection.source_file,
        rejection.source_row_number,
        rejection.record_key if rejection.source_row_number is None else None,
        rejection.raw_record if rejection.source_row_number is None else None,
    )
    if occurrence not in seen:
        seen.add(occurrence)
        collected.append(rejection)


def _malformed_rejection(
    extraction: ExtractionResult,
    malformed: MalformedCsvRecord,
) -> RowRejection:
    raw_values = malformed.raw_values
    headers = extraction.headers or ()
    key_column = "customer_id" if extraction.dataset_name == "customers" else "order_id"
    record_key: str | None = None
    if key_column in headers:
        key_index = headers.index(key_column)
        if key_index < len(raw_values):
            record_key = raw_values[key_index]
    return RowRejection(
        dataset_name=extraction.dataset_name,
        source_file=malformed.source_file,
        source_row_number=malformed.source_row_number,
        physical_line_start=malformed.physical_line_start,
        physical_line_end=malformed.physical_line_end,
        record_key=record_key,
        raw_record=malformed.raw_record,
        issues=(
            ValidationIssue(
                reason_code=malformed.reason_code,
                reason_detail=malformed.reason_detail,
            ),
        ),
    )


def _collect_batch_errors(
    extraction_batch: ExtractionBatchResult,
    customer_result: TransformationResult,
    order_result: TransformationResult,
) -> tuple[BatchError, ...]:
    candidates = list(extraction_batch.batch_errors)
    for extraction in (extraction_batch.customers, extraction_batch.orders):
        if extraction is not None and extraction.batch_error is not None:
            candidates.append(extraction.batch_error)
    candidates.extend(customer_result.batch_errors)
    candidates.extend(order_result.batch_errors)

    unique: list[BatchError] = []
    seen: set[tuple[object, ...]] = set()
    for error in candidates:
        identity = (
            error.dataset_name,
            error.source_file,
            error.stage,
            error.error_code,
            error.error_message,
            error.physical_line,
        )
        if identity not in seen:
            seen.add(identity)
            unique.append(error)
    return tuple(unique)


def _extractions_by_dataset(
    extraction_batch: ExtractionBatchResult,
) -> dict[DatasetName, ExtractionResult | None]:
    return {
        "customers": extraction_batch.customers,
        "orders": extraction_batch.orders,
    }


def _transformations_by_dataset(
    customer_result: TransformationResult,
    order_result: TransformationResult,
) -> dict[DatasetName, TransformationResult]:
    if customer_result.dataset_name != "customers":
        raise ValueError("customer_result must describe customers")
    if order_result.dataset_name != "orders":
        raise ValueError("order_result must describe orders")
    return {
        "customers": customer_result,
        "orders": order_result,
    }


def _update_pre_publish_metrics(
    connection: sqlite3.Connection,
    run_id: str,
    dataset_name: DatasetName,
    extraction: ExtractionResult | None,
    transformation: TransformationResult,
    rejections: tuple[RowRejection, ...],
    *,
    has_batch_errors: bool,
) -> None:
    if extraction is None or not extraction.complete:
        values: tuple[object, ...] = (
            None,
            None,
            None,
            None,
            None,
            None,
        )
    else:
        rows_read = extraction.rows_read
        malformed_rows = len(extraction.malformed_rows)
        rows_parsed = rows_read - malformed_rows
        if has_batch_errors:
            valid_candidates = None
            rejected_parsed = None
            rows_rejected = None
        else:
            malformed_occurrences = {
                (row.source_file, row.source_row_number)
                for row in extraction.malformed_rows
            }
            parsed_rejection_occurrences = {
                (row.source_file, row.source_row_number)
                for row in rejections
                if row.dataset_name == dataset_name
                and (row.source_file, row.source_row_number)
                not in malformed_occurrences
            }
            rejected_parsed = len(parsed_rejection_occurrences)
            valid_candidates = rows_parsed - rejected_parsed
            if valid_candidates != len(transformation.transformed_records):
                raise ValueError(
                    f"{dataset_name} row outcomes do not reconcile: "
                    f"{valid_candidates} candidates but "
                    f"{len(transformation.transformed_records)} transformed rows."
                )
            rows_rejected = malformed_rows + rejected_parsed
        values = (
            rows_read,
            rows_parsed,
            malformed_rows,
            valid_candidates,
            rejected_parsed,
            rows_rejected,
        )

    connection.execute(
        """
        UPDATE run_dataset_metrics
        SET rows_read = ?,
            rows_parsed = ?,
            malformed_rows = ?,
            rows_valid_candidates = ?,
            rows_rejected_parsed = ?,
            rows_rejected = ?,
            status = 'running',
            error_message = NULL
        WHERE run_id = ? AND dataset_name = ?
        """,
        (*values, run_id, dataset_name),
    )


def _finalize_dataset_metrics(
    connection: sqlite3.Connection,
    run_id: str,
    dataset_name: DatasetName,
    inserted: int,
    unchanged: int,
) -> None:
    rejection_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM rejected_records
        WHERE run_id = ? AND dataset_name = ?
        """,
        (run_id, dataset_name),
    ).fetchone()[0]
    accepted = inserted + unchanged
    status = "completed_with_rejects" if rejection_count else "succeeded"
    connection.execute(
        """
        UPDATE run_dataset_metrics
        SET rows_accepted = ?,
            rows_rejected = ?,
            rows_inserted = ?,
            rows_unchanged = ?,
            status = ?,
            reconciliation_status = 'complete',
            ended_at = ?,
            error_message = NULL
        WHERE run_id = ? AND dataset_name = ?
        """,
        (
            accepted,
            rejection_count,
            inserted,
            unchanged,
            status,
            _utc_now(),
            run_id,
            dataset_name,
        ),
    )


def _final_run_status(connection: sqlite3.Connection, run_id: str) -> str:
    rejected = connection.execute(
        "SELECT COUNT(*) FROM rejected_records WHERE run_id = ?",
        (run_id,),
    ).fetchone()[0]
    return "completed_with_rejects" if rejected else "succeeded"


def _record_failure(
    database_path: str | Path,
    run_context: RunContext,
    error: BaseException | BatchError,
    reconciliation_status: str,
    additional_rejections: tuple[RowRejection, ...] | list[RowRejection],
) -> None:
    if reconciliation_status not in _RECONCILIATION_STATUSES:
        raise ValueError(
            "reconciliation_status must be incomplete_batch_failure or "
            "incomplete_publish_failure"
        )
    batch_error = _as_batch_error(error)
    with database_connection(database_path) as connection:
        with transaction(connection):
            _require_run(connection, run_context.run_id)
            for rejection in additional_rejections:
                _insert_rejection_once(connection, run_context.run_id, rejection)
            _insert_batch_error_once(connection, run_context.run_id, batch_error)
            ended_at = _utc_now()
            message = batch_error.error_message
            connection.execute(
                """
                UPDATE pipeline_runs
                SET status = 'failed', ended_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                (ended_at, message, run_context.run_id),
            )
            for dataset_name in _DATASET_NAMES:
                rejection_count = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM rejected_records
                    WHERE run_id = ? AND dataset_name = ?
                    """,
                    (run_context.run_id, dataset_name),
                ).fetchone()[0]
                connection.execute(
                    """
                    UPDATE run_dataset_metrics
                    SET status = 'failed',
                        reconciliation_status = ?,
                        rows_accepted = 0,
                        rows_inserted = 0,
                        rows_unchanged = 0,
                        rows_rejected = ?,
                        ended_at = ?,
                        error_message = ?
                    WHERE run_id = ? AND dataset_name = ?
                    """,
                    (
                        reconciliation_status,
                        rejection_count,
                        ended_at,
                        message,
                        run_context.run_id,
                        dataset_name,
                    ),
                )


def _as_batch_error(error: BaseException | BatchError) -> BatchError:
    if isinstance(error, BatchError):
        return error
    return BatchError(
        dataset_name=None,
        source_file="",
        stage="load",
        error_code="load_failure",
        error_message=str(error) or type(error).__name__,
    )


def _preflight_error(
    extraction_batch: ExtractionBatchResult,
    customer_result: TransformationResult,
    order_result: TransformationResult,
) -> LoadBatchError | None:
    errors = _collect_batch_errors(
        extraction_batch,
        customer_result,
        order_result,
    )
    incomplete_datasets = [
        dataset_name
        for dataset_name, extraction in _extractions_by_dataset(
            extraction_batch
        ).items()
        if extraction is None or not extraction.complete
    ]
    if not errors and not incomplete_datasets:
        return None

    details = [
        f"{error.dataset_name or 'run'}:{error.error_code}"
        for error in errors
    ]
    details.extend(
        f"{dataset_name}:incomplete_extraction"
        for dataset_name in incomplete_datasets
    )
    return LoadBatchError(
        "Batch is not publishable: " + "; ".join(details)
    )


def _require_run(connection: sqlite3.Connection, run_id: str) -> None:
    row = connection.execute(
        "SELECT 1 FROM pipeline_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Run {run_id} does not exist")


def _require_running_run(connection: sqlite3.Connection, run_id: str) -> None:
    row = connection.execute(
        "SELECT status FROM pipeline_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None or row["status"] != "running":
        raise ValueError(f"Run {run_id} is not in running state")


def _customer_exists(connection: sqlite3.Connection, customer_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM customers WHERE customer_id = ?",
            (customer_id,),
        ).fetchone()
        is not None
    )


def _is_foreign_key_error(error: sqlite3.IntegrityError) -> bool:
    return getattr(error, "sqlite_errorname", None) == "SQLITE_CONSTRAINT_FOREIGNKEY"


def _read_load_result(database_path: str | Path, run_id: str) -> LoadResult:
    with database_connection(database_path) as connection:
        run = connection.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if run is None:
            raise ValueError(f"Run {run_id} does not exist")
        metrics = {
            row["dataset_name"]: DatasetLoadMetrics(
                dataset_name=row["dataset_name"],
                rows_read=row["rows_read"],
                rows_parsed=row["rows_parsed"],
                malformed_rows=row["malformed_rows"],
                rows_valid_candidates=row["rows_valid_candidates"],
                rows_rejected_parsed=row["rows_rejected_parsed"],
                rows_accepted=row["rows_accepted"],
                rows_rejected=row["rows_rejected"],
                rows_inserted=row["rows_inserted"],
                rows_unchanged=row["rows_unchanged"],
                status=row["status"],
                reconciliation_status=row["reconciliation_status"],
            )
            for row in connection.execute(
                """
                SELECT *
                FROM run_dataset_metrics
                WHERE run_id = ?
                """,
                (run_id,),
            )
        }
    return LoadResult(
        run_id=run_id,
        status=run["status"],
        customer_metrics=metrics["customers"],
        order_metrics=metrics["orders"],
    )


def _safe_filename(path: str | Path) -> str:
    """Keep local parent directories out of persisted source filename values."""
    return Path(str(path).replace("\\", "/")).name


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
