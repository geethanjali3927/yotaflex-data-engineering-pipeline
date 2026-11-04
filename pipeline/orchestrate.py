"""End-to-end orchestration for the customer and order data pipeline."""

import argparse
from collections.abc import Mapping, Sequence
from pathlib import Path
import sys

from pipeline.database import initialize_database
from pipeline.extract import (
    BatchError,
    DatasetName,
    ExtractionBatchResult,
    ExtractionResult,
    extract_required_csv_files,
)
from pipeline.load import (
    DatasetLoadMetrics,
    LoadResult,
    RunContext,
    fail_run,
    publish_batch,
    start_run,
)
from pipeline.transform import (
    TransformationResult,
    transform_customers,
    transform_orders,
)
from pipeline.validate import (
    ValidationResult,
    build_customer_reference_map,
    validate_customers,
    validate_order_schema,
    validate_orders,
)


def run_pipeline(
    database_path: str | Path,
    customers_path: str | Path,
    orders_path: str | Path,
    config: Mapping[str, object] | None = None,
) -> LoadResult:
    """Run extraction through atomic publication for both required datasets."""
    initialize_database(database_path)
    run_context = start_run(
        database_path,
        {
            "customers": customers_path,
            "orders": orders_path,
        },
        config=config,
    )

    try:
        extraction_batch = extract_required_csv_files(
            customers_path,
            orders_path,
        )

        customer_validation = (
            validate_customers(extraction_batch.customers)
            if extraction_batch.customers is not None
            else None
        )
        customer_result = (
            transform_customers(customer_validation)
            if customer_validation is not None
            else _empty_transformation_result("customers")
        )

        if (
            extraction_batch.orders is None
            or not extraction_batch.orders.complete
        ):
            order_result = _empty_transformation_result("orders")
        elif customer_validation is None or customer_validation.batch_errors:
            order_result = _orders_without_customer_references(
                extraction_batch.orders
            )
        else:
            customer_references = build_customer_reference_map(
                customer_validation
            )
            order_validation = validate_orders(
                extraction_batch.orders,
                customer_references,
            )
            order_result = transform_orders(order_validation)
    except Exception as error:
        try:
            fail_run(
                database_path,
                run_context,
                error,
                "incomplete_batch_failure",
            )
        except Exception as failure_recording_error:
            raise ExceptionGroup(
                "Pipeline processing failed and the run failure could not be recorded.",
                [error, failure_recording_error],
            ) from None
        raise

    return publish_batch(
        database_path,
        run_context,
        extraction_batch,
        customer_result,
        order_result,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline from command-line database and CSV paths."""
    parser = argparse.ArgumentParser(
        description="Load customer and order CSV files into SQLite."
    )
    parser.add_argument(
        "--database",
        required=True,
        type=Path,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--customers",
        required=True,
        type=Path,
        help="Path to the customers CSV file.",
    )
    parser.add_argument(
        "--orders",
        required=True,
        type=Path,
        help="Path to the orders CSV file.",
    )
    arguments = parser.parse_args(argv)

    try:
        result = run_pipeline(
            arguments.database,
            arguments.customers,
            arguments.orders,
        )
    except Exception as error:
        print(f"Pipeline failed: {error}", file=sys.stderr)
        return 1

    print(f"Run ID: {result.run_id}")
    print(f"Status: {result.status}")
    _print_dataset_metrics("customers", result.customer_metrics)
    _print_dataset_metrics("orders", result.order_metrics)
    return 0 if result.status in ("succeeded", "completed_with_rejects") else 1


def _empty_transformation_result(
    dataset_name: DatasetName,
) -> TransformationResult:
    return TransformationResult(
        dataset_name=dataset_name,
        transformed_records=(),
        row_rejections=(),
        batch_errors=(),
    )


def _orders_without_customer_references(
    order_extraction: ExtractionResult,
) -> TransformationResult:
    order_schema_errors = validate_order_schema(
        order_extraction.headers,
        order_extraction.source_file,
    )
    dependency_error = BatchError(
        dataset_name="orders",
        source_file=order_extraction.source_file,
        stage="validation",
        error_code="customer_reference_unavailable",
        error_message=(
            "Orders could not be validated because customer validation "
            "did not provide a reliable customer reference map."
        ),
    )
    validation_result = ValidationResult(
        dataset_name="orders",
        headers=order_extraction.headers,
        valid_records=(),
        row_rejections=(),
        batch_errors=(
            *order_schema_errors,
            dependency_error,
        ),
    )
    return transform_orders(validation_result)


def _print_dataset_metrics(
    dataset_name: DatasetName,
    metrics: DatasetLoadMetrics,
) -> None:
    print(
        f"{dataset_name}: status={metrics.status}, "
        f"rows_read={metrics.rows_read}, "
        f"rows_parsed={metrics.rows_parsed}, "
        f"malformed_rows={metrics.malformed_rows}, "
        f"rows_valid_candidates={metrics.rows_valid_candidates}, "
        f"rows_rejected_parsed={metrics.rows_rejected_parsed}, "
        f"rows_accepted={metrics.rows_accepted}, "
        f"rows_rejected={metrics.rows_rejected}, "
        f"rows_inserted={metrics.rows_inserted}, "
        f"rows_unchanged={metrics.rows_unchanged}, "
        f"reconciliation={metrics.reconciliation_status}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
