"""Reproducible stage-level performance benchmarks for the local pipeline."""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import tempfile
import time
from typing import Any, Sequence

from pipeline.database import initialize_database
from pipeline.extract import extract_required_csv_files
from pipeline.generate_data import generate_data
from pipeline.load import publish_batch, start_run
from pipeline.transform import transform_customers, transform_orders
from pipeline.validate import (
    build_customer_reference_map,
    validate_customers,
    validate_orders,
)


_TIMED_STAGES = (
    "generation",
    "extraction",
    "validation",
    "transformation",
    "loading",
    "total_pipeline",
)


def run_benchmark(
    customers: int = 1000,
    orders: int = 5000,
    iterations: int = 1,
    seed: int = 42,
    output: str | Path = Path("data") / "output" / "benchmark",
) -> dict[str, Any]:
    """Run isolated benchmark iterations and write detailed and summary files."""
    if customers < 0:
        raise ValueError("customers must be non-negative")
    if orders < 0:
        raise ValueError("orders must be non-negative")
    if orders > 0 and customers == 0:
        raise ValueError("orders cannot be generated without customers")
    if iterations < 1:
        raise ValueError("iterations must be at least one")

    python_version = platform.python_version()
    iteration_results: list[dict[str, Any]] = []
    database_paths: set[str] = set()

    for iteration in range(1, iterations + 1):
        with tempfile.TemporaryDirectory(
            prefix=f"pipeline-benchmark-{iteration}-"
        ) as temp_directory:
            temporary_root = Path(temp_directory)
            input_directory = temporary_root / "input"
            database_path = temporary_root / "pipeline.sqlite"
            resolved_database_path = str(database_path.resolve())
            if resolved_database_path in database_paths:
                raise RuntimeError(
                    "Benchmark iterations unexpectedly reused a database path."
                )
            database_paths.add(resolved_database_path)

            started = time.perf_counter()
            customer_path, order_path = generate_data(
                customers=customers,
                orders=orders,
                output_dir=input_directory,
                seed=seed,
            )
            generation_seconds = time.perf_counter() - started

            total_started = time.perf_counter()
            load_started = total_started
            initialize_database(database_path)
            run_context = start_run(
                database_path,
                {
                    "customers": customer_path,
                    "orders": order_path,
                },
                config={
                    "benchmark": True,
                    "iteration": iteration,
                    "seed": seed,
                    "customers": customers,
                    "orders": orders,
                },
            )
            load_setup_seconds = time.perf_counter() - load_started

            started = time.perf_counter()
            extraction_batch = extract_required_csv_files(
                customer_path,
                order_path,
            )
            extraction_seconds = time.perf_counter() - started

            if extraction_batch.customers is None or extraction_batch.orders is None:
                raise RuntimeError(
                    "Generated benchmark files were not both extracted."
                )

            started = time.perf_counter()
            customer_validation = validate_customers(
                extraction_batch.customers
            )
            customer_references = build_customer_reference_map(
                customer_validation
            )
            order_validation = validate_orders(
                extraction_batch.orders,
                customer_references,
            )
            validation_seconds = time.perf_counter() - started

            started = time.perf_counter()
            customer_transformation = transform_customers(customer_validation)
            order_transformation = transform_orders(order_validation)
            transformation_seconds = time.perf_counter() - started

            started = time.perf_counter()
            load_result = publish_batch(
                database_path,
                run_context,
                extraction_batch,
                customer_transformation,
                order_transformation,
            )
            publish_seconds = time.perf_counter() - started
            loading_seconds = load_setup_seconds + publish_seconds
            total_pipeline_seconds = time.perf_counter() - total_started

            customer_count = extraction_batch.customers.rows_read
            order_count = extraction_batch.orders.rows_read
            combined_count = customer_count + order_count
            durations = {
                "generation": generation_seconds,
                "extraction": extraction_seconds,
                "validation": validation_seconds,
                "transformation": transformation_seconds,
                "loading": loading_seconds,
                "total_pipeline": total_pipeline_seconds,
            }
            throughputs = _throughputs(
                durations,
                customer_count,
                order_count,
            )
            customer_metrics = load_result.customer_metrics
            order_metrics = load_result.order_metrics

            iteration_results.append(
                {
                    "iteration": iteration,
                    "python_version": python_version,
                    "seed": seed,
                    "configured_customers": customers,
                    "configured_orders": orders,
                    "generated_customers": customer_count,
                    "generated_orders": order_count,
                    "database_path": resolved_database_path,
                    **{
                        f"{stage}_seconds": duration
                        for stage, duration in durations.items()
                    },
                    **throughputs,
                    "customer_accepted": customer_metrics.rows_accepted,
                    "customer_rejected": customer_metrics.rows_rejected,
                    "customer_inserted": customer_metrics.rows_inserted,
                    "customer_unchanged": customer_metrics.rows_unchanged,
                    "customer_status": customer_metrics.status,
                    "customer_reconciliation_status": (
                        customer_metrics.reconciliation_status
                    ),
                    "order_accepted": order_metrics.rows_accepted,
                    "order_rejected": order_metrics.rows_rejected,
                    "order_inserted": order_metrics.rows_inserted,
                    "order_unchanged": order_metrics.rows_unchanged,
                    "order_status": order_metrics.status,
                    "order_reconciliation_status": (
                        order_metrics.reconciliation_status
                    ),
                    "run_status": load_result.status,
                }
            )

    summary = _summarize_durations(iteration_results)
    report = {
        "benchmark": {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "python_version": python_version,
            "configured_customers": customers,
            "configured_orders": orders,
            "seed": seed,
            "iterations": iterations,
            "timing_notes": {
                "generation": "Data generation only; excluded from total_pipeline.",
                "loading": (
                    "Database initialization, run creation, and atomic publish."
                ),
                "total_pipeline": (
                    "Database initialization through successful publish result."
                ),
                "throughput": (
                    "Generated input record counts divided by stage duration; "
                    "zero when duration is zero."
                ),
            },
        },
        "iterations": iteration_results,
        "duration_summary": summary,
    }

    output_prefix = _output_prefix(output)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = output_prefix.with_suffix(".json")
    csv_path = output_prefix.with_suffix(".csv")
    summary_json_path = output_prefix.with_name(
        f"{output_prefix.name}_summary.json"
    )
    summary_csv_path = output_prefix.with_name(
        f"{output_prefix.name}_summary.csv"
    )

    _write_json(json_path, report)
    _write_csv(csv_path, iteration_results)
    _write_json(summary_json_path, {"duration_summary": summary})
    _write_csv(summary_csv_path, _summary_rows(summary))

    print(f"Detailed JSON: {json_path}")
    print(f"Detailed CSV: {csv_path}")
    print(f"Summary JSON: {summary_json_path}")
    print(f"Summary CSV: {summary_csv_path}")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Run benchmarks from the command line."""
    parser = argparse.ArgumentParser(
        description="Benchmark extraction, validation, transformation, and loading."
    )
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data") / "output" / "benchmark",
        help="Output filename prefix; writes detailed and summary JSON/CSV files.",
    )
    arguments = parser.parse_args(argv)

    try:
        run_benchmark(
            customers=arguments.customers,
            orders=arguments.orders,
            iterations=arguments.iterations,
            seed=arguments.seed,
            output=arguments.output,
        )
    except ValueError as error:
        parser.error(str(error))
    return 0


def _throughputs(
    durations: dict[str, float],
    customers: int,
    orders: int,
) -> dict[str, float]:
    counts = {
        "customer": customers,
        "order": orders,
        "combined": customers + orders,
    }
    rates: dict[str, float] = {}
    for stage, duration in durations.items():
        for dataset, count in counts.items():
            rate = count / duration if duration > 0 else 0.0
            rates[f"{stage}_{dataset}_records_per_second"] = rate
    return rates


def _summarize_durations(
    iteration_results: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for stage in _TIMED_STAGES:
        values = [
            float(result[f"{stage}_seconds"])
            for result in iteration_results
        ]
        summary[stage] = {
            "average_seconds": sum(values) / len(values),
            "minimum_seconds": min(values),
            "maximum_seconds": max(values),
        }
    return summary


def _summary_rows(
    summary: dict[str, dict[str, float]],
) -> list[dict[str, float | str]]:
    return [
        {
            "stage": stage,
            "average_seconds": values["average_seconds"],
            "minimum_seconds": values["minimum_seconds"],
            "maximum_seconds": values["maximum_seconds"],
        }
        for stage, values in summary.items()
    ]


def _output_prefix(output: str | Path) -> Path:
    path = Path(output)
    if path.suffix.lower() in {".json", ".csv"}:
        return path.with_suffix("")
    return path


def _write_json(path: Path, value: object) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        json.dump(value, output_file, indent=2, sort_keys=True)
        output_file.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Cannot write benchmark CSV without rows")
    fieldnames = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
