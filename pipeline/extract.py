"""Reusable CSV extraction with source locations and raw-record retention."""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal, TextIO


DatasetName = Literal["customers", "orders"]


@dataclass(frozen=True, slots=True)
class ExtractedRecord:
    """A structurally parsed source record, without validation or conversion."""

    dataset_name: DatasetName
    source_file: str
    source_row_number: int
    physical_line_start: int
    physical_line_end: int
    raw_values: tuple[str, ...]
    raw_record: str


@dataclass(frozen=True, slots=True)
class MalformedCsvRecord:
    """A recoverable CSV row whose field count differs from the header."""

    dataset_name: DatasetName
    source_file: str
    source_row_number: int
    physical_line_start: int
    physical_line_end: int
    expected_field_count: int
    actual_field_count: int
    raw_values: tuple[str, ...]
    raw_record: str
    reason_code: str
    reason_detail: str


@dataclass(frozen=True, slots=True)
class BatchError:
    """An error that prevents a file or required-file batch from completing."""

    dataset_name: DatasetName | None
    source_file: str
    stage: str
    error_code: str
    error_message: str
    physical_line: int | None = None
    raw_record: str | None = None


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Extraction output and status for one input CSV file."""

    dataset_name: DatasetName
    source_file: str
    headers: tuple[str, ...] | None
    records: tuple[ExtractedRecord, ...]
    malformed_rows: tuple[MalformedCsvRecord, ...]
    rows_read: int
    is_empty_file: bool
    batch_error: BatchError | None = None

    @property
    def complete(self) -> bool:
        """Return whether the file was read through its end without fatal error."""
        return self.batch_error is None


@dataclass(frozen=True, slots=True)
class ExtractionBatchResult:
    """Results for the two required inputs, or preflight errors if either is bad."""

    customers: ExtractionResult | None
    orders: ExtractionResult | None
    batch_errors: tuple[BatchError, ...]

    @property
    def complete(self) -> bool:
        """Return whether both required files were fully extracted."""
        return (
            self.customers is not None
            and self.orders is not None
            and self.customers.complete
            and self.orders.complete
            and not self.batch_errors
        )


class _RecordLineIterator:
    """Capture original physical lines consumed for one csv.reader record."""

    def __init__(self, source: TextIO) -> None:
        self._source: Iterator[str] = iter(source)
        self.line_number = 0
        self.record_lines: list[str] = []
        self.record_line_start = 1

    def begin_record(self) -> None:
        """Start capturing the next logical CSV record."""
        self.record_lines = []
        self.record_line_start = self.line_number + 1

    def __iter__(self) -> "_RecordLineIterator":
        return self

    def __next__(self) -> str:
        line = next(self._source)
        self.line_number += 1
        self.record_lines.append(line)
        return line


def _validate_dataset_name(dataset_name: str) -> DatasetName:
    if dataset_name not in ("customers", "orders"):
        raise ValueError("dataset_name must be 'customers' or 'orders'")
    return dataset_name


def _source_filename(path: str | Path) -> str:
    """Return a filename suitable for logs and audit data, without directories."""
    return Path(str(path).replace("\\", "/")).name


def _make_file_error(
    path: str | Path,
    dataset_name: DatasetName,
    *,
    stage: str,
) -> BatchError | None:
    """Check that an input is a readable regular file without reading its data."""
    source_file = _source_filename(path)
    file_path = Path(path)

    if not file_path.exists():
        return BatchError(
            dataset_name=dataset_name,
            source_file=source_file,
            stage=stage,
            error_code="missing_file",
            error_message="Required input file does not exist.",
        )

    if not file_path.is_file():
        return BatchError(
            dataset_name=dataset_name,
            source_file=source_file,
            stage=stage,
            error_code="not_a_file",
            error_message="Input path is not a regular file.",
        )

    try:
        with file_path.open("rb"):
            pass
    except OSError as error:
        return BatchError(
            dataset_name=dataset_name,
            source_file=source_file,
            stage=stage,
            error_code="unreadable_file",
            error_message=f"Input file cannot be opened ({type(error).__name__}).",
        )

    return None


def extract_csv(
    path: str | Path,
    dataset_name: DatasetName,
) -> ExtractionResult:
    """Extract one UTF-8 CSV file, preserving source and raw text metadata."""
    dataset = _validate_dataset_name(dataset_name)
    source_file = _source_filename(path)
    file_error = _make_file_error(path, dataset, stage="input_check")
    if file_error is not None:
        return ExtractionResult(
            dataset_name=dataset,
            source_file=source_file,
            headers=None,
            records=(),
            malformed_rows=(),
            rows_read=0,
            is_empty_file=False,
            batch_error=file_error,
        )

    file_path = Path(path)
    try:
        is_empty_file = file_path.stat().st_size == 0
        with file_path.open("r", encoding="utf-8", newline="") as source:
            captured_lines = _RecordLineIterator(source)
            reader = csv.reader(captured_lines, strict=True)
            captured_lines.begin_record()
            try:
                headers = next(reader, None)
            except csv.Error as error:
                batch_error = BatchError(
                    dataset_name=dataset,
                    source_file=source_file,
                    stage="csv_parse",
                    error_code="malformed_csv_syntax",
                    error_message=f"CSV syntax error ({type(error).__name__}).",
                    physical_line=(
                        captured_lines.line_number
                        if captured_lines.line_number > 0
                        else None
                    ),
                    raw_record="".join(captured_lines.record_lines),
                )
                return ExtractionResult(
                    dataset_name=dataset,
                    source_file=source_file,
                    headers=None,
                    records=(),
                    malformed_rows=(),
                    rows_read=0,
                    is_empty_file=is_empty_file,
                    batch_error=batch_error,
                )
            except UnicodeError as error:
                batch_error = BatchError(
                    dataset_name=dataset,
                    source_file=source_file,
                    stage="csv_parse",
                    error_code="encoding_error",
                    error_message=(
                        f"CSV text decoding failed ({type(error).__name__})."
                    ),
                    physical_line=(
                        captured_lines.line_number + 1
                        if captured_lines.line_number > 0
                        else None
                    ),
                    raw_record="".join(captured_lines.record_lines),
                )
                return ExtractionResult(
                    dataset_name=dataset,
                    source_file=source_file,
                    headers=None,
                    records=(),
                    malformed_rows=(),
                    rows_read=0,
                    is_empty_file=is_empty_file,
                    batch_error=batch_error,
                )

            if headers is None:
                return ExtractionResult(
                    dataset_name=dataset,
                    source_file=source_file,
                    headers=None,
                    records=(),
                    malformed_rows=(),
                    rows_read=0,
                    is_empty_file=is_empty_file,
                )

            header_values = tuple(headers)
            records: list[ExtractedRecord] = []
            malformed_rows: list[MalformedCsvRecord] = []
            rows_read = 0

            while True:
                captured_lines.begin_record()
                try:
                    values = next(reader)
                except StopIteration:
                    break
                except csv.Error as error:
                    batch_error = BatchError(
                        dataset_name=dataset,
                        source_file=source_file,
                        stage="csv_parse",
                        error_code="malformed_csv_syntax",
                        error_message=f"CSV syntax error ({type(error).__name__}).",
                        physical_line=(
                            captured_lines.line_number
                            if captured_lines.line_number > 0
                            else None
                        ),
                        raw_record="".join(captured_lines.record_lines),
                    )
                    return ExtractionResult(
                        dataset_name=dataset,
                        source_file=source_file,
                        headers=header_values,
                        records=tuple(records),
                        malformed_rows=tuple(malformed_rows),
                        rows_read=rows_read,
                        is_empty_file=is_empty_file,
                        batch_error=batch_error,
                    )
                except UnicodeError as error:
                    batch_error = BatchError(
                        dataset_name=dataset,
                        source_file=source_file,
                        stage="csv_parse",
                        error_code="encoding_error",
                        error_message=(
                            f"CSV text decoding failed ({type(error).__name__})."
                        ),
                        physical_line=(
                            captured_lines.line_number + 1
                            if captured_lines.line_number > 0
                            else None
                        ),
                        raw_record="".join(captured_lines.record_lines),
                    )
                    return ExtractionResult(
                        dataset_name=dataset,
                        source_file=source_file,
                        headers=header_values,
                        records=tuple(records),
                        malformed_rows=tuple(malformed_rows),
                        rows_read=rows_read,
                        is_empty_file=is_empty_file,
                        batch_error=batch_error,
                    )
                except OSError as error:
                    batch_error = BatchError(
                        dataset_name=dataset,
                        source_file=source_file,
                        stage="input_check",
                        error_code="file_read_error",
                        error_message=(
                            f"Input file could not be read ({type(error).__name__})."
                        ),
                        physical_line=(
                            captured_lines.line_number + 1
                            if captured_lines.line_number > 0
                            else None
                        ),
                        raw_record="".join(captured_lines.record_lines),
                    )
                    return ExtractionResult(
                        dataset_name=dataset,
                        source_file=source_file,
                        headers=header_values,
                        records=tuple(records),
                        malformed_rows=tuple(malformed_rows),
                        rows_read=rows_read,
                        is_empty_file=is_empty_file,
                        batch_error=batch_error,
                    )

                rows_read += 1
                physical_start = captured_lines.record_line_start
                physical_end = captured_lines.line_number
                raw_record = "".join(captured_lines.record_lines)
                raw_values = tuple(values)
                source_row_number = rows_read

                if len(raw_values) != len(header_values):
                    malformed_rows.append(
                        MalformedCsvRecord(
                            dataset_name=dataset,
                            source_file=source_file,
                            source_row_number=source_row_number,
                            physical_line_start=physical_start,
                            physical_line_end=physical_end,
                            expected_field_count=len(header_values),
                            actual_field_count=len(raw_values),
                            raw_values=raw_values,
                            raw_record=raw_record,
                            reason_code="field_count_mismatch",
                            reason_detail=(
                                f"Expected {len(header_values)} fields but found "
                                f"{len(raw_values)}."
                            ),
                        )
                    )
                else:
                    records.append(
                        ExtractedRecord(
                            dataset_name=dataset,
                            source_file=source_file,
                            source_row_number=source_row_number,
                            physical_line_start=physical_start,
                            physical_line_end=physical_end,
                            raw_values=raw_values,
                            raw_record=raw_record,
                        )
                    )

            return ExtractionResult(
                dataset_name=dataset,
                source_file=source_file,
                headers=header_values,
                records=tuple(records),
                malformed_rows=tuple(malformed_rows),
                rows_read=rows_read,
                is_empty_file=is_empty_file,
            )
    except OSError as error:
        batch_error = BatchError(
            dataset_name=dataset,
            source_file=source_file,
            stage="input_check",
            error_code="file_read_error",
            error_message=f"Input file could not be read ({type(error).__name__}).",
        )
        return ExtractionResult(
            dataset_name=dataset,
            source_file=source_file,
            headers=None,
            records=(),
            malformed_rows=(),
            rows_read=0,
            is_empty_file=False,
            batch_error=batch_error,
        )


def extract_required_csv_files(
    customers_path: str | Path,
    orders_path: str | Path,
) -> ExtractionBatchResult:
    """Preflight both required files before extracting either one."""
    required_files = (
        (customers_path, "customers"),
        (orders_path, "orders"),
    )
    preflight_errors = tuple(
        error
        for path, dataset in required_files
        if (
            error := _make_file_error(
                path,
                _validate_dataset_name(dataset),
                stage="input_check",
            )
        )
        is not None
    )
    if preflight_errors:
        return ExtractionBatchResult(
            customers=None,
            orders=None,
            batch_errors=preflight_errors,
        )

    customers = extract_csv(customers_path, "customers")
    orders = extract_csv(orders_path, "orders")
    batch_errors = tuple(
        result.batch_error
        for result in (customers, orders)
        if result.batch_error is not None
    )
    return ExtractionBatchResult(
        customers=customers,
        orders=orders,
        batch_errors=batch_errors,
    )
