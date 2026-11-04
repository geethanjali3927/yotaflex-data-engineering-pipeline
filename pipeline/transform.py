"""Deterministic transformations for validated customer and order records."""

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import TypeAlias

from pipeline.extract import BatchError, DatasetName, ExtractedRecord
from pipeline.validate import (
    RowRejection,
    ValidationIssue,
    ValidationResult,
)


SQLITE_INTEGER_MAX = 9_223_372_036_854_775_807
_UTC_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?"
    r"(?:Z|[+-](?:(?:0\d|1[0-3]):[0-5]\d|14:00))$"
)


@dataclass(frozen=True, slots=True)
class TransformedCustomer:
    """A normalized customer row and its unchanged source record."""

    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str | None
    signup_date: str
    country_code: str
    status: str
    source_record: ExtractedRecord


@dataclass(frozen=True, slots=True)
class TransformedOrder:
    """A normalized order row and its unchanged source record."""

    order_id: str
    customer_id: str
    order_ts: str
    total_amount_cents: int
    currency_code: str
    status: str
    source_record: ExtractedRecord


TransformedRecord: TypeAlias = TransformedCustomer | TransformedOrder


@dataclass(frozen=True, slots=True)
class TransformationResult:
    """Transformed rows, row-level rejections, and batch errors."""

    dataset_name: DatasetName
    transformed_records: tuple[TransformedRecord, ...]
    row_rejections: tuple[RowRejection, ...]
    batch_errors: tuple[BatchError, ...]

    @property
    def complete(self) -> bool:
        """Return whether transformation had no batch-level errors."""
        return not self.batch_errors


def transform_customers(
    validation_result: ValidationResult,
) -> TransformationResult:
    """Normalize valid customer records while retaining their source records."""
    if validation_result.dataset_name != "customers":
        return _dataset_mismatch_result(validation_result, "customers")
    if validation_result.batch_errors:
        return TransformationResult(
            dataset_name="customers",
            transformed_records=(),
            row_rejections=validation_result.row_rejections,
            batch_errors=validation_result.batch_errors,
        )
    if validation_result.headers is None:
        return _missing_headers_result(validation_result, "customers")

    transformed: list[TransformedCustomer] = []
    rejections = list(validation_result.row_rejections)
    for record in validation_result.valid_records:
        values = _map_record(record, validation_result.headers)
        if values is None:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_record_shape",
                    "Record values do not match the validation headers.",
                    _record_key(record, validation_result.headers, "customer_id"),
                )
            )
            continue
        missing_fields = set(
            (
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "signup_date",
                "country_code",
                "status",
            )
        ) - values.keys()
        if missing_fields:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_missing_field",
                    "Validated customer is missing required fields: "
                    + ", ".join(sorted(missing_fields))
                    + ".",
                    values.get("customer_id"),
                )
            )
            continue

        try:
            phone = values.get("phone") if "phone" in values else None
            transformed.append(
                TransformedCustomer(
                    customer_id=values["customer_id"].strip(),
                    first_name=values["first_name"].strip(),
                    last_name=values["last_name"].strip(),
                    email=values["email"].strip().lower(),
                    phone=phone,
                    signup_date=values["signup_date"],
                    country_code=values["country_code"].strip().upper(),
                    status=values["status"],
                    source_record=record,
                )
            )
        except (KeyError, AttributeError) as error:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_missing_field",
                    f"Validated customer is missing a required field ({error}).",
                    _record_key(record, validation_result.headers, "customer_id"),
                )
            )

    return TransformationResult(
        dataset_name="customers",
        transformed_records=tuple(transformed),
        row_rejections=tuple(rejections),
        batch_errors=(),
    )


def transform_orders(
    validation_result: ValidationResult,
) -> TransformationResult:
    """Normalize valid orders, UTC timestamps, and integer minor-unit amounts."""
    if validation_result.dataset_name != "orders":
        return _dataset_mismatch_result(validation_result, "orders")
    if validation_result.batch_errors:
        return TransformationResult(
            dataset_name="orders",
            transformed_records=(),
            row_rejections=validation_result.row_rejections,
            batch_errors=validation_result.batch_errors,
        )
    if validation_result.headers is None:
        return _missing_headers_result(validation_result, "orders")

    transformed: list[TransformedOrder] = []
    rejections = list(validation_result.row_rejections)
    for record in validation_result.valid_records:
        values = _map_record(record, validation_result.headers)
        if values is None:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_record_shape",
                    "Record values do not match the validation headers.",
                    _record_key(record, validation_result.headers, "order_id"),
                )
            )
            continue
        missing_fields = set(
            (
                "order_id",
                "customer_id",
                "order_ts",
                "total_amount_cents",
                "currency_code",
                "status",
            )
        ) - values.keys()
        if missing_fields:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_missing_field",
                    "Validated order is missing required fields: "
                    + ", ".join(sorted(missing_fields))
                    + ".",
                    values.get("order_id"),
                )
            )
            continue

        try:
            order_ts = _normalize_order_timestamp(values["order_ts"])
        except (ValueError, OverflowError) as error:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_invalid_timestamp",
                    f"order_ts could not be normalized to UTC ({error}).",
                    values.get("order_id"),
                )
            )
            continue

        amount_text = values.get("total_amount_cents")
        try:
            if amount_text is None or re.fullmatch(r"[0-9]+", amount_text) is None:
                raise ValueError("amount is not an integer written with digits")
            amount = int(amount_text)
        except ValueError as error:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_invalid_amount",
                    f"total_amount_cents could not be converted to an integer ({error}).",
                    values.get("order_id"),
                )
            )
            continue

        if amount > SQLITE_INTEGER_MAX:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_amount_out_of_range",
                    "total_amount_cents exceeds SQLite's signed 64-bit integer limit.",
                    values.get("order_id"),
                )
            )
            continue

        try:
            transformed.append(
                TransformedOrder(
                    order_id=values["order_id"].strip(),
                    customer_id=values["customer_id"].strip(),
                    order_ts=order_ts,
                    total_amount_cents=amount,
                    currency_code=values["currency_code"],
                    status=values["status"],
                    source_record=record,
                )
            )
        except (KeyError, AttributeError) as error:
            rejections.append(
                _transformation_rejection(
                    record,
                    "transformation_missing_field",
                    f"Validated order is missing a required field ({error}).",
                    values.get("order_id"),
                )
            )

    return TransformationResult(
        dataset_name="orders",
        transformed_records=tuple(transformed),
        row_rejections=tuple(rejections),
        batch_errors=(),
    )


def _map_record(
    record: ExtractedRecord,
    headers: tuple[str, ...],
) -> dict[str, str] | None:
    if len(record.raw_values) != len(headers):
        return None
    return dict(zip(headers, record.raw_values))


def _normalize_order_timestamp(value: str) -> str:
    if _UTC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "timestamp is outside the supported timezone or precision format"
        )
    timestamp = datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    utc_timestamp = timestamp.astimezone(timezone.utc)
    timespec = "microseconds" if utc_timestamp.microsecond else "seconds"
    return utc_timestamp.isoformat(timespec=timespec)


def _record_key(
    record: ExtractedRecord,
    headers: tuple[str, ...],
    key_column: str,
) -> str | None:
    try:
        return record.raw_values[headers.index(key_column)]
    except (ValueError, IndexError):
        return None


def _transformation_rejection(
    record: ExtractedRecord,
    reason_code: str,
    reason_detail: str,
    record_key: str | None,
) -> RowRejection:
    return RowRejection(
        dataset_name=record.dataset_name,
        source_file=record.source_file,
        source_row_number=record.source_row_number,
        physical_line_start=record.physical_line_start,
        physical_line_end=record.physical_line_end,
        record_key=record_key,
        raw_record=record.raw_record,
        issues=(
            ValidationIssue(
                reason_code=reason_code,
                reason_detail=reason_detail,
                field_name=None,
            ),
        ),
    )


def _dataset_mismatch_result(
    validation_result: ValidationResult,
    expected_dataset: DatasetName,
) -> TransformationResult:
    source_file = (
        validation_result.valid_records[0].source_file
        if validation_result.valid_records
        else "<unknown>"
    )
    error = BatchError(
        dataset_name=validation_result.dataset_name,
        source_file=source_file,
        stage="transform",
        error_code="transformation_dataset_mismatch",
        error_message=(
            f"Expected {expected_dataset} validation result but received "
            f"{validation_result.dataset_name}."
        ),
    )
    return TransformationResult(
        dataset_name=expected_dataset,
        transformed_records=(),
        row_rejections=validation_result.row_rejections,
        batch_errors=(*validation_result.batch_errors, error),
    )


def _missing_headers_result(
    validation_result: ValidationResult,
    dataset_name: DatasetName,
) -> TransformationResult:
    error = BatchError(
        dataset_name=dataset_name,
        source_file=(
            validation_result.valid_records[0].source_file
            if validation_result.valid_records
            else "<unknown>"
        ),
        stage="transform",
        error_code="transformation_missing_headers",
        error_message="Validation result has no source headers.",
    )
    return TransformationResult(
        dataset_name=dataset_name,
        transformed_records=(),
        row_rejections=validation_result.row_rejections,
        batch_errors=(*validation_result.batch_errors, error),
    )
