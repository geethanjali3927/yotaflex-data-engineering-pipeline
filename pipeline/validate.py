"""Schema and row validation for the customer and order CSV datasets."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence

from pipeline.extract import (
    BatchError,
    DatasetName,
    ExtractedRecord,
    ExtractionResult,
    MalformedCsvRecord,
)


CUSTOMER_REQUIRED_COLUMNS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "signup_date",
    "country_code",
    "status",
)
CUSTOMER_OPTIONAL_COLUMNS = ("phone",)
ORDER_REQUIRED_COLUMNS = (
    "order_id",
    "customer_id",
    "order_ts",
    "total_amount_cents",
    "currency_code",
    "status",
)

CUSTOMER_STATUSES = frozenset({"active", "inactive"})
ORDER_STATUSES = frozenset(
    {"placed", "paid", "shipped", "delivered", "cancelled", "returned"}
)

# ISO 3166-1 alpha-2 data snapshot verified 2026-11-04 against:
# https://github.com/pycountry/pycountry/blob/main/src/pycountry/databases/
# iso3166-1.json
# The snapshot contains all 249 assigned codes. Update this constant deliberately
# when adopting a newer ISO 3166-1 snapshot.
ISO_3166_1_ALPHA_2_CODES = frozenset(
    """
    AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ
    BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR
    CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR
    GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU
    ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ
    LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ
    MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF
    PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI
    SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR
    TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW
    """.split()
)

_EMAIL_ATOM = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
_EMAIL_LOCAL_PART = rf"{_EMAIL_ATOM}(?:\.{_EMAIL_ATOM})*"
_EMAIL_DOMAIN_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_EMAIL_PATTERN = re.compile(
    rf"^{_EMAIL_LOCAL_PART}@{_EMAIL_DOMAIN_LABEL}(?:\.{_EMAIL_DOMAIN_LABEL})+$"
)
_ISO_TIMEZONE = r"(?:Z|[+-](?:(?:0\d|1[0-3]):[0-5]\d|14:00))"
_ISO_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
    rf"(?::\d{{2}}(?:\.\d+)?)?{_ISO_TIMEZONE}$"
)
_INTEGER_PATTERN = re.compile(r"^[0-9]+$")


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One stable, field-specific reason why a source row is invalid."""

    reason_code: str
    reason_detail: str
    field_name: str | None = None


@dataclass(frozen=True, slots=True)
class RowRejection:
    """A rejected source occurrence with its complete available provenance."""

    dataset_name: DatasetName
    source_file: str
    source_row_number: int | None
    physical_line_start: int | None
    physical_line_end: int | None
    record_key: str | None
    raw_record: str
    issues: tuple[ValidationIssue, ...]

    @property
    def reason_code(self) -> str:
        """Return one database-friendly code for this rejected occurrence."""
        if len(self.issues) == 1:
            return self.issues[0].reason_code
        return "multiple_validation_errors"

    @property
    def reason_detail(self) -> str:
        """Combine issue details while retaining every individual explanation."""
        return "; ".join(issue.reason_detail for issue in self.issues)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Valid source rows, row rejects, and batch-level errors kept separate."""

    dataset_name: DatasetName
    headers: tuple[str, ...] | None
    valid_records: tuple[ExtractedRecord, ...]
    row_rejections: tuple[RowRejection, ...]
    batch_errors: tuple[BatchError, ...]

    @property
    def complete(self) -> bool:
        """Return whether schema and row validation completed without batch error."""
        return not self.batch_errors


def validate_customer_schema(
    headers: Sequence[str] | None,
    source_file: str = "<unknown>",
) -> tuple[BatchError, ...]:
    """Check customer headers and report schema failures as batch errors."""
    return _validate_schema(
        headers,
        dataset_name="customers",
        source_file=source_file,
        required_columns=CUSTOMER_REQUIRED_COLUMNS,
        optional_columns=CUSTOMER_OPTIONAL_COLUMNS,
    )


def validate_order_schema(
    headers: Sequence[str] | None,
    source_file: str = "<unknown>",
) -> tuple[BatchError, ...]:
    """Check order headers and report schema failures as batch errors."""
    return _validate_schema(
        headers,
        dataset_name="orders",
        source_file=source_file,
        required_columns=ORDER_REQUIRED_COLUMNS,
        optional_columns=(),
    )


def _validate_schema(
    headers: Sequence[str] | None,
    *,
    dataset_name: DatasetName,
    source_file: str,
    required_columns: Sequence[str],
    optional_columns: Sequence[str],
) -> tuple[BatchError, ...]:
    if headers is None:
        return (
            BatchError(
                dataset_name=dataset_name,
                source_file=source_file,
                stage="schema_check",
                error_code="missing_header",
                error_message="CSV file has no header row.",
            ),
        )

    errors: list[BatchError] = []
    header_list = list(headers)
    duplicates = sorted(
        {header for header in header_list if header_list.count(header) > 1}
    )
    if duplicates:
        errors.append(
            BatchError(
                dataset_name=dataset_name,
                source_file=source_file,
                stage="schema_check",
                error_code="duplicate_columns",
                error_message=(
                    "Header contains duplicate column names: "
                    + ", ".join(duplicates)
                    + "."
                ),
            )
        )

    header_set = set(header_list)
    missing = sorted(set(required_columns) - header_set)
    if missing:
        errors.append(
            BatchError(
                dataset_name=dataset_name,
                source_file=source_file,
                stage="schema_check",
                error_code="missing_required_columns",
                error_message=(
                    "Missing required columns: " + ", ".join(missing) + "."
                ),
            )
        )

    allowed = set(required_columns) | set(optional_columns)
    unexpected = sorted(header_set - allowed)
    if unexpected:
        errors.append(
            BatchError(
                dataset_name=dataset_name,
                source_file=source_file,
                stage="schema_check",
                error_code="unexpected_columns",
                error_message=(
                    "Unexpected columns: " + ", ".join(unexpected) + "."
                ),
            )
        )

    return tuple(errors)


def validate_customers(extraction: ExtractionResult) -> ValidationResult:
    """Validate customer records without changing any source values."""
    if extraction.dataset_name != "customers":
        return _dataset_mismatch_result(extraction, "customers")
    if extraction.batch_error is not None:
        return _batch_failure_result(extraction)

    schema_errors = validate_customer_schema(
        extraction.headers,
        extraction.source_file,
    )
    if schema_errors:
        return _schema_failure_result(
            "customers",
            extraction.headers,
            schema_errors,
        )

    assert extraction.headers is not None
    valid_candidates: list[tuple[ExtractedRecord, Mapping[str, str]]] = []
    row_rejections = [
        _malformed_row_rejection(row) for row in extraction.malformed_rows
    ]

    for record in extraction.records:
        row, shape_issue = _record_mapping(record, extraction.headers)
        if shape_issue is not None:
            row_rejections.append(_record_rejection(record, (shape_issue,), None))
            continue

        issues = _customer_issues(row)
        if issues:
            row_rejections.append(
                _record_rejection(record, issues, row.get("customer_id"))
            )
            continue
        valid_candidates.append((record, row))

    valid_records, duplicate_rejections = _resolve_duplicates(
        valid_candidates,
        key_column="customer_id",
        signature_columns=(
            "first_name",
            "last_name",
            "email",
            "phone",
            "signup_date",
            "country_code",
            "status",
        ),
    )
    row_rejections.extend(duplicate_rejections)
    return ValidationResult(
        dataset_name="customers",
        headers=extraction.headers,
        valid_records=valid_records,
        row_rejections=tuple(
            sorted(row_rejections, key=_rejection_sort_key)
        ),
        batch_errors=(),
    )


def validate_orders(
    extraction: ExtractionResult,
    known_customers: Mapping[str, str],
) -> ValidationResult:
    """Validate orders against customer IDs and signup dates supplied by caller."""
    if extraction.dataset_name != "orders":
        return _dataset_mismatch_result(extraction, "orders")
    if extraction.batch_error is not None:
        return _batch_failure_result(extraction)

    schema_errors = validate_order_schema(
        extraction.headers,
        extraction.source_file,
    )
    if schema_errors:
        return _schema_failure_result(
            "orders",
            extraction.headers,
            schema_errors,
        )

    assert extraction.headers is not None
    normalized_customers, ambiguous_customer_ids = _index_customers(
        known_customers
    )
    valid_candidates: list[tuple[ExtractedRecord, Mapping[str, str]]] = []
    row_rejections = [
        _malformed_row_rejection(row) for row in extraction.malformed_rows
    ]

    for record in extraction.records:
        row, shape_issue = _record_mapping(record, extraction.headers)
        if shape_issue is not None:
            row_rejections.append(_record_rejection(record, (shape_issue,), None))
            continue

        issues = _order_issues(
            row,
            normalized_customers,
            ambiguous_customer_ids,
        )
        if issues:
            row_rejections.append(
                _record_rejection(record, issues, row.get("order_id"))
            )
            continue
        valid_candidates.append((record, row))

    valid_records, duplicate_rejections = _resolve_duplicates(
        valid_candidates,
        key_column="order_id",
        signature_columns=(
            "customer_id",
            "order_ts",
            "total_amount_cents",
            "currency_code",
            "status",
        ),
    )
    row_rejections.extend(duplicate_rejections)
    return ValidationResult(
        dataset_name="orders",
        headers=extraction.headers,
        valid_records=valid_records,
        row_rejections=tuple(
            sorted(row_rejections, key=_rejection_sort_key)
        ),
        batch_errors=(),
    )


def build_customer_reference_map(
    validated_customers: ValidationResult,
) -> Mapping[str, str]:
    """Build an immutable, trimmed-ID to signup-date map for order validation."""
    if validated_customers.dataset_name != "customers":
        raise ValueError("validated_customers must be a customer result")
    if validated_customers.batch_errors:
        raise ValueError("Cannot build references from a schema-invalid batch")

    references: dict[str, str] = {}
    if validated_customers.headers is None:
        raise ValueError("validated_customers has no source header")
    for record in validated_customers.valid_records:
        values = dict(zip(validated_customers.headers, record.raw_values))
        customer_id = values["customer_id"].strip()
        references[customer_id] = values["signup_date"]
    return MappingProxyType(references)


def _record_mapping(
    record: ExtractedRecord,
    headers: Sequence[str],
) -> tuple[Mapping[str, str], ValidationIssue | None]:
    if len(record.raw_values) != len(headers):
        return {}, ValidationIssue(
            reason_code="record_field_count_mismatch",
            reason_detail=(
                f"Expected {len(headers)} values but found "
                f"{len(record.raw_values)}."
            ),
        )
    return dict(zip(headers, record.raw_values)), None


def _customer_issues(row: Mapping[str, str]) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    customer_id = row["customer_id"]
    first_name = row["first_name"]
    last_name = row["last_name"]
    email = row["email"]
    signup_date = row["signup_date"]
    country_code = row["country_code"]
    status = row["status"]

    if not customer_id.strip():
        issues.append(
            ValidationIssue(
                "missing_customer_id",
                "customer_id must not be empty or whitespace-only.",
                "customer_id",
            )
        )

    for field_name, value in (
        ("first_name", first_name),
        ("last_name", last_name),
    ):
        if not value.strip():
            issues.append(
                ValidationIssue(
                    "missing_required_field",
                    f"{field_name} must not be empty or whitespace-only.",
                    field_name,
                )
            )
        elif not 1 <= len(value.strip()) <= 100:
            issues.append(
                ValidationIssue(
                    "invalid_name_length",
                    f"{field_name} must contain 1 to 100 non-padding characters.",
                    field_name,
                )
            )

    checked_email = email.strip()
    if not checked_email:
        issues.append(
            ValidationIssue(
                "missing_required_field",
                "email must not be empty or whitespace-only.",
                "email",
            )
        )
    elif (
        len(checked_email) > 254
        or _EMAIL_PATTERN.fullmatch(checked_email) is None
    ):
        issues.append(
            ValidationIssue(
                "invalid_email_format",
                "email must have a plausible local-part and dotted domain.",
                "email",
            )
        )

    parsed_signup_date: date | None = None
    try:
        parsed_signup_date = date.fromisoformat(signup_date)
        if parsed_signup_date.isoformat() != signup_date:
            parsed_signup_date = None
    except ValueError:
        pass
    if parsed_signup_date is None:
        issues.append(
            ValidationIssue(
                "invalid_signup_date",
                "signup_date must be a real calendar date in YYYY-MM-DD form.",
                "signup_date",
            )
        )

    checked_country_code = country_code.strip().upper()
    if checked_country_code not in ISO_3166_1_ALPHA_2_CODES:
        issues.append(
            ValidationIssue(
                "unsupported_country_code",
                "country_code must be an assigned ISO 3166-1 alpha-2 code.",
                "country_code",
            )
        )

    if status not in CUSTOMER_STATUSES:
        issues.append(
            ValidationIssue(
                "invalid_customer_status",
                "status must be 'active' or 'inactive'.",
                "status",
            )
        )

    return tuple(issues)


def _order_issues(
    row: Mapping[str, str],
    known_customers: Mapping[str, str],
    ambiguous_customer_ids: frozenset[str],
) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    order_id = row["order_id"]
    customer_id = row["customer_id"]
    order_ts = row["order_ts"]
    amount = row["total_amount_cents"]
    currency_code = row["currency_code"]
    status = row["status"]

    if not order_id.strip():
        issues.append(
            ValidationIssue(
                "missing_order_id",
                "order_id must not be empty or whitespace-only.",
                "order_id",
            )
        )

    if not customer_id.strip():
        issues.append(
            ValidationIssue(
                "missing_customer_id",
                "customer_id must not be empty or whitespace-only.",
                "customer_id",
            )
        )
        customer_signup_date = None
    else:
        normalized_customer_id = customer_id.strip()
        if normalized_customer_id in ambiguous_customer_ids:
            issues.append(
                ValidationIssue(
                    "ambiguous_customer_reference",
                    "customer_id matches conflicting supplied customer references.",
                    "customer_id",
                )
            )
            customer_signup_date = None
        else:
            customer_signup_date = known_customers.get(normalized_customer_id)
            if customer_signup_date is None:
                issues.append(
                    ValidationIssue(
                        "invalid_customer_reference",
                        "customer_id does not identify a known customer.",
                        "customer_id",
                    )
                )

    parsed_order_ts: datetime | None = None
    if _ISO_TIMESTAMP_PATTERN.fullmatch(order_ts) is not None:
        try:
            parsed_order_ts = datetime.fromisoformat(
                order_ts[:-1] + "+00:00" if order_ts.endswith("Z") else order_ts
            )
        except ValueError:
            parsed_order_ts = None
    if (
        parsed_order_ts is None
        or parsed_order_ts.tzinfo is None
        or parsed_order_ts.utcoffset() is None
    ):
        issues.append(
            ValidationIssue(
                "invalid_order_timestamp",
                "order_ts must be a valid timezone-aware ISO-8601 timestamp.",
                "order_ts",
            )
        )

    if not _INTEGER_PATTERN.fullmatch(amount):
        issues.append(
            ValidationIssue(
                "invalid_order_amount",
                "total_amount_cents must be an integer written with digits only.",
                "total_amount_cents",
            )
        )
    elif int(amount) <= 0:
        issues.append(
            ValidationIssue(
                "non_positive_order_amount",
                "total_amount_cents must be greater than zero.",
                "total_amount_cents",
            )
        )

    if currency_code != "USD":
        issues.append(
            ValidationIssue(
                "unsupported_currency",
                "currency_code must be 'USD' for this sample pipeline.",
                "currency_code",
            )
        )

    if status not in ORDER_STATUSES:
        issues.append(
            ValidationIssue(
                "invalid_order_status",
                "status must be one of the approved order statuses.",
                "status",
            )
        )

    if (
        parsed_order_ts is not None
        and parsed_order_ts.tzinfo is not None
        and parsed_order_ts.utcoffset() is not None
        and customer_signup_date is not None
    ):
        try:
            signup = date.fromisoformat(customer_signup_date)
            if signup.isoformat() != customer_signup_date:
                raise ValueError("signup date is not canonical ISO format")
        except ValueError:
            issues.append(
                ValidationIssue(
                    "invalid_customer_signup_date",
                    "The known customer's signup date is not a valid ISO date.",
                    "customer_id",
                )
            )
        else:
            order_utc_date = parsed_order_ts.astimezone(timezone.utc).date()
            if order_utc_date < signup:
                issues.append(
                    ValidationIssue(
                        "order_before_customer_signup",
                        "Order UTC date must not precede the customer's signup date.",
                        "order_ts",
                    )
                )

    return tuple(issues)


def _index_customers(
    known_customers: Mapping[str, str],
) -> tuple[Mapping[str, str], frozenset[str]]:
    indexed: dict[str, str] = {}
    ambiguous: set[str] = set()
    for customer_id, signup_date in known_customers.items():
        normalized_id = customer_id.strip()
        if not normalized_id:
            continue
        if normalized_id in indexed and indexed[normalized_id] != signup_date:
            ambiguous.add(normalized_id)
        else:
            indexed[normalized_id] = signup_date
    return MappingProxyType(indexed), frozenset(ambiguous)


def _resolve_duplicates(
    candidates: Iterable[tuple[ExtractedRecord, Mapping[str, str]]],
    *,
    key_column: str,
    signature_columns: Sequence[str],
) -> tuple[tuple[ExtractedRecord, ...], tuple[RowRejection, ...]]:
    groups: dict[str, list[tuple[ExtractedRecord, Mapping[str, str]]]] = (
        defaultdict(list)
    )
    for record, row in candidates:
        groups[row[key_column].strip()].append((record, row))

    accepted: list[ExtractedRecord] = []
    rejected: list[RowRejection] = []
    for normalized_key, occurrences in groups.items():
        if len(occurrences) == 1:
            accepted.append(occurrences[0][0])
            continue

        signatures = {
            tuple(
                normalized_key if column == key_column else row.get(column, "")
                for column in (key_column, *signature_columns)
            )
            for _, row in occurrences
        }
        if len(signatures) == 1:
            accepted.append(occurrences[0][0])
            duplicates = occurrences[1:]
            for record, row in duplicates:
                rejected.append(
                    _record_rejection(
                        record,
                        (
                            ValidationIssue(
                                "duplicate_record",
                                f"Duplicate {key_column} matches an earlier valid row.",
                                key_column,
                            ),
                        ),
                        row[key_column],
                    )
                )
        else:
            for record, row in occurrences:
                rejected.append(
                    _record_rejection(
                        record,
                        (
                            ValidationIssue(
                                "conflicting_duplicate_key",
                                f"Rows sharing {key_column} have conflicting values.",
                                key_column,
                            ),
                        ),
                        row[key_column],
                    )
                )

    accepted.sort(key=lambda record: record.source_row_number)
    return tuple(accepted), tuple(rejected)


def _record_rejection(
    record: ExtractedRecord,
    issues: tuple[ValidationIssue, ...],
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
        issues=issues,
    )


def _malformed_row_rejection(
    row: MalformedCsvRecord,
) -> RowRejection:
    return RowRejection(
        dataset_name=row.dataset_name,
        source_file=row.source_file,
        source_row_number=row.source_row_number,
        physical_line_start=row.physical_line_start,
        physical_line_end=row.physical_line_end,
        record_key=None,
        raw_record=row.raw_record,
        issues=(
            ValidationIssue(
                reason_code=row.reason_code,
                reason_detail=row.reason_detail,
            ),
        ),
    )


def _rejection_sort_key(rejection: RowRejection) -> tuple[int, str]:
    return (
        rejection.source_row_number
        if rejection.source_row_number is not None
        else 0,
        rejection.source_file,
    )


def _dataset_mismatch_result(
    extraction: ExtractionResult,
    expected_dataset: DatasetName,
) -> ValidationResult:
    error = BatchError(
        dataset_name=extraction.dataset_name,
        source_file=extraction.source_file,
        stage="schema_check",
        error_code="dataset_mismatch",
        error_message=(
            f"Expected {expected_dataset} input but received "
            f"{extraction.dataset_name} input."
        ),
    )
    return ValidationResult(
        dataset_name=expected_dataset,
        headers=extraction.headers,
        valid_records=(),
        row_rejections=(),
        batch_errors=(error,),
    )


def _batch_failure_result(extraction: ExtractionResult) -> ValidationResult:
    assert extraction.batch_error is not None
    return ValidationResult(
        dataset_name=extraction.dataset_name,
        headers=extraction.headers,
        valid_records=(),
        row_rejections=(),
        batch_errors=(extraction.batch_error,),
    )


def _schema_failure_result(
    dataset_name: DatasetName,
    headers: tuple[str, ...] | None,
    errors: tuple[BatchError, ...],
) -> ValidationResult:
    return ValidationResult(
        dataset_name=dataset_name,
        headers=headers,
        valid_records=(),
        row_rejections=(),
        batch_errors=errors,
    )
