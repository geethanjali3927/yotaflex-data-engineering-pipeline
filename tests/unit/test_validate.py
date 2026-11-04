from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from pipeline.extract import extract_csv
from pipeline.validate import (
    build_customer_reference_map,
    validate_customer_schema,
    validate_customers,
    validate_order_schema,
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


def _write_csv(tmp_path, filename, headers, rows, *, newline="\n"):
    path = tmp_path / filename
    content = ",".join(headers) + newline
    content += "".join(",".join(row) + newline for row in rows)
    path.write_text(content, encoding="utf-8", newline="")
    return path


def _valid_customer(
    customer_id="C-1",
    *,
    first_name="Ada",
    last_name="Lovelace",
    email="ada@example.com",
    signup_date="2026-01-01",
    country_code="US",
    status="active",
    phone="",
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


def _valid_order(
    order_id="O-1",
    *,
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


def _customer_result(tmp_path, rows, *, headers=CUSTOMER_HEADERS):
    path = _write_csv(tmp_path, "customers.csv", headers, rows)
    return validate_customers(extract_csv(path, "customers"))


def _order_result(tmp_path, rows, *, headers=ORDER_HEADERS, customers=None):
    path = _write_csv(tmp_path, "orders.csv", headers, rows)
    return validate_orders(
        extract_csv(path, "orders"),
        customers if customers is not None else {"C-1": "2026-01-01"},
    )


def _reason_codes(rejection):
    return {issue.reason_code for issue in rejection.issues}


def test_customer_and_order_schemas_accept_exact_columns():
    assert validate_customer_schema(CUSTOMER_HEADERS) == ()
    assert validate_customer_schema(CUSTOMER_HEADERS[:-1]) == ()
    assert validate_order_schema(ORDER_HEADERS) == ()


@pytest.mark.parametrize(
    ("validator", "headers", "expected_code"),
    [
        (validate_customer_schema, CUSTOMER_HEADERS[1:], "missing_required_columns"),
        (
            validate_order_schema,
            ORDER_HEADERS[:-1],
            "missing_required_columns",
        ),
        (
            validate_customer_schema,
            (*CUSTOMER_HEADERS, "unexpected"),
            "unexpected_columns",
        ),
        (
            validate_order_schema,
            (*ORDER_HEADERS, "unexpected"),
            "unexpected_columns",
        ),
        (
            validate_customer_schema,
            (*CUSTOMER_HEADERS, "email"),
            "duplicate_columns",
        ),
        (
            validate_order_schema,
            (*ORDER_HEADERS, "status"),
            "duplicate_columns",
        ),
    ],
)
def test_schema_validation_reports_missing_unexpected_and_duplicate_columns(
    validator,
    headers,
    expected_code,
):
    errors = validator(headers, "source.csv")

    assert expected_code in {error.error_code for error in errors}
    assert all(error.stage == "schema_check" for error in errors)
    assert all(error.source_file == "source.csv" for error in errors)


def test_schema_errors_are_batch_errors_not_row_rejections(tmp_path):
    result = _customer_result(
        tmp_path,
        [_valid_customer()],
        headers=("customer_id", "first_name"),
    )

    assert not result.complete
    assert result.valid_records == ()
    assert result.row_rejections == ()
    assert "missing_required_columns" in {
        error.error_code for error in result.batch_errors
    }


def test_valid_customer_accepts_outer_email_whitespace_and_uppercase(tmp_path):
    row = _valid_customer(
        customer_id=" C-1 ",
        email=" Ada.Lovelace@Example.COM ",
        country_code="us",
        phone="+001234",
    )

    result = _customer_result(tmp_path, [row])

    assert result.complete
    assert len(result.valid_records) == 1
    assert result.valid_records[0].raw_values == row
    assert result.row_rejections == ()


@pytest.mark.parametrize(
    ("row", "expected_code"),
    [
        (_valid_customer(customer_id="   "), "missing_customer_id"),
        (_valid_customer(first_name=""), "missing_required_field"),
        (_valid_customer(last_name=" " * 3), "missing_required_field"),
        (_valid_customer(first_name="x" * 101), "invalid_name_length"),
        (_valid_customer(last_name="y" * 101), "invalid_name_length"),
        (_valid_customer(email="  "), "missing_required_field"),
        (_valid_customer(email="not-an-email"), "invalid_email_format"),
        (_valid_customer(email="a..b@example.com"), "invalid_email_format"),
        (_valid_customer(signup_date="2026-02-30"), "invalid_signup_date"),
        (_valid_customer(signup_date="2026-1-01"), "invalid_signup_date"),
        (_valid_customer(country_code="ZZ"), "unsupported_country_code"),
        (_valid_customer(status="pending"), "invalid_customer_status"),
    ],
)
def test_invalid_customer_fields_are_row_rejections(
    tmp_path,
    row,
    expected_code,
):
    result = _customer_result(tmp_path, [row])

    assert result.complete
    assert result.valid_records == ()
    assert len(result.row_rejections) == 1
    rejection = result.row_rejections[0]
    assert expected_code in _reason_codes(rejection)
    assert rejection.source_file == "customers.csv"
    assert rejection.source_row_number == 1
    assert rejection.physical_line_start == 2
    assert rejection.raw_record
    assert rejection.reason_detail


def test_optional_phone_preserves_plus_and_leading_zeroes(tmp_path):
    row = _valid_customer(phone="+000123456")

    result = _customer_result(tmp_path, [row])

    assert result.valid_records[0].raw_values[-1] == "+000123456"


@pytest.mark.parametrize(
    ("row", "expected_code"),
    [
        (_valid_order(order_id=" "), "missing_order_id"),
        (_valid_order(customer_id="unknown"), "invalid_customer_reference"),
        (_valid_order(order_ts="2026-01-02T12:00:00"), "invalid_order_timestamp"),
        (_valid_order(total_amount_cents="1.5"), "invalid_order_amount"),
        (_valid_order(total_amount_cents="0"), "non_positive_order_amount"),
        (_valid_order(total_amount_cents="-1"), "invalid_order_amount"),
        (_valid_order(currency_code="EUR"), "unsupported_currency"),
        (_valid_order(status="processing"), "invalid_order_status"),
    ],
)
def test_invalid_order_fields_are_row_rejections(
    tmp_path,
    row,
    expected_code,
):
    result = _order_result(tmp_path, [row])

    assert result.complete
    assert result.valid_records == ()
    assert len(result.row_rejections) == 1
    rejection = result.row_rejections[0]
    assert expected_code in _reason_codes(rejection)
    assert rejection.source_file == "orders.csv"
    assert rejection.source_row_number == 1
    assert rejection.reason_detail


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-11-04T10:30+05:30",
        "2026-11-04T10:30:45+05:30",
        "2026-11-04T10:30:45.123Z",
    ],
)
def test_accepts_timezone_aware_iso_timestamps(tmp_path, timestamp):
    result = _order_result(tmp_path, [_valid_order(order_ts=timestamp)])

    assert len(result.valid_records) == 1
    assert result.valid_records[0].raw_values[2] == timestamp


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-11-04T10:30",
        "2026-11-04T10:30:45",
        "2026-02-30T10:30Z",
        "2026-11-04T25:30Z",
        "2026-11-04T10:30+05:60",
        "2026-11-04T10:30+14:01",
        "2026-11-04T10:30+24:00",
    ],
)
def test_rejects_timezone_less_invalid_date_time_or_offset(tmp_path, timestamp):
    result = _order_result(tmp_path, [_valid_order(order_ts=timestamp)])

    assert result.valid_records == ()
    assert "invalid_order_timestamp" in _reason_codes(result.row_rejections[0])


def test_identical_customer_duplicates_keep_first_and_reject_later(tmp_path):
    first = _valid_customer(customer_id="C-1")
    duplicate = _valid_customer(customer_id=" C-1 ")
    result = _customer_result(tmp_path, [first, duplicate])

    assert [record.source_row_number for record in result.valid_records] == [1]
    assert len(result.row_rejections) == 1
    rejection = result.row_rejections[0]
    assert rejection.reason_code == "duplicate_record"
    assert rejection.source_row_number == 2
    assert rejection.record_key == " C-1 "


def test_conflicting_customer_duplicates_reject_every_occurrence(tmp_path):
    first = _valid_customer(customer_id="C-1", email="first@example.com")
    conflicting = _valid_customer(
        customer_id=" C-1 ",
        email="second@example.com",
    )
    result = _customer_result(tmp_path, [first, conflicting])

    assert result.valid_records == ()
    assert [rejection.source_row_number for rejection in result.row_rejections] == [
        1,
        2,
    ]
    assert all(
        rejection.reason_code == "conflicting_duplicate_key"
        for rejection in result.row_rejections
    )
    assert [rejection.record_key for rejection in result.row_rejections] == [
        "C-1",
        " C-1 ",
    ]


def test_identical_order_duplicates_keep_first_and_reject_later(tmp_path):
    first = _valid_order(order_id="O-1")
    duplicate = _valid_order(order_id=" O-1 ")
    result = _order_result(tmp_path, [first, duplicate])

    assert [record.source_row_number for record in result.valid_records] == [1]
    assert len(result.row_rejections) == 1
    assert result.row_rejections[0].reason_code == "duplicate_record"
    assert result.row_rejections[0].source_row_number == 2


def test_conflicting_order_duplicates_reject_every_occurrence(tmp_path):
    first = _valid_order(order_id="O-1", total_amount_cents="100")
    conflicting = _valid_order(order_id=" O-1 ", total_amount_cents="200")
    result = _order_result(tmp_path, [first, conflicting])

    assert result.valid_records == ()
    assert [rejection.source_row_number for rejection in result.row_rejections] == [
        1,
        2,
    ]
    assert all(
        rejection.reason_code == "conflicting_duplicate_key"
        for rejection in result.row_rejections
    )


def test_customer_reference_map_uses_trimmed_ids(tmp_path):
    result = _customer_result(
        tmp_path,
        [_valid_customer(customer_id=" C-1 ", signup_date="2026-01-01")],
    )

    references = build_customer_reference_map(result)

    assert references == {"C-1": "2026-01-01"}


def test_order_before_customer_signup_uses_utc_calendar_date(tmp_path):
    result = _order_result(
        tmp_path,
        [
            _valid_order(
                order_ts="2026-01-01T00:30:00+02:00",
            )
        ],
        customers={"C-1": "2026-01-01"},
    )

    assert result.valid_records == ()
    assert "order_before_customer_signup" in _reason_codes(
        result.row_rejections[0]
    )


def test_malformed_csv_row_becomes_row_rejection(tmp_path):
    path = tmp_path / "customers.csv"
    path.write_text(
        "customer_id,first_name,last_name,email,signup_date,country_code,status,phone\n"
        "C-1,Ada,Lovelace,ada@example.com,2026-01-01,US,active,\n"
        "C-2,Grace\n",
        encoding="utf-8",
        newline="",
    )

    result = validate_customers(extract_csv(path, "customers"))

    assert result.complete
    assert len(result.valid_records) == 1
    assert len(result.row_rejections) == 1
    rejection = result.row_rejections[0]
    assert rejection.reason_code == "field_count_mismatch"
    assert rejection.source_row_number == 2
    assert rejection.raw_record == "C-2,Grace\n"


def test_fatal_extraction_error_remains_a_batch_error(tmp_path):
    path = tmp_path / "malformed.csv"
    path.write_text(
        'customer_id,first_name,last_name,email,signup_date,country_code,status,phone\n'
        'C-1,"unterminated',
        encoding="utf-8",
        newline="",
    )

    result = validate_customers(extract_csv(path, "customers"))

    assert not result.complete
    assert result.valid_records == ()
    assert result.row_rejections == ()
    assert len(result.batch_errors) == 1
    assert result.batch_errors[0].error_code == "malformed_csv_syntax"


def test_validation_keeps_source_records_unchanged(tmp_path):
    customer = _valid_customer(
        customer_id=" C-1 ",
        email=" Ada@Example.COM ",
        country_code="us",
        phone="+0001",
    )
    result = _customer_result(tmp_path, [customer])

    assert result.valid_records[0].raw_values == customer
    assert result.valid_records[0].raw_record.startswith(" C-1 ,")
    assert result.valid_records[0].raw_record.endswith(",+0001\n")


def test_validation_results_and_rejections_are_immutable(tmp_path):
    result = _customer_result(tmp_path, [_valid_customer()])

    with pytest.raises(FrozenInstanceError):
        result.dataset_name = "orders"

    rejected_result = _customer_result(
        tmp_path,
        [_valid_customer(customer_id=" ")],
    )
    rejection = rejected_result.row_rejections[0]
    with pytest.raises(FrozenInstanceError):
        rejection.record_key = "changed"
