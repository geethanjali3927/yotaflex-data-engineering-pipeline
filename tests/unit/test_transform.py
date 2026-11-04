from dataclasses import FrozenInstanceError
import csv

import pytest

from pipeline.extract import extract_csv
from pipeline.transform import (
    SQLITE_INTEGER_MAX,
    TransformedCustomer,
    TransformedOrder,
    transform_customers,
    transform_orders,
)
from pipeline.validate import validate_customers, validate_orders


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
CUSTOMER_HEADERS_WITHOUT_PHONE = CUSTOMER_HEADERS[:-1]
ORDER_HEADERS = (
    "order_id",
    "customer_id",
    "order_ts",
    "total_amount_cents",
    "currency_code",
    "status",
)


def _write_csv(tmp_path, filename, headers, rows):
    path = tmp_path / filename
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
    return path


def _customer_row(
    customer_id="C-1",
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


def _order_row(
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


def _validated_customers(tmp_path, rows, headers=CUSTOMER_HEADERS):
    path = _write_csv(tmp_path, "customers.csv", headers, rows)
    return validate_customers(extract_csv(path, "customers"))


def _validated_orders(tmp_path, rows):
    path = _write_csv(tmp_path, "orders.csv", ORDER_HEADERS, rows)
    return validate_orders(
        extract_csv(path, "orders"),
        {"C-1": "2026-01-01"},
    )


def test_customer_fields_are_normalized_and_source_is_preserved(tmp_path):
    row = _customer_row(
        customer_id=" AbC-1 ",
        first_name=" Ada ",
        last_name=" Lovelace  ",
        email="  ADA.Lovelace@Example.COM ",
        country_code=" us ",
        phone="+000123",
    )
    validation = _validated_customers(tmp_path, [row])
    source_record = validation.valid_records[0]

    result = transform_customers(validation)

    assert result.complete
    assert result.row_rejections == ()
    assert len(result.transformed_records) == 1
    customer = result.transformed_records[0]
    assert isinstance(customer, TransformedCustomer)
    assert customer.customer_id == "AbC-1"
    assert customer.first_name == "Ada"
    assert customer.last_name == "Lovelace"
    assert customer.email == "ada.lovelace@example.com"
    assert customer.country_code == "US"
    assert customer.phone == "+000123"
    assert customer.signup_date == "2026-01-01"
    assert customer.status == "active"
    assert customer.source_record is source_record
    assert source_record.raw_values == row
    assert source_record.raw_record


@pytest.mark.parametrize("phone", ["00001234", "+0001234", "+001-0203"])
def test_phone_is_preserved_as_text(tmp_path, phone):
    result = transform_customers(
        _validated_customers(tmp_path, [_customer_row(phone=phone)])
    )

    customer = result.transformed_records[0]
    assert customer.phone == phone
    assert isinstance(customer.phone, str)


def test_missing_phone_column_maps_to_none(tmp_path):
    row = _customer_row()[:-1]
    result = transform_customers(
        _validated_customers(
            tmp_path,
            [row],
            headers=CUSTOMER_HEADERS_WITHOUT_PHONE,
        )
    )

    assert result.transformed_records[0].phone is None


def test_present_but_empty_phone_remains_empty_string(tmp_path):
    result = transform_customers(
        _validated_customers(tmp_path, [_customer_row(phone="")])
    )

    assert result.transformed_records[0].phone == ""


def test_order_ids_are_trimmed_and_case_is_preserved(tmp_path):
    result = transform_orders(
        _validated_orders(
            tmp_path,
            [_order_row(order_id=" Ord-X ", customer_id=" C-1 ")],
        )
    )

    order = result.transformed_records[0]
    assert isinstance(order, TransformedOrder)
    assert order.order_id == "Ord-X"
    assert order.customer_id == "C-1"


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2026-11-04T10:30Z", "2026-11-04T10:30:00+00:00"),
        ("2026-11-04T10:30+05:30", "2026-11-04T05:00:00+00:00"),
        ("2026-11-04T10:30-04:00", "2026-11-04T14:30:00+00:00"),
        (
            "2026-11-04T10:30:45+05:30",
            "2026-11-04T05:00:45+00:00",
        ),
    ],
)
def test_order_timestamp_is_normalized_to_utc(tmp_path, timestamp, expected):
    result = transform_orders(
        _validated_orders(tmp_path, [_order_row(order_ts=timestamp)])
    )

    assert result.transformed_records[0].order_ts == expected


@pytest.mark.parametrize(
    ("fraction", "expected_microseconds"),
    [
        ("1", "100000"),
        ("12", "120000"),
        ("123", "123000"),
        ("1234", "123400"),
        ("12345", "123450"),
        ("123456", "123456"),
    ],
)
def test_fractional_seconds_are_preserved_to_six_digit_precision(
    tmp_path,
    fraction,
    expected_microseconds,
):
    timestamp = f"2026-11-04T10:30:45.{fraction}Z"
    result = transform_orders(
        _validated_orders(tmp_path, [_order_row(order_ts=timestamp)])
    )

    assert result.transformed_records[0].order_ts == (
        f"2026-11-04T10:30:45.{expected_microseconds}+00:00"
    )


def test_more_than_six_fractional_digits_are_rejected_with_traceability(tmp_path):
    timestamp = "2026-11-04T10:30:45.1234567Z"
    result = transform_orders(
        _validated_orders(
            tmp_path,
            [_order_row(order_id=" O-7 ", order_ts=timestamp)],
        )
    )

    assert result.transformed_records == ()
    assert len(result.row_rejections) == 1
    rejection = result.row_rejections[0]
    assert rejection.reason_code == "transformation_invalid_timestamp"
    assert rejection.source_file == "orders.csv"
    assert rejection.source_row_number == 1
    assert rejection.record_key == " O-7 "
    assert rejection.raw_record


def test_amount_remains_a_python_integer(tmp_path):
    result = transform_orders(
        _validated_orders(tmp_path, [_order_row(total_amount_cents="1200")])
    )

    amount = result.transformed_records[0].total_amount_cents
    assert amount == 1200
    assert type(amount) is int
    assert not isinstance(amount, float)


def test_sqlite_maximum_integer_is_supported(tmp_path):
    result = transform_orders(
        _validated_orders(
            tmp_path,
            [_order_row(total_amount_cents=str(SQLITE_INTEGER_MAX))],
        )
    )

    assert result.transformed_records[0].total_amount_cents == SQLITE_INTEGER_MAX
    assert type(result.transformed_records[0].total_amount_cents) is int
    assert result.row_rejections == ()


def test_amount_above_sqlite_maximum_is_rejected(tmp_path):
    result = transform_orders(
        _validated_orders(
            tmp_path,
            [_order_row(total_amount_cents=str(SQLITE_INTEGER_MAX + 1))],
        )
    )

    assert result.transformed_records == ()
    assert len(result.row_rejections) == 1
    assert (
        result.row_rejections[0].reason_code
        == "transformation_amount_out_of_range"
    )


def test_existing_validation_rejections_are_preserved(tmp_path):
    invalid_row = _customer_row(email="not-an-email")
    validation = _validated_customers(tmp_path, [invalid_row])

    result = transform_customers(validation)

    assert result.transformed_records == ()
    assert result.row_rejections == validation.row_rejections
    assert result.row_rejections[0] is validation.row_rejections[0]


def test_batch_errors_produce_no_transformed_records(tmp_path):
    path = _write_csv(tmp_path, "customers.csv", ("customer_id",), [("C-1",)])
    validation = validate_customers(extract_csv(path, "customers"))

    result = transform_customers(validation)

    assert result.transformed_records == ()
    assert result.batch_errors == validation.batch_errors
    assert result.batch_errors
    assert result.row_rejections == validation.row_rejections


def test_transformation_rejection_preserves_full_source_traceability(tmp_path):
    path = _write_csv(
        tmp_path,
        "orders.csv",
        ORDER_HEADERS,
        [
            _order_row(order_id=" O-9 ", total_amount_cents="1"),
            _order_row(
                order_id=" O-10 ",
                order_ts="2026-11-04T10:30:45.1234567Z",
            ),
        ],
    )
    validation = validate_orders(
        extract_csv(path, "orders"),
        {"C-1": "2026-01-01"},
    )
    source_record = validation.valid_records[1]

    result = transform_orders(validation)

    rejection = next(
        item
        for item in result.row_rejections
        if item.reason_code == "transformation_invalid_timestamp"
    )
    assert rejection.source_file == source_record.source_file == "orders.csv"
    assert rejection.source_row_number == source_record.source_row_number == 2
    assert rejection.record_key == " O-10 "
    assert rejection.raw_record == source_record.raw_record
    assert "2026-11-04T10:30:45.1234567Z" in rejection.raw_record


def test_original_customer_and_order_records_remain_unchanged(tmp_path):
    customer_validation = _validated_customers(
        tmp_path,
        [_customer_row(customer_id=" C-1 ", email=" ADA@Example.com ")],
    )
    customer_source = customer_validation.valid_records[0]
    customer_values_before = customer_source.raw_values
    customer_raw_before = customer_source.raw_record

    order_validation = _validated_orders(
        tmp_path,
        [
            _order_row(
                order_id=" O-1 ",
                customer_id=" C-1 ",
                order_ts="2026-01-02T12:00-05:00",
            )
        ],
    )
    order_source = order_validation.valid_records[0]
    order_values_before = order_source.raw_values
    order_raw_before = order_source.raw_record

    transform_customers(customer_validation)
    transform_orders(order_validation)

    assert customer_source.raw_values == customer_values_before
    assert customer_source.raw_record == customer_raw_before
    assert order_source.raw_values == order_values_before
    assert order_source.raw_record == order_raw_before


def test_repeated_transformations_are_deterministic(tmp_path):
    customer_validation = _validated_customers(
        tmp_path,
        [_customer_row(customer_id=" C-1 ", email=" Ada@Example.com ")],
    )
    order_validation = _validated_orders(
        tmp_path,
        [_order_row(order_ts="2026-01-02T12:30+02:00")],
    )

    assert transform_customers(customer_validation) == transform_customers(
        customer_validation
    )
    assert transform_orders(order_validation) == transform_orders(order_validation)


def test_transformation_dataclasses_are_frozen(tmp_path):
    customer_result = transform_customers(
        _validated_customers(tmp_path, [_customer_row()])
    )
    order_result = transform_orders(
        _validated_orders(tmp_path, [_order_row()])
    )

    with pytest.raises(FrozenInstanceError):
        customer_result.transformed_records[0].email = "changed@example.com"
    with pytest.raises(FrozenInstanceError):
        order_result.transformed_records[0].order_id = "changed"
    with pytest.raises(FrozenInstanceError):
        customer_result.transformed_records = ()


def test_wrong_dataset_input_returns_batch_error_and_no_records(tmp_path):
    customer_validation = _validated_customers(
        tmp_path,
        [_customer_row()],
    )

    result = transform_orders(customer_validation)

    assert result.dataset_name == "orders"
    assert result.transformed_records == ()
    assert len(result.batch_errors) == 1
    assert (
        result.batch_errors[0].error_code
        == "transformation_dataset_mismatch"
    )
