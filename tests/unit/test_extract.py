import sqlite3

import pytest

import pipeline.extract as extract_module
from pipeline.extract import extract_csv, extract_required_csv_files


def test_extracts_valid_customers_csv(tmp_path):
    source = tmp_path / "customers.csv"
    source.write_text(
        "customer_id,first_name\nC-1,Ada\n",
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "customers")

    assert result.complete
    assert result.dataset_name == "customers"
    assert result.source_file == "customers.csv"
    assert result.headers == ("customer_id", "first_name")
    assert len(result.records) == 1
    assert result.records[0].raw_values == ("C-1", "Ada")
    assert result.records[0].source_row_number == 1
    assert result.records[0].raw_record == "C-1,Ada\n"


def test_extracts_valid_orders_csv_with_same_function(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id\nO-1,C-1\n",
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "orders")

    assert result.complete
    assert result.dataset_name == "orders"
    assert result.headers == ("order_id", "customer_id")
    assert result.records[0].raw_values == ("O-1", "C-1")


def test_preserves_quotes_commas_and_multiline_raw_records(tmp_path):
    source = tmp_path / "customers.csv"
    raw_quoted = 'C-1,"Doe, Jane"\r\n'
    raw_multiline = 'C-2,"first line\r\nsecond line"\r\n'
    source.write_bytes(
        b"customer_id,name\r\n"
        + raw_quoted.encode("utf-8")
        + raw_multiline.encode("utf-8")
    )

    result = extract_csv(source, "customers")

    assert [record.raw_record for record in result.records] == [
        raw_quoted,
        raw_multiline,
    ]
    assert result.records[0].raw_values == ("C-1", "Doe, Jane")
    assert result.records[1].raw_values == ("C-2", "first line\r\nsecond line")


def test_tracks_logical_rows_independently_of_physical_lines(tmp_path):
    source = tmp_path / "customers.csv"
    source.write_text(
        'id,name\n1,"two\nphysical\nlines"\n2,one-line\n',
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "customers")

    assert [record.source_row_number for record in result.records] == [1, 2]
    assert [
        (record.physical_line_start, record.physical_line_end)
        for record in result.records
    ] == [(2, 4), (5, 5)]


@pytest.mark.parametrize(
    ("bad_row", "expected_fields"),
    [
        ("C-1\n", 1),
        ("C-1,Ada,extra\n", 3),
    ],
    ids=["too-few-fields", "too-many-fields"],
)
def test_detects_recoverable_field_count_mismatches(
    tmp_path,
    bad_row,
    expected_fields,
):
    source = tmp_path / "customers.csv"
    source.write_text(
        "customer_id,name\n" + bad_row,
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "customers")

    assert result.complete
    assert len(result.malformed_rows) == 1
    assert result.malformed_rows[0].reason_code == "field_count_mismatch"
    assert result.malformed_rows[0].expected_field_count == 2
    assert result.malformed_rows[0].actual_field_count == expected_fields
    assert result.malformed_rows[0].source_row_number == 1
    assert result.malformed_rows[0].raw_record == bad_row


def test_continues_extracting_valid_rows_after_malformed_rows(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id\n"
        "O-1,C-1\n"
        "O-2\n"
        "O-3,C-3,extra\n"
        "O-4,C-4\n",
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "orders")

    assert result.complete
    assert result.rows_read == 4
    assert [record.source_row_number for record in result.records] == [1, 4]
    assert [record.raw_values[0] for record in result.records] == ["O-1", "O-4"]
    assert [row.source_row_number for row in result.malformed_rows] == [2, 3]


def test_detects_missing_file_without_exposing_parent_path(tmp_path):
    source = tmp_path / "private-directory" / "customers.csv"

    result = extract_csv(source, "customers")

    assert not result.complete
    assert result.batch_error is not None
    assert result.batch_error.error_code == "missing_file"
    assert result.source_file == "customers.csv"
    assert str(tmp_path) not in result.batch_error.error_message


def test_detects_zero_byte_file(tmp_path):
    source = tmp_path / "empty.csv"
    source.write_bytes(b"")

    result = extract_csv(source, "orders")

    assert result.complete
    assert result.is_empty_file
    assert result.headers is None
    assert result.rows_read == 0
    assert result.records == ()


def test_handles_header_only_file(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("order_id,customer_id\n", encoding="utf-8", newline="")

    result = extract_csv(source, "orders")

    assert result.complete
    assert not result.is_empty_file
    assert result.headers == ("order_id", "customer_id")
    assert result.rows_read == 0
    assert result.records == ()


def test_detects_unrecoverable_csv_syntax(tmp_path):
    source = tmp_path / "broken.csv"
    raw_broken_record = 'C-1,"unterminated\n'
    source.write_text(
        "customer_id,name\n" + raw_broken_record,
        encoding="utf-8",
        newline="",
    )

    result = extract_csv(source, "customers")

    assert not result.complete
    assert result.batch_error is not None
    assert result.batch_error.error_code == "malformed_csv_syntax"
    assert result.batch_error.physical_line == 2
    assert result.batch_error.raw_record == raw_broken_record


def test_detects_invalid_utf8_encoding(tmp_path):
    source = tmp_path / "invalid-encoding.csv"
    source.write_bytes(b"order_id,customer_id\nO-1,\xff\n")

    result = extract_csv(source, "orders")

    assert not result.complete
    assert result.batch_error is not None
    assert result.batch_error.error_code == "encoding_error"
    assert result.batch_error.physical_line is None


def test_preflights_both_required_files_before_extracting_either(
    tmp_path,
    monkeypatch,
):
    customers = tmp_path / "customers.csv"
    customers.write_text("customer_id\nC-1\n", encoding="utf-8")
    orders = tmp_path / "missing-orders.csv"

    def unexpected_extraction(*args, **kwargs):
        pytest.fail("A file was extracted before both required files were checked")

    monkeypatch.setattr(extract_module, "extract_csv", unexpected_extraction)

    result = extract_required_csv_files(customers, orders)

    assert not result.complete
    assert result.customers is None
    assert result.orders is None
    assert [(error.dataset_name, error.error_code) for error in result.batch_errors] == [
        ("orders", "missing_file")
    ]


def test_required_file_preflight_reports_both_missing_inputs(tmp_path):
    result = extract_required_csv_files(
        tmp_path / "missing-customers.csv",
        tmp_path / "missing-orders.csv",
    )

    assert not result.complete
    assert [error.dataset_name for error in result.batch_errors] == [
        "customers",
        "orders",
    ]
    assert all(error.error_code == "missing_file" for error in result.batch_errors)


def test_extraction_does_not_access_sqlite(tmp_path, monkeypatch):
    source = tmp_path / "customers.csv"
    source.write_text("customer_id\nC-1\n", encoding="utf-8")

    def unexpected_sqlite_access(*args, **kwargs):
        raise AssertionError("Extraction must not access SQLite")

    monkeypatch.setattr(sqlite3, "connect", unexpected_sqlite_access)

    result = extract_csv(source, "customers")

    assert result.complete
    assert len(result.records) == 1


def test_reports_unreadable_file_as_structured_preflight_error(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "unreadable.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    original_open = extract_module.Path.open

    def fail_binary_open(path, mode="r", *args, **kwargs):
        if path == source and mode == "rb":
            raise PermissionError("sensitive local path")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(extract_module.Path, "open", fail_binary_open)

    result = extract_csv(source, "customers")

    assert not result.complete
    assert result.batch_error is not None
    assert result.batch_error.error_code == "unreadable_file"
    assert result.batch_error.source_file == "unreadable.csv"
    assert str(tmp_path) not in result.batch_error.error_message


def test_reports_read_error_after_successful_preflight(tmp_path, monkeypatch):
    source = tmp_path / "read-error.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    original_open = extract_module.Path.open

    def fail_text_open(path, mode="r", *args, **kwargs):
        if path == source and mode == "r":
            raise PermissionError("cannot read")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(extract_module.Path, "open", fail_text_open)

    result = extract_csv(source, "customers")

    assert not result.complete
    assert result.batch_error is not None
    assert result.batch_error.error_code == "file_read_error"
    assert result.batch_error.source_file == "read-error.csv"
