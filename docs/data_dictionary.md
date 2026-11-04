# Customer and Order Data Dictionary

This document describes the CSV input contract, validation and transformation
behavior, SQLite storage, and source traceability implemented in
`pipeline/extract.py`, `pipeline/validate.py`, `pipeline/transform.py`, and
`pipeline/database.py`.

## Conventions and schema behavior

- CSV input fields are read as Python strings. Numeric and date-like values are
  not automatically converted by extraction.
- Header names are exact and case-sensitive. Customer CSVs require all
  customer required columns and may include the optional `phone` column.
  Order CSVs require all order columns and have no optional columns.
- Header ordering is not prescribed by the validator; row values are mapped
  using the actual header positions.
- Missing required columns, duplicate header names, and unexpected columns
  produce batch-level schema errors. They do not become row-level rejects.
- A CSV row with a different number of values than the header is a recoverable
  malformed row. The extractor records it separately and continues where
  possible.
- Validation does not modify source records. Transformations create new
  immutable records while retaining the original `source_record`.
- SQLite business tables store normalized customer/order fields and separate
  source lineage columns. Exact table constraints are summarized below.

## Customers dataset

CSV columns, in the sample generator's order:
`customer_id, first_name, last_name, email, signup_date, country_code, status,
phone`.

| Field | CSV input type | Required? | Null / empty handling and validation | Transformation | SQLite storage and constraints |
|---|---|---|---|---|---|
| `customer_id` | Text (`str`) | Yes | Must contain a non-whitespace character (`missing_customer_id`). Surrounding whitespace is allowed at validation. Duplicate comparison trims surrounding whitespace and preserves case. | Trim surrounding whitespace; preserve case. | `customers.customer_id TEXT PRIMARY KEY NOT NULL`; trimmed value must be nonempty. |
| `first_name` | Text (`str`) | Yes | Must contain 1–100 non-padding characters. Whitespace-only values are rejected (`missing_required_field`); out-of-range trimmed lengths use `invalid_name_length`. Validation measures the trimmed value while retaining the original until transformation. | Trim surrounding whitespace. | `TEXT NOT NULL`; SQLite checks trimmed length 1–100. |
| `last_name` | Text (`str`) | Yes | Same rule as `first_name`: 1–100 non-padding characters and not whitespace-only (`missing_required_field`, `invalid_name_length`). | Trim surrounding whitespace. | `TEXT NOT NULL`; SQLite checks trimmed length 1–100. |
| `email` | Text (`str`) | Yes | Must be nonempty after trimming (`missing_required_field`) and have a plausible local-part and dotted domain (`invalid_email_format`). Maximum checked length is 254. Surrounding whitespace and uppercase letters are accepted by validation. | Trim surrounding whitespace and lowercase. | `TEXT NOT NULL`; SQLite checks only that trimmed value is nonempty. |
| `signup_date` | Text (`str`) | Yes | Must be a real calendar date written exactly `YYYY-MM-DD`; alternate ISO spellings are rejected (`invalid_signup_date`). | Preserved as validated. | `TEXT NOT NULL`; SQLite checks length is 10. |
| `country_code` | Text (`str`) | Yes | Trimmed and uppercased for comparison to the full 249-code ISO 3166-1 alpha-2 allowlist snapshot in `validate.py`; other values use `unsupported_country_code`. Source casing/whitespace is retained until transformation. | Trim and uppercase. | `TEXT NOT NULL`; SQLite checks exactly two uppercase ASCII letters using `[A-Z][A-Z]`. |
| `status` | Text (`str`) | Yes | Must exactly equal `active` or `inactive`; comparison is case-sensitive and does not trim whitespace (`invalid_customer_status`). | Preserved as validated. | `TEXT NOT NULL`; check allows only `active` and `inactive`. |
| `phone` | Text (`str`) | No; optional column | The column may be absent. If present, an empty cell is allowed and preserved as `""`. There is no phone-format validation. CSV does not have a separate null token interpretation. | Preserved as text without trimming or numeric conversion, including leading zeroes and `+`. If the column is absent, transformed value is `None`. | `TEXT`, nullable. When column exists with an empty CSV cell, stored value is the empty string rather than SQL `NULL`. |

The database customer table also stores `source_file`, `source_row_number`,
`first_seen_run_id`, and `loaded_at`. Those are pipeline metadata, not CSV
columns.

## Orders dataset

CSV columns, in the sample generator's order:
`order_id, customer_id, order_ts, total_amount_cents, currency_code, status`.

| Field | CSV input type | Required? | Null / empty handling and validation | Transformation | SQLite storage and constraints |
|---|---|---|---|---|---|
| `order_id` | Text (`str`) | Yes | Must contain a non-whitespace character (`missing_order_id`). Surrounding whitespace is permitted at validation. Duplicate comparison trims whitespace and preserves case. | Trim surrounding whitespace; preserve case. | `orders.order_id TEXT PRIMARY KEY NOT NULL`; trimmed value must be nonempty. |
| `customer_id` | Text (`str`) | Yes | Must contain a non-whitespace character (`missing_customer_id`) and match a validated customer reference (`invalid_customer_reference`). Reference matching trims surrounding whitespace and preserves case. A reference with conflicting supplied signup dates is ambiguous (`ambiguous_customer_reference`). | Trim surrounding whitespace; preserve case. | `TEXT NOT NULL`; foreign key to `customers.customer_id`. |
| `order_ts` | Text (`str`) | Yes | Must be a valid timezone-aware ISO-8601 timestamp (`invalid_order_timestamp`). Accepted forms include minute precision (`2026-11-04T10:30Z`) and seconds, with a `Z` or valid numeric UTC offset. Invalid calendar/time values, missing timezones, and invalid offsets are rejected. | Normalize to a timezone-aware UTC ISO timestamp. Transformation accepts fractional seconds from 1 to 6 digits; more than 6 digits can pass the validation parser but are rejected during transformation. | `TEXT NOT NULL`; SQLite checks nonempty text. Stored form is UTC ISO text. |
| `total_amount_cents` | Text (`str`) | Yes | Must consist of decimal digits only (`invalid_order_amount`) and parse to an integer greater than zero (`non_positive_order_amount` for zero). Signs, decimal points, and whitespace are not accepted. | Convert to Python `int`. Values above SQLite's signed 64-bit maximum (`9223372036854775807`) are rejected during transformation. Never converted to floating-point currency. | `INTEGER NOT NULL CHECK (total_amount_cents > 0)`. The transformer enforces the signed 64-bit SQLite maximum. |
| `currency_code` | Text (`str`) | Yes | Must exactly equal `USD`; case-sensitive (`unsupported_currency`). | Preserved. | `TEXT NOT NULL CHECK (currency_code = 'USD')`. |
| `status` | Text (`str`) | Yes | Must exactly equal one of `placed`, `paid`, `shipped`, `delivered`, `cancelled`, or `returned`; case-sensitive (`invalid_order_status`). | Preserved. | `TEXT NOT NULL`; check restricts it to those six statuses. |

The database order table also stores `source_file`, `source_row_number`,
`first_seen_run_id`, and `loaded_at`. Those fields are pipeline metadata, not
CSV columns.

## Relationships and business rules

- **Customer primary key:** `customers.customer_id`.
- **Order primary key:** `orders.order_id`.
- **Relationship:** One customer may have multiple orders. Each
  `orders.customer_id` references one `customers.customer_id`.
- **Reference validation:** `validate_orders(extraction, known_customers)`
  receives the customer ID to signup-date map produced by
  `build_customer_reference_map(validated_customers)`. Matching trims
  surrounding whitespace but preserves case. Unknown references use reason
  code `invalid_customer_reference`; conflicting references can produce
  `ambiguous_customer_reference`.
- **Signup-date rule:** The UTC calendar date of `order_ts` must not precede
  that customer's `signup_date`. A violation uses
  `order_before_customer_signup`.
- **Duplicate source keys:** Duplicate detection only groups individually
  valid candidate rows. Keys are compared after trimming, case-sensitively.
  If occurrences have identical non-key source values, the first valid
  occurrence is retained and later occurrences are rejected with
  `duplicate_record`. If the same normalized key has conflicting values, all
  occurrences are rejected with `conflicting_duplicate_key`. The original
  values and provenance remain available on each rejection.
- **Database ID conflicts:** During loading, an existing key with identical
  normalized business values is classified as `unchanged`. A different
  business payload is rejected; the existing business row is never
  overwritten.
- **Immutable records:** Extraction, validation, and transformation result
  records use frozen dataclasses. Validation does not rewrite extracted
  values; transformation creates normalized records and carries the original
  `ExtractedRecord` through `source_record`.
- **Accepted and rejected handling:** Accepted inserts and unchanged records
  are written to `accepted_records` with their `outcome`. Rejections are
  written to `rejected_records` with reason and source traceability. Known
  input/validation/transform rejects are distinct from batch-level errors in
  `batch_errors`. Customers and orders publish in one atomic transaction,
  customers first.

## Pipeline traceability

| Name | Where it appears | Meaning |
|---|---|---|
| `run_id` | `pipeline_runs.run_id`, metric and audit tables, and business rows' `first_seen_run_id` | UUID identifying one pipeline execution. The database requires the UUID's 36-character hyphenated representation. |
| `source_file` | Extraction records and database business/audit/metrics tables | Filename identifying the input source. Extraction and the loader store a basename rather than an absolute local path. |
| `source_row_number` | `ExtractedRecord`, accepted/rejected audit tables, and business tables | One-based logical data-row number, excluding the CSV header. A multiline CSV record remains one logical row. |
| `physical_line_start`, `physical_line_end` | `ExtractedRecord` and `rejected_records` | One-based physical file line positions covered by the CSV record. Multiline records span multiple physical lines. |
| `raw_record` | `ExtractedRecord`, malformed records, and `rejected_records` | Original CSV record text, retained for rejection traceability. It can include source quoting, commas, and embedded newlines. It is not stored for accepted audit records. |
| `record_key` | Rejected audit rows and `RowRejection` | Available source identifier, typically `customer_id` or `order_id`; may be `NULL` when it cannot be identified. |
| `reason_code` | `ValidationIssue`, `RowRejection`, and `rejected_records` | Stable machine-readable explanation, such as `invalid_email_format`, `invalid_order_amount`, `invalid_customer_reference`, or `field_count_mismatch`. |
| `stage` | `BatchError` and `batch_errors` | Stage associated with a batch-level error. Database-allowed values are `input_check`, `schema_check`, `csv_parse`, `validation`, `transform`, `load`, and `finalize`. |

Extraction result structures distinguish recoverable `malformed_rows` from
fatal `batch_error` values. Schema failures and fatal read/parse failures are
batch errors rather than row-level validation rejects. Rejected source
occurrences retain their available logical row, physical lines, record key,
raw text, reason code, and reason detail.

## Metrics and status fields

The `run_dataset_metrics` table stores per-dataset input and outcome counts.
The schema enforces these reconciliation equations when the participating
values are known:

- `rows_read = rows_parsed + malformed_rows`
- `rows_parsed = rows_valid_candidates + rows_rejected_parsed`
- `rows_accepted = rows_inserted + rows_unchanged`

For a completed publish, `rows_rejected` counts all rejected source
occurrences, including records rejected after passing validation:

- `rows_rejected = malformed_rows + rows_rejected_parsed + load-stage rejects`

For example, an order can pass validation but be rejected during loading if
its customer foreign key cannot be satisfied. That occurrence is recorded in
`rejected_records` with its load-stage reason and traceability, and contributes
to `rows_rejected` rather than `rows_accepted`.

Metrics that cannot be determined remain `NULL`. Dataset and run statuses
include `running`, `succeeded`, `completed_with_rejects`, `failed`, and
`abandoned` (dataset metrics also have `pending`). Reconciliation status is
one of `pending`, `complete`, `incomplete_batch_failure`, or
`incomplete_publish_failure`.
