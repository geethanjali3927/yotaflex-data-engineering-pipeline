# GitHub Copilot Chat Prompts — YottaFlex AI Data Engineering Pipeline Framework

## PROMPT 1 — PERFORMANCE BENCHMARK MODULE

Implement a reproducible performance benchmarking module in:

`pipeline/benchmark.py`

Follow the approved benchmark design and use the actual existing project APIs.

Requirements:

Use the existing synthetic data generator.

Benchmark extraction, validation, transformation, loading, and total pipeline execution separately.

Measure durations using time.perf_counter().

Measure data generation separately from pipeline execution.

Support configurable customer count, order count, seed, and number of iterations.

Every iteration must use a fresh temporary SQLite database.

Measure customer and order throughput separately, as well as combined throughput.

Record Python version, dataset sizes, seed, iteration number, stage durations, and throughput.

Record customer and order accepted, rejected, inserted, unchanged counts, run status, and reconciliation status.

Calculate average, minimum, and maximum stage durations across iterations.

Support exporting detailed iteration results and summary statistics to JSON and CSV.

Handle zero-duration measurements safely without division by zero.

Do not modify existing pipeline modules or their behavior.

Use only Python standard-library additions; no new dependencies.

Add a CLI using argparse.

Suggested command:
`python -m pipeline.benchmark --customers 10000 --orders 50000 --iterations 3 --seed 42 --output data/output/benchmark`

Do not modify existing files. Create only pipeline/benchmark.py.

After implementation:

Compile the module.

Run a small benchmark with 10 customers, 25 orders, and 2 iterations.

Verify that each iteration uses a fresh database.

Verify that both JSON and CSV outputs are generated correctly.

Report the actual measurements and verification results.

Important: Don't run the 50,000-order benchmark yet. First we'll verify the small benchmark and review its implementation. Then we'll measure larger datasets and use the real results in your documentation.

---

## PROMPT 2 — PERFORMANCE BENCHMARK DOCUMENTATION

Create docs/performance_benchmark.md using the actual benchmark results from:

data/output/benchmark.json

data/output/benchmark.csv

data/output/benchmark_summary.json

data/output/benchmark_summary.csv

Document:

Benchmark objective and methodology.

Hardware/software environment, including Python 3.14.10. Do not invent hardware specifications.

Dataset sizes: 10,000 customers and 50,000 orders.

Three independent iterations using seed 42 and fresh SQLite databases.

Average, minimum, and maximum duration for each pipeline stage.

Total pipeline execution time and throughput.

Accepted, rejected, inserted, and unchanged records.

Reconciliation results.

Loading as the main performance bottleneck.

Limitations and future optimization opportunities.

Use only measured values from the benchmark files. Clearly distinguish data generation time from pipeline execution time.

Do not invent performance comparisons or claim optimization improvements that have not been measured.

Create only docs/performance_benchmark.md. Do not modify any existing files.

After creating it, verify every reported metric against the source benchmark files.

Share Copilot's result with me. Next we'll prepare the data dictionary and architecture documentation, then update the README for your final GitHub submission.

---

## PROMPT 3 — DATA DICTIONARY DOCUMENTATION

Create a comprehensive data dictionary in a new file:

docs/data_dictionary.md

First inspect the actual schemas and validation rules in:

pipeline/validate.py

pipeline/database.py

pipeline/transform.py

pipeline/extract.py

Document the two datasets separately.

1. Customers dataset
Include every actual column:

customer_id

first_name

last_name

email

signup_date

country_code

status

phone

For each field document:

Data type

Required or optional

Null/empty handling

Validation rules

Transformation/normalization

Database constraints, where applicable

2. Orders dataset
Include every actual column:

order_id

customer_id

order_ts

total_amount_cents

currency_code

status

Document the same details for each field.

3. Relationships and business rules
Explain:

Customer primary key.

Order primary key.

Customer-to-order relationship.

Foreign-key validation.

Order timestamp and signup-date rule.

Duplicate handling.

Immutable record behavior.

Accepted and rejected record handling.

4. Pipeline traceability
Document:

run_id

source_file

source row number

raw record

rejection reason code

processing stage

Use actual field names from the code and database schema.

Important rules
Do not invent columns or constraints.

Do not silently change existing validation rules.

Clearly distinguish CSV input types from SQLite storage types.

Include examples based on the generated sample data where useful.

Create only docs/data_dictionary.md.

Do not modify production code or other documentation.

After creating the file, verify every documented field and rule against the actual source code.
