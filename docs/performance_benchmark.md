# Performance Benchmark

## Objective and methodology

This benchmark measures the customer-and-order pipeline at a reproducible
10,000-customer and 50,000-order input size. The synthetic CSVs were generated
with seed `42`. Three independent iterations were run, each using a fresh
temporary SQLite database.

The benchmark times data generation separately from the pipeline. Pipeline
stages use `time.perf_counter()` and cover:

- **Extraction:** read both generated CSV files.
- **Validation:** validate customers, build the customer reference map, and
  validate orders.
- **Transformation:** transform both validated datasets.
- **Loading:** initialize the database, create the run, and publish both
  datasets atomically.
- **Total pipeline:** database initialization through successful publishing
  and result retrieval. Data generation is excluded.

Throughput is generated input records divided by the measured duration. The
reported average throughputs below are arithmetic means of the three measured
per-iteration rates. Timing details and methodology notes are in the benchmark
JSON and CSV outputs.

## Environment and configuration

| Property | Recorded value |
|---|---|
| Python | 3.14.10 |
| Customers per iteration | 10,000 |
| Orders per iteration | 50,000 |
| Iterations | 3 |
| Random seed | 42 |
| Database | Fresh temporary SQLite database per iteration |

The detailed CSV records distinct Windows temporary database paths for the
three iterations. The benchmark artifacts do not record hardware model,
processor, memory, storage device, or OS build, so those specifications cannot
be reported.

## Measured stage durations

Times are seconds; average, minimum, and maximum are calculated across the
three iterations.

| Stage | Average (s) | Minimum (s) | Maximum (s) |
|---|---:|---:|---:|
| Data generation | 0.538524 | 0.497462 | 0.574750 |
| Extraction | 0.266855 | 0.238913 | 0.305669 |
| Validation | 0.378563 | 0.336541 | 0.404323 |
| Transformation | 0.433772 | 0.424667 | 0.449078 |
| Loading | 2.064060 | 1.895657 | 2.193611 |
| Total pipeline, generation excluded | 3.143258 | 2.937263 | 3.261524 |

Generation is shown separately and is **not** included in total pipeline time.
The loading measurement includes database initialization and run creation, in
addition to atomic publishing.

## Pipeline throughput

The total input size is 60,000 records. Total pipeline throughput is combined
customer-and-order records per second.

| Iteration | Total pipeline (s) | Customers/s | Orders/s | Combined records/s |
|---:|---:|---:|---:|---:|
| 1 | 3.230987 | 3,095.03 | 15,475.15 | 18,570.18 |
| 2 | 3.261524 | 3,066.05 | 15,330.25 | 18,396.31 |
| 3 | 2.937263 | 3,404.53 | 17,022.65 | 20,427.18 |
| **Mean measured rate** | — | **3,188.54** | **15,942.68** | **19,131.22** |

## Load outcomes and reconciliation

All three runs completed with status `succeeded`. The per-iteration counts
were identical:

| Dataset | Accepted | Rejected | Inserted | Unchanged | Dataset status | Reconciliation |
|---|---:|---:|---:|---:|---|---|
| Customers | 10,000 | 0 | 10,000 | 0 | succeeded | complete |
| Orders | 50,000 | 0 | 50,000 | 0 | succeeded | complete |

The accepted counts reconcile to inserted plus unchanged records for each
dataset. All generated records were accepted and inserted; no records were
rejected or classified as unchanged in these fresh databases.

## Bottleneck observation

Loading was the longest measured pipeline stage in every iteration, averaging
2.064060 seconds. The next-longest pipeline stage was transformation at
0.433772 seconds average. Loading therefore dominates the observed stage
durations for this workload. Its measured scope includes database setup and
run creation as well as publishing, so these results do not isolate the cost
of individual SQLite operations.

## Limitations and future work

- Results describe one dataset size, one seed, and three iterations on a
  machine whose hardware specifications were not recorded. They should not be
  generalized to other machines or workloads.
- The generated inputs contain valid data. This run does not measure the cost
  of large rejection volumes, malformed CSVs, or schema failures.
- Total pipeline time excludes data generation. Use the separate generation
  measurement when estimating end-to-end time that also includes creating
  input files.
- The loading stage combines database initialization, run creation, and
  publishing; a future benchmark could time those loader sub-operations
  separately without changing pipeline behavior.
- Future work could profile loading with larger and varied dataset sizes,
  measure repeated warm/cold database conditions, and evaluate batch-oriented
  SQL operations. These are investigation opportunities only; no optimization
  improvement is claimed or measured here.

## Source results

This report is based on the recorded benchmark outputs:

- [Detailed JSON](../data/output/benchmark.json)
- [Detailed CSV](../data/output/benchmark.csv)
- [Duration summary JSON](../data/output/benchmark_summary.json)
- [Duration summary CSV](../data/output/benchmark_summary.csv)
