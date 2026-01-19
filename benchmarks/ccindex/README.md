# CCIndex Benchmarks

This folder contains one-off benchmark scripts for the Common Crawl (CCIndex) tooling.

## Running

Most benchmarks assume the canonical storage layout:

- Parquet pointers: `/storage/ccindex_parquet/`
- DuckDB indexes: `/storage/ccindex_duckdb/`

Examples:

- `python3 benchmarks/ccindex/benchmark_cc_domain_search.py`
- `python3 benchmarks/ccindex/benchmark_cc_duckdb_search.py --quick`
- `python3 benchmarks/ccindex/benchmark_cc_pointer_search.py --count 50`
- `python3 benchmarks/ccindex/benchmark_parallel_duckdb_indexes.py`

## Outputs

Some benchmarks write JSON output files into the *current working directory* (CWD). If you want outputs to land next to the scripts, run them from repo root and pass an explicit output path when supported.
