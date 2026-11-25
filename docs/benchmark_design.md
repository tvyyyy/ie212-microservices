# Benchmark Design

Benchmarks have not yet been executed (`T31`–`T35`). Locked design rules as of **25 November 2026**:

- **Engines & Workloads:** Compare **PySpark** (pinned `4.1.*` or `3.5.*`), **DuckDB**, and **Pandas** on three workloads from fixed Bronze/Silver inputs: (1) service-minute metric aggregation, (2) metrics + container/host resource joins (with 30s→60s alignment), and (3) service-edge aggregation.
- **Scales & Repetitions:** Run at **10%, 25%, 50%, and 100%** of the declared benchmark input, with at least **3 repetitions** per configuration on the primary Docker Linux host (Vy's machine) under fixed CPU/RAM limits.
- **DuckDB Interoperability:** Test direct `iceberg_scan('s3://...')` over MinIO in `T08`; if catalog compatibility limits arise, use a documented Parquet export for RQ3 and report export cost separately.
- **Core Scaling:** Run a separate **1/2/4/8-core** scaling experiment (within physical host core limits) on a representative parallelizable workload (metrics–resource–host join or edge aggregation) on Spark and DuckDB (noting single-threaded Pandas behavior).
- **Reporting:** Verify output equivalence (row counts, keys, checksums/numeric tolerance) before comparing wall-clock times with explicit result materialization; record cache policy, peak RAM (including JVM where measurable), spill (or `NA`), and OOM/timeout/unsupported outcomes.

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).
