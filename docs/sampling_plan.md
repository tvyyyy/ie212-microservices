# Sampling Plan

Exact partition counts, storage footprints, and entity coverage metrics will be finalized after the `P0` sample inspection (`T04`, `T06`, `T07`).

## Locked Acquisition & Sampling Strategy (25 November 2026)

1. **`MSCallGraph` (Trace-Complete Hash Partitions):** Use the pre-shuffled **Harvard Dataverse CASPER Shuffled v2021** dataset (`doi:10.7910/DVN/RXIC9Z`, Huye et al., ACM/SPEC ICPE 2024; 1,999 `partition_id=*.csv.gz` files, ~21.05 GiB total). Select a deterministic **10–25%** subset of hash partitions (200–500 files ≈ 2.1–5.2 GiB compressed) so every selected `traceid` retains all of its call records across the 12-hour window without cross-file fragmentation.
2. **`Node`, `MSResource`, and `MSRTQps` (Full 12-Hour Time Series):** Stream-download the full 12-hour archives sequentially from Alibaba OSS (`Node_0`, `MSResource_0..11`, `MSRTQps_0..24`), record source URLs, file sizes, and SHA-256 checksums in `artifacts/manifest/`, convert immediately to Bronze Parquet (ZSTD/Snappy), and delete temporary `.tar.gz` archives to conserve disk space while preserving full temporal continuity for RQ2.
3. **Prohibited Sampling:** Never use `head(N)` on `MSRTQps` (rows are grouped in metric blocks) or `MSCallGraph` (breaks trace structure).

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).
