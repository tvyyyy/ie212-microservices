# Sampling Plan — T07

Audited on 26 November 2026 (Asia/Saigon). Canonical seed: **1217**. Configuration: `configs/data.yaml`.

## Frozen acquisition cohort

- CASPER Shuffled v2021: DOI `10.7910/DVN/RXIC9Z`, Dataverse version 1.0, release `2024-03-27T19:37:20Z`. The published shuffle places records sharing a trace ID in the same file; it is not evidence that every trace topology is valid or repaired.
- Population: 1,999 signed partitions (`-999..999`). Acquired cohort: **200 partitions**, `10.00500250%` of file count; **2,260,903,092 compressed bytes**. This is a partition fraction, not an observed-row or service-coverage fraction.
- Selection: lexicographically sort partition filenames, use `random.Random(1217).sample` with `round(1999 * 0.10) = 200`, then sort the selected filenames. Acquisition and Bronze selectors are tested to return the same cohort.
- Exact 200 filenames and integrity results are committed in [t07_sampling_audit.json](../reports/data_quality/t07_sampling_audit.json). The audit recomputes SHA-256 against the acquisition manifest and CASPER MD5 against the cached Dataverse v1.0 listing. It fails on missing files, checksum mismatch, seed drift, or a different cohort. No new downloads were required.

## Development / T06 / T10 cohort

The five CASPER development files are the first five lexicographically sorted files in the acquired 200-partition cohort:

- `partition_id=-10.csv.gz`
- `partition_id=-106.csv.gz`
- `partition_id=-131.csv.gz`
- `partition_id=-133.csv.gz`
- `partition_id=-138.csv.gz`

These five files are **not an independently randomized 5-file sample** and may overrepresent some services. They contain 2,443,480 rows and 51,956 known trace IDs. T06 found zero trace IDs crossing these five files; this is a local consistency check, not a proof that parent-child topology is complete. Do not use `head(N)` or time truncation on call partitions.

OSS development files contain **all rows with `0 <= timestamp <= 600000` ms** extracted from archive 0 for Node, MSResource, and MSRTQps. The inclusive endpoint yields 11 minute buckets; minute 600000 is partial for 30s inputs. Metrics are scanned across all metric blocks before filtering by time, retaining every metric in the selected interval. Node: 275,356 rows; Resource: 2,023,518; RTQps: 5,790,196.

Acquired OSS archives currently comprise Node_0, MSResource_0, and MSRTQps_0. Only the declared development window was profiled for T06/T10. A single Resource/RTQps archive is not evidence of a full 12-hour time series. The separate acquisition manifest keeps original source URLs, checksums, and derived-sample lineage.

## Research-scale acquisition and gates

- RQ1 may use the frozen 200 whole CASPER partitions; any change to the 10–25% fraction requires a new declared cohort/manifest and new quality measurements. Increasing `random.sample` size is not assumed to preserve a nested cohort.
- RQ2 requires full 12-hour Node/Resource/RTQps continuity: Node index 0, Resource indexes 0..11, RTQps indexes 0..24. Use the bounded `run_bronze.py --url` stream/convert path, record original checksums, and delete temporary archives only after successful conversion. Existing local files go through `--input`. `download_data.py` and Bronze maintain separate manifests.
- Check free space on both host volumes and Docker's backing disk, WSL memory, measured row counts/time ranges, and service-minute coverage before enlarging a run. No automatic full-data download is part of this task.
- Model train/validation/test boundaries and the graph warm-up are selected later from measured coverage. Development results do not establish generalization or causal bottlenecks.
- Benchmark 10/25/50/100% means fractions of a frozen declared benchmark input. Use a seeded filename ranking and nested prefixes of whole trace partitions for call workloads, or a documented compatible entity/time unit for metrics. Record actual rows/bytes per level. Do not confuse these percentages with the CASPER population or the upstream 0.5% call sampling rate.

## Reproduction

```bash
python scripts/audit_sampling.py
python scripts/run_quality.py
```

The audit is read-only for raw/sample inputs and writes its result under `reports/data_quality`. Raw data, local manifests/cache, staging databases, Parquet outputs, and credentials remain outside Git. [T06](../reports/data_quality/t06_sample_profile.md) supplies measured null/sentinel, duplicate, cadence, and join coverage for the development scope.
