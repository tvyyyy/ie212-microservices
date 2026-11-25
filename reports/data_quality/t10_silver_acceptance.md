# T10 — Measured Silver Acceptance

Accepted on 26 November 2026 (Asia/Saigon), using Vy's Docker Linux / WSL2 host. Spark 4.1.4, Python 3.12.13, 2 local worker threads; configured driver/executor memory 2g. Transformation source: `978a727`.

Run ID: `t10_20261125T190326Z`. Scope: all rows of the three declared OSS development files (inclusive 0..600000 ms). Full 12-hour acquisition, calls/trace transformation, and model/label acceptance are separate work.

| Dataset | Input rows | Native canonical rows | Quarantine source rows | Minute rows | Partial time windows | Incomplete measurement windows |
|---|---:|---:|---:|---:|---:|---:|
| oss_node | 275,356 | 275,356 | 0 | 144,234 | 13,112 | 13,132 |
| oss_msresource | 2,023,518 | 2,023,518 | 0 | 1,059,938 | 96,358 | 96,448 |
| oss_msrtqps | 5,790,196 | 5,790,196 | 0 | — | — | — |

- All eight native/minute/quarantine tables were written and read back through Iceberg. Source-row accounting passed for all 8,089,070 native input observations.
- A second full execution preserved every Iceberg table row count and all join diagnostics; no row append was observed. The report compares counts/joins, not Parquet byte identity.
- Independent DuckDB checks found zero duplicate native or minute keys. Every integer source offset appeared exactly once across native/quarantine outputs, with matching filename and source SHA-256. All recomputed minute averages matched within absolute tolerance 1e-12.
- Metric/resource left join: 1,058,011 container-minute keys before and after; 1,057,824 resource matches (187 unmatched). Resource/host left join: 1,059,938 rows before and after, all matched. Multiplication factor: 1.0.
- Resource had zero ambiguous host windows in this sample; synthetic regression fixtures exercise the host-change/quarantine behavior. Partial final-minute buckets are retained and flagged. A full time bucket can still have missing numeric values, which is why complete-measurement counts differ from partial-time counts.
- All 122 regression tests passed in the Linux Python 3.12 container (140.14 s), including stale-success report invalidation on a failed replacement run.

## Reproduction and provenance

Follow [Silver metrics runbook](../../docs/silver_metrics_runbook.md). [Structured acceptance](t10_silver_acceptance.json) contains exact source hashes, Iceberg readbacks, row accounting, independent checks, and rerun comparisons. [T06 raw profile](t06_sample_profile.json) and [T07 acquisition audit](t07_sampling_audit.json) establish input scope.

Local ignored logs: `artifacts/t10-silver-run.log`, `artifacts/t10-silver-rerun.log`, `artifacts/t10-independent-verification.log`, and `artifacts/t02-t06-t07-t10-final-tests.log`. The first-run count snapshot is retained under `artifacts/t10/first_run_report.json`.

Known boundaries: RT/MCR measurement-window semantics remain unverified before T19 weighting; HTTP metrics remain suspect; finite utilization above 1 remains flagged; no missing RT is imputed to zero.
