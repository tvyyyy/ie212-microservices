# T06 — Measured Development-Sample Quality

Measured on 26 November 2026 (Asia/Saigon). All rows of the 8 explicitly listed local sample files were scanned. Counts apply only to this scope; JSON timestamps are UTC.

| Dataset | Files | Rows | Exact duplicate excess | Candidate-key multiple-payload groups |
|---|---:|---:|---:|---:|
| oss_node | 1 | 275,356 | 0 | 0 |
| oss_msresource | 1 | 2,023,518 | 0 | 0 |
| oss_msrtqps | 1 | 5,790,196 | 0 | 0 |
| casper_mscallgraph | 5 | 2,443,480 | 205,078 | 276,045 |

Exact duplicates compare every domain column as raw strings, excluding source indexes and filenames. For CASPER, `(traceid, rpcid)` is an occurrence grouping, not a unique key: multiple payloads can be legitimate two-sided or repeated observations, not proven errors. No call-side repair or canonical call counting is performed by T06.

## Missing / sentinel values

| Dataset | Column | Missing rows | Denominator |
|---|---|---:|---:|
| oss_node | node_cpu_usage | 42 | 275,356 |
| oss_node | node_memory_usage | 21 | 275,356 |
| oss_msresource | instance_cpu_usage | 189 | 2,023,518 |
| oss_msresource | instance_memory_usage | 189 | 2,023,518 |
| casper_mscallgraph | um | 216,431 | 2,443,480 |
| casper_mscallgraph | dm | 188,873 | 2,443,480 |
| casper_mscallgraph | interface | 1,339,578 | 2,443,480 |

The JSON provides blank, SQL-null, and sentinel-token counts. Empty interface values are not automatically invalid calls: some communication types have no interface. Unknown endpoints must remain null/flagged, not be merged into an artificial common service.

## Findings and resulting policy

- All measured timestamps were valid integer milliseconds. Node and MSResource use the 30,000 ms grid with both 0 and 30,000 ms minute residues; MSRTQps uses the 60,000 ms grid. Development OSS samples include timestamp 600,000, creating an eleventh, partial minute bucket.
- Three OSS candidate keys had zero duplicate/conflicting groups on these samples. This is sample evidence, not a guarantee of whole-dataset uniqueness.
- Resource CPU exceeded 1 in 24 observations (maximum 1.1082083333361272). Retain values with an above-one flag; unpublished capacities prevent interpreting this as a comparable number of CPU cores.
- All 246,620 paired HTTP metric keys had equal HTTP_MCR and HTTP_RT. Mark both HTTP metrics suspect; do not treat equality as a validated RT label.
- CASPER has 51,956 known trace IDs, 224,208 known self-loop rows, and no trace ID spanning more than one of the five sampled files. Hash locality does not establish complete/valid parent-child topology.
- CASPER MQ has 37,288 negative and 31,719 zero RT values among 81,659 rows. Negative MQ RT is not a majority in this sample. Retain these events for topology, flag negative duration, and exclude it from latency summaries. RPC/HTTP negative RT retains its caller/callee-side meaning.

## Join coverage (keys, before modeling exclusions)

- Resource services: 1,303; metric services: 1,301; overlap: 1,301.
- Metric container-minute keys: 1,058,011; resource matches: 1,057,824; 187 lack a resource match. Resource timestamps are floored to minutes for this diagnostic only.
- Known call services: 5,889; metric services: 1,301; overlap: 578. This is 9.81% of call services and 44.43% of metric services in the selected sample, not a dataset-wide overlap estimate.

## Reproduction and evidence

```bash
python scripts/run_quality.py
# Optional: reuse staging only after every input file SHA-256 matches the prior evidence
python scripts/run_quality.py --reuse-raw
```

DuckDB uses a 1 GB memory limit, 2 threads, and disk spill under ignored `artifacts/t06`. No raw rows are collected into Python for group statistics. Inputs and hashes, per-column diagnostics, exact duplicates, metric counts, RT signs, and coverage are in [t06_sample_profile.json](t06_sample_profile.json). Acquisition selection/checksums are independently audited in [t07_sampling_audit.json](t07_sampling_audit.json).
