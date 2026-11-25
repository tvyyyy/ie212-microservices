# Silver Measurements — T10

`scripts/run_silver.py` implements Node, MSResource, and MSRTQps normalization with Spark and Iceberg. Calls/trace T13/T14 are separate work. Use the tested Linux container on Vy's Windows/WSL2 host; see `resources.md` for actual memory/disk limits.

## Input and execution

Start the existing stack following [T08 runbook](smoke_test_runbook.md). The ignored `.env` supplies local MinIO credentials. The following commands use existing whole development sample files; no new downloads or `head(N)` sampling are performed.

```bash
docker compose exec -T spark python scripts/run_bronze.py \
  --dataset oss_node --input data/sample/node/Node_sample_0_10m.csv.gz \
  --output-dir data/raw/bronze_t10 --manifest-path artifacts/t10/bronze_manifest.json --chunk-rows 25000
docker compose exec -T spark python scripts/run_bronze.py \
  --dataset oss_msresource --input data/sample/msresource/MSResource_sample_0_10m.csv.gz \
  --output-dir data/raw/bronze_t10 --manifest-path artifacts/t10/bronze_manifest.json --chunk-rows 25000
docker compose exec -T spark python scripts/run_bronze.py \
  --dataset oss_msrtqps --input data/sample/msrtqps/MSRTQps_sample_0_10m.csv.gz \
  --output-dir data/raw/bronze_t10 --manifest-path artifacts/t10/bronze_manifest.json --chunk-rows 25000

docker compose exec -T spark python scripts/run_silver.py --check-config
docker compose exec -T spark python scripts/run_silver.py \
  --bronze-root data/raw/bronze_t10 --namespace silver --threads 2
```

`--dataset` may select one of the three tables. `--namespace` allows an isolated verification namespace. `--target local` uses host-accessible endpoints when the host has matching Spark/Java/Iceberg dependencies. Avoid simultaneous heavy Spark runs on the 7.45 GiB WSL allocation. The default CLI requires configured MinIO/Iceberg access; `--check-config` only validates and does not claim execution.

## Output contracts

| Iceberg table in selected namespace | Grain |
|---|---|
| `node` | Unique `(nodeid, timestamp)`, native 30s |
| `ms_resource` | Unique `(msname, msinstanceid, timestamp)`, native 30s; includes node mapping |
| `ms_rt_qps` | Unique `(msname, msinstanceid, timestamp, metric)`, native 60s |
| `node_minute` | Unique `(nodeid, minute_bucket)` |
| `ms_resource_minute` | Unique `(msname, msinstanceid, minute_bucket)` |
| Corresponding `*_quarantine` | Invalid key/time/type or conflicting normalized payload groups with reason and full lineage |

All native tables contain source row pointers (file, offset, SHA-256) in `source_lineage` and original observation multiplicity in `observation_count`. `bronze_*` columns are string renderings of **Bronze values**, not a promise to recover pre-coercion CSV tokens: the raw file/offset and the separate T06 raw-string report are authoritative for those tokens.

Normalize known sentinel identifiers to null. Reject invalid/missing timestamps, negative timestamps, off-cadence observations, missing entity identifiers, unknown metric types, and every variant of a conflicting candidate key into quarantine. Collapse equal Bronze payloads with all row pointers retained. Never average conflicting key variants or choose one arbitrarily.

Invalid, non-finite, or negative numeric measurements remain present with an explicit validity flag and null canonical value. Numeric validity does not establish measurement semantics. Keep finite resource utilization above 1 and flag it; do not clamp it, convert to CPU cores, or subtract container utilization from host utilization. HTTP_MCR/HTTP_RT rows carry `http_metric_suspect=true` based on the reproduced source bug. Zero MCR remains zero; missing RT remains null. No labels, RT/MCR weighting, or zero-latency imputation are performed here.

## Minute alignment and joins

`minute_bucket = floor(timestamp / 60000)`; `minute_ts = minute_bucket * 60000`. Averages use only the two possible 30s samples inside the same minute. Record valid numeric sample counts, `half_window_sample_count`, `is_partial_minute`, and `is_complete_measurement_window`. A complete time bucket can still lack a numeric value. If a container changes hosts within a minute, retain the host set, set `host_mapping_ambiguous=true`, and leave minute-level `nodeid` null; do not choose the first host or create duplicate minute keys.

This floor convention is a processing decision. MSRTQps has observed recording cadence 60s, but exact RT/MCR measurement-window boundary semantics remain unverified (`window_boundaries_verified=false`); T19 must address that before weighted labels. Key-join coverage does not establish a valid modeling cohort.

The all-dataset run checks metric-container-minute → resource-minute and resource-minute → node-minute left joins. Missing matches remain observable; row counts must not multiply. Downstream work must also honor partial/invalid/ambiguous flags.

## Verification, reruns, and evidence

Each table is atomically replaced through Iceberg `createOrReplace`, partitioned by `minute_bucket`; it is not appended on rerun. The run ID is recorded as a table property. The eight-table update is **not** a multi-table transaction: consume a run only after its report says `status=passed`. The runner checks source-row accounting, writes and reads every Iceberg table, validates row counts, and exports local Parquet snapshots to ignored `artifacts/t10/silver`.

Measured results are in [t10_silver_acceptance.json](../reports/data_quality/t10_silver_acceptance.json); T06/T07 input scope and checksums remain separate. Fixtures are explicitly synthetic and test duplicate lineage, key conflicts, invalid numeric values, zero traffic, minute boundaries, missing half-windows, and host changes.

```bash
docker compose exec -T spark python -m pytest -v tests
```

These are development-sample acceptance results. Full 12-hour continuity, full-data scale, causal interpretations, call-side repairs, and model accuracy are not claimed.
