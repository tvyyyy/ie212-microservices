# Decision Log

## 23 November 2026 — Documentation Structure

- The authoritative `metadata.txt` defines the project scope.
- Maintain a flat `docs/` directory without separate report or chapter directories.
- The team has an existing LaTeX template. No `.tex` file has been created; the team will supply `docs/main.tex`.
- Store analytical figures and tables in `reports/`, and generated run outputs in `artifacts/`.
- The repository is a scaffold; no pipeline implementation or experiments have been completed.

## 23 November 2026 — Source Document Management

- Maintain `metadata.txt`, `agents_ie212.md`, and the two planning/structure workbooks (`IE212_ke_hoach.xlsx`, `IE212_cau_truc_thu_muc_cap_nhat.xlsx`) exclusively in the external workspace context directory (outside the Git repository and excluded via `.gitignore`). Do not hard-code machine-specific Windows paths inside repository files.
- The previous `output/` and `outputs/` directories were removed as requested. Deleted PDFs (including `De_xuat_IE212_theo_metadata.pdf`) must not be used to define the project scope.

## 25 November 2026 — Pre-Implementation Technical & Governance Decisions

- **Course Syllabus Alignment (IE212 2017 vs. Modern Lakehouse):** The course instructor has approved the **Python Data Lakehouse** scope (`MinIO + Parquet + Apache Iceberg + PySpark/DuckDB/Pandas`) in place of the legacy 2017 UIT syllabus stack (`Hadoop HDFS + MapReduce Java + Spark 2.2.0`). The final report will include a concise note documenting this approved modernization.
- **Primary Execution Host & Development Split:** Vy's machine (`>=16 GB RAM`, `>=100 GB free disk`) is assigned as the planned primary execution host for **Docker Linux** (`MinIO 9000/9001`, `Iceberg REST Catalog 8181`, `PySpark`); Huy's machine (`7.73 GB RAM`, `38.5 GB free disk`) focuses on modular code development, unit tests, and small-sample validation using **Python 3.12 (`uv`)**.
- **Data Acquisition & Sampling Strategy:**
  - `MSCallGraph`: Acquire 10–25% of trace-complete hash partitions from **Harvard Dataverse CASPER Shuffled v2021** (`doi:10.7910/DVN/RXIC9Z`, Huye et al., ACM/SPEC ICPE 2024) to preserve intact traces without cross-file splitting.
  - `Node`, `MSResource`, `MSRTQps`: Stream-download the full 12-hour archives from Alibaba OSS, record original checksums/manifests, convert immediately to Bronze Parquet, and remove temporary `.tar.gz` archives.
- **Schema Corrections (Alibaba OSS inspection and CASPER sample follow-up, 25 Nov 2026):**
  - The four Alibaba OSS raw CSV tables (`Node`, `MSResource`, `MSRTQps`, and OSS `MSCallGraph`) begin with an unnamed index column (`,...`). CASPER Shuffled v2021 has **8 columns starting directly with `traceid`**, with **no leading unnamed index column** (sample `partition_id=-10.csv.gz`, recorded in the PR #2 review follow-up).
  - Both `Node` and `MSResource` record at **30-second intervals** (`ts % 30000 == 0`), not 60s for `MSResource` as stated in the Alibaba README; `MSResource` places `timestamp` as the **last column** (`instance_cpu_usage`, `instance_memory_usage`). Both must be aligned 30s→60s before joining 60s `MSRTQps`.
  - `Node` vs. `MSResource` CPU/memory denominators are incompatible (Alibaba Issue #119); subtracting container CPU from node CPU is prohibited.
  - `MSRTQps` uses singular `metric`, is grouped in metric blocks (prohibiting `head(N)` sampling), and exhibits `HTTP_MCR == HTTP_RT` (Issue #120).
  - Alibaba OSS `MSCallGraph` header is `,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt` with six lowercase/camelCase `rpctype` values (`rpc`, `mc`, `http`, `userDefined`, `db`, `mq`), negative `mq` latencies, ~12% self-loops, ~17% sentinels, and `(traceid, rpcid)` multiplicities >2.
  - CASPER Shuffled v2021 `MSCallGraph` header is `traceid,timestamp,rpcid,um,rpctype,dm,interface,rt`. Its zero-based `timestamp` column index is **1**, versus **2** for the raw OSS format; select by header name where possible and do not drop CASPER's `traceid` column. The ingestion follow-up must correct the CASPER timestamp selection and regenerate affected manifest timestamp ranges before accepting them.
- **Locked Stack & Anti-Leakage Choices:**
  - Pin `pyspark==4.1.*` (matching Maven `iceberg-spark-runtime-4.1_2.13`) or `pyspark==3.5.*` (matching `3.5_2.12`) with Python 3.11/3.12 and OpenJDK 17+. Added `pyproject.toml` for `src/ie212/` editable installs.
  - Use **NetworKit** for RQ1 & Feature B centrality (`Degree`, `PageRank`, `EstimateBetweenness`, `CoreDecomposition`).
  - Use a dedicated **warm-up window** inside Train for the `train_only` graph snapshot to prevent intra-train leakage.
  - Use **DuckDB `iceberg_scan('s3://...')`** for RQ3 if validated in `T08`, with an explicit Parquet-export fallback (logging export cost separately).
  - Use **Streamlit** (`8501`) for `dashboard/` and **XeLaTeX** for compiling `docs/main.tex`.

## Silver Processing Policy — Agreed Design, Not Yet Implemented

- Preserve raw records and source file/row lineage. Log row counts and quality flags before and after every normalization, exclusion, and deduplication step.
- **Response time by communication type:** Keep raw signed `rt` in milliseconds. For `rpc`/`http`, positive and negative observations represent caller and callee respectively; zero does not identify a side. Store caller/callee RT separately; normalize the callee sign only after side classification. For `mq`, retain negative RT with an invalid-duration flag and exclude it from duration summaries/labels until its semantics are established; retain the event for topology/counts subject to deduplication. For `mc`, `db`, and `userDefined`, retain zero, flag negative duration, and exclude negative values from duration summaries. Never apply absolute value or drop negative rows across the whole table.
- **Two-sided deduplication:** `(traceid, rpcid)` is a candidate occurrence group, not a unique key. Within it, compare communication type, interface, endpoints, observation side, and timestamps. Collapse exact duplicate observations while retaining all lineage; pair caller/callee only when there is one unambiguous compatible occurrence. Count a paired occurrence once, retaining both RT observations. Do not arbitrarily select one of more than two records or deduplicate globally by `rpcid`. Keep unresolved groups in an ambiguity table and exclude them from canonical call counts and latency summaries; report excluded coverage. Timestamp matching tolerance must be validated and configured before pairing is enabled.
- **Sentinels:** Preserve original endpoint strings; normalize empty/whitespace, `(?)`, and documented missing tokens such as `NAN` to null with a sentinel flag. Never merge all unknown endpoints into one service vertex. Exclude unresolved endpoints from canonical service edges and centrality while retaining records for trace completeness diagnostics. Repair from a paired observation only if the mapping is unambiguous, with repair lineage.
- **Self-loops:** Keep identified self-calls in canonical calls and report their count separately. For the default service centrality graph, remove self-loops; compute K-core on the simple undirected projection. Preserve the directed edge table and record every projection/filter so graph conventions remain reproducible.
- Casper is a methodological reference and source of the shuffled dataset; its repair tool has not been implemented or reproduced in this project.


## 25 November 2026 — PR #3 Integration and Acceptance Contract

- PR #2 was merged into `main` at `3da2801`. PR #3 now targets `main` and includes a normal merge of `origin/main`; no branch history was rewritten.
- The team-approved canonical `configs/data.yaml` uses the PR #3 schema: `seed`, `paths.raw_dir` / `sample_dir`, structured `sources`, `datasets`, `download`, and `bronze`. Seed **1217** is used across the checked-in data, graph, model, and benchmark configurations; CASPER's sampling seed is also 1217. `datasets.casper_mscallgraph.sampling.dev_sample_count` is 5. Existing manifests retain their recorded seed and source selection; changing configuration does not retroactively change acquired data.
- `scripts/download_data.py` on `feature/sample-ingestion` owns dataset discovery, deterministic partition selection, acquisition, and the raw/sample acquisition manifest. It must migrate to this canonical schema when integrating `main`; do not replace the canonical config with its older `dataset` / `sampling` schema. `scripts/run_bronze.py` owns streaming CSV-to-Parquet conversion, lineage, the separate `bronze_manifest.json`, and optional MinIO upload. Its `--url` option is an alternative bounded download-and-convert path for large OSS input; do not download the same sources through both entrypoints in a run. Use `run_bronze.py --input` for files already acquired by `download_data.py`.
- `scripts/run_silver.py` is a **configuration-validating stub** (`status: planned_stub`), pending Vy's T10/T13/T14; no Silver ETL completion is claimed by this PR.
- Acceptance runs use Vy's confirmed Acer Windows host with Linux containers through Docker Desktop/WSL2. Hardware and runtime constraints are recorded in `resources.md`. The old Python 3.11 `.venv` was preserved; verification uses ignored `artifacts/venv-pr3` with CPython 3.12.16 and the project's declared dependencies.
- Added `.dockerignore` before building to exclude raw/sample data, virtual environments, generated artifacts, Git metadata, and `.env` from Docker's build context.
- Missing-credential tests explicitly clear supported AWS/MinIO environment fallbacks, so they test the intended missing-credential condition even inside the configured Spark container. Production credential resolution is unchanged.
- Fixed a concurrency-sensitive test: when checking cleanup before the next download chunk, only files belonging to the previous chunk must be absent; a peer download in the current chunk may already exist. This changes the test assertion, not download cleanup behavior.
- Docker Desktop initially failed before starting the Linux engine due to stale `dockerInference` and Secrets Engine socket paths (`The file cannot be accessed by the system`). With Docker stopped, the two runtime socket directories were renamed for backup, then Desktop was restarted. Linux Engine `29.4.0` became available; no image/volume/WSL-data deletion was performed by this task. Similar upstream report: https://github.com/docker/desktop-feedback/issues/448. This is host recovery evidence, not an Iceberg compatibility result.


### Measured Acceptance Results — Vy Host, 25 November 2026

| Task | Actual execution | Result |
|---|---|---|
| T03 | Windows CIM, WSL `uname -r` / `free -h`, Docker CLI/Engine inventory | Acer Ryzen 7 5700U, 8 cores / 16 threads; physical RAM 15.39 GiB; Docker Linux Engine 29.4.0, WSL kernel 6.6.87.2, 16 CPUs and 7,998,730,240 bytes memory visible to Docker. Detailed snapshots and disk constraints are in `resources.md`. |
| T05 | `docker compose up -d --build` using ignored `.env` with locally generated credentials | Build and startup succeeded. `minio` and `iceberg-rest` healthy, `minio-init` and `iceberg-rest-init` exited 0, `spark` running. Buckets `bronze`, `silver`, `gold` verified through the S3 API. |
| T08 Spark | `docker compose exec -T spark python scripts/run_smoke_test.py --mode all --target container --export-fallback --fallback-on-error` | Spark REST Catalog write/read passed: 4 synthetic rows, 2 traces, sum RT 15.25 ms. Elapsed 41.895388 s; Parquet export recorded separately at 4.670134 s. |
| T08 DuckDB | Same combined command, DuckDB 2.0.1 Iceberg REST `ATTACH` + MinIO S3 secret | Native Iceberg read passed: 4 synthetic rows, 2 traces, sum RT 15.25 ms; elapsed 0.390914 s; `fallback_used=false`, `primary_error=null`. No catalog compatibility failure was observed in this smoke test. |
| T08 local fallback | Python 3.12.16 `scripts/run_smoke_test.py --mode local --fallback-dir artifacts/pr3-local-smoke --cleanup-local` | DuckDB/Parquet round-trip passed: 4 synthetic rows, sum RT 15.25 ms. This separately checks fallback file reading; the combined native Iceberg run did not need fallback. |
| T09 | `run_bronze.py` on real `data/sample/mscallgraph/partition_id=-10.csv.gz`, 50,000-row chunks, separate acceptance output/manifest; then rerun with `--upload-s3 --s3-endpoint http://localhost:9000` | 487,950 rows; `timestamp_min=419`, `timestamp_max=43197984` ms; 0 null timestamps; `dropped_leading_index=false`; 487,950 distinct offsets spanning 0..487949. Rerun skipped local conversion (`rows_written=0`) and uploaded the existing Parquet. |
| T09 S3 readback | Downloaded `s3://bronze/ms_call_graph/dataset=casper_mscallgraph/source_file_stem=partition_id_-10/part-00000.parquet` through boto3 | 6,680,675 bytes; SHA-256 exactly matched the local Parquet file. |
| T11 Linux | `docker compose exec -T spark python -m pytest -v tests` on Python 3.12.13 | 111/111 tests passed (38.63 s), with production container credential variables present and missing-credential test cases isolated through monkeypatch. |
| T11 Windows | CPython 3.12.16: `python -m pytest -v tests`; all 9 `run_*.py --check-config` | 111/111 tests passed (46.75 s); 9/9 entrypoint config validations passed. |

Acceptance dependency versions: Windows Python 3.12.16, pandas 2.3.3, PyArrow 26.0.1, DuckDB 2.0.1, PySpark 4.1.4, PyYAML 6.0.4, boto3 1.44.11, pytest 9.2.0. The Docker image installed Python 3.12.13. These timings are smoke-test observations on this host, **not RQ3 benchmarks** and not evidence of engine performance rankings. Spark still requires Java 17 and the matching Iceberg JARs; S3 path-style settings and the documented Parquet fallback remain part of the supported configuration. WSL memory limits and C: free space constrain larger runs; no full-data acceptance, model training, Silver ETL, or scaling claim is made.

Reproducibility evidence is retained locally in ignored `artifacts/pr3-compose-build.log`, `pr3-smoke-all.log`, `pr3-smoke-summary.json`, `pr3-pytest.log`, `pr3-pytest-linux.log`, `pr3-cli-validation.json`, `pr3-bronze-local.json`, `pr3-bronze-s3.json`, `pr3-s3-verification.json`, and `pr3-acceptance/bronze_manifest.json`. Raw source partition SHA-256: `99e37af49cd19d7a87196fa0918b431fa71a74893987d799180ead590101525e` (11,075,625 compressed bytes). No secrets or raw data are committed.


## 25 November 2026 — Sample-Ingestion Integration After PR #3

- Merged `main` at `6d57ff2` into `feature/sample-ingestion` with a normal merge. Resolved the `configs/data.yaml` conflict by retaining the canonical PR #3 schema and agreed seed 1217; the branch no longer maintains a competing config format.
- Migrated `download_data.py` and its loader/directory setup to the canonical `seed`, `sources`, `datasets`, and `paths` fields. CASPER workers now default to `download.max_concurrency` and the CLI allows only 1 or 2. Discovery/acquisition remains separate from Bronze conversion/upload and uses the separate acquisition manifest.
- CASPER timestamp inspection now locates `timestamp` by CSV header. An explicitly supplied wrong column index fails clearly instead of silently reading `rpcid`. Added regression cases for both CASPER's 8-column format and OSS's leading-index format, and checked that the acquisition and Bronze selectors choose the same partition filenames with seed 1217.
- Recomputed timestamp ranges for the five already-downloaded CASPER sample entries in the local acquisition manifest, preserving raw data, hashes, original acquisition metadata, and row counts. The previous manifest was backed up under ignored `artifacts/pr3-acceptance/`. For `partition_id=-10.csv.gz`, the incorrect 0..0 range became 419..43197984 ms across 487,950 rows; all five sample entries now have measured ranges.
- Validation: 6/6 acquisition tests passed on Windows Python 3.12.16; the integrated full Linux container suite passed **117/117** (39.97 s); `download_data.py --help` and `git diff --check` passed. No new dataset downloads were launched by this migration. These changes are on `feature/sample-ingestion`, pending that branch's own review/merge.


## 26 November 2026 — T02 / T06 / T07 Evidence and T10 Policy

### T02 — Source crosscheck

- Crosschecked Alibaba's v2021 README at commit `2c9879e690f478d761a76e4777fcbb451bfd10cc` against the eight actual development-sample headers and timestamp grids. The README is a semantic reference; observed CSV schemas govern ingestion. Differences and source links are in `docs/data_dictionary.md` and `reports/data_quality/t02_schema_crosscheck.json`.
- The README's 60s Resource cadence differs from all 2,023,518 observed Resource sample timestamps, which are on a 30s grid with both minute residues. RTQps uses singular `metric`; actual source header order and field prefixes differ from README examples. CASPER has eight named columns and no source index.
- Maintainer replies in Alibaba Issues 119 and 120 document unpublished host/container capacities and the HTTP metric equality bug. Do not infer CPU-core counts or subtract host/container utilization.

### T06 — Measured raw-string quality

- Profiled all rows of three OSS development files and five CASPER partitions: 10,532,550 rows. DuckDB uses a 1 GB limit, 2 threads, disk staging/spill, and exact domain grouping. Inputs are unchanged; original tokens are inspected before Bronze numeric coercion.
- Node/Resource/RTQps sample candidate keys had no duplicate or conflicting groups. CASPER has 205,078 exact textual duplicate excess rows; `(traceid,rpcid)` has repeated/multiple payloads and maximum multiplicity 758, so it cannot be used as a unique deduplication key. This task does not implement call-side pairing/repair.
- Missing Node measurements: CPU 42, memory 21; Resource CPU/memory each 189. Resource CPU is above 1 in 24 rows and is retained/flagged, not clamped. All 246,620 paired HTTP metric keys reproduce equality and remain suspect.
- CASPER has 51,956 known traces and 370,693 rows with at least one unresolved endpoint. It has 37,288 negative MQ RTs among 81,659 MQ rows (45.66%): the earlier statement that MQ negative RT is a majority must not be generalized to this CASPER sample. RPC/HTTP signed observations still retain their documented side meaning.
- Metric/resource service overlap is 1,301/1,301 metric services. Resource matching covers 1,057,824/1,058,011 metric container-minute keys. Known call-service overlap is 578/5,889 call services and 578/1,301 metric services. These are sample denominators, not global percentages. Full evidence is in `reports/data_quality/t06_sample_profile.json` and its Markdown summary.

### T07 — Frozen acquisition vs development scope

- Canonical seed 1217 selects exactly 200/1,999 CASPER partitions, 2,260,903,092 compressed bytes, using the same selection order as acquisition. Audited all 211 raw/sample entries with recomputed SHA-256; CASPER MD5s additionally match the declared Dataverse v1.0 metadata. The committed audit lists the selected cohort and per-file verification results.
- The five development call files are the first five lexicographically sorted acquired partitions, not a separate random cohort. OSS development selection is inclusive 0..600000 ms; it produces 11 minute buckets, the last partial for 30s inputs. Current raw OSS acquisition is archive 0 for each group; full 12-hour Resource/RTQps continuity is not accepted by this task.
- `docs/sampling_plan.md` now records actual scope, selection bias, checksum/reproduction steps, research-scale resource gates, and nested benchmark-input requirements. No further dataset downloads were launched.

### T10 — Native measurements, explicit minute projections

- `run_silver.py` implements Node/Resource/RTQps processing rather than a config-only stub. The prior stub status is superseded for T10; calls/trace T13/T14 and all later Gold/model stages remain separate work.
- Native tables retain 30s Node/Resource and 60s RTQps grain. Only explicit `*_minute` tables aggregate resource observations with floor(timestamp/60000), never mixing the next minute. Source row pointers and multiplicities survive duplicate collapse; every conflicting payload group is quarantined rather than averaged or arbitrarily selected.
- Invalid numeric observations remain visible with null canonical values/flags. HTTP metric values carry a suspect flag. Finite utilization above 1 is retained. A container's within-minute host set is preserved; ambiguity leaves minute-level host null rather than selecting a host. Missing matches remain in left-join diagnostics.
- Recording cadence is observed; exact RT/MCR measurement-window boundaries are not established by README/header agreement (`window_boundaries_verified=false`). T19 must validate the weighting contract before generating labels. T10 does not impute missing RT to zero or produce labels.
- Each table uses an atomic Iceberg replacement with a run-ID property. There is no multi-table transaction; downstream work requires the completed run report. Native/source row accounting, Iceberg readback counts, partial/invalid windows, and non-multiplying joins are checked. See `docs/silver_metrics_runbook.md` and measured acceptance reports for execution evidence.


### T10 — Measured acceptance and rerun (26 November 2026)

- Both complete sample executions passed Iceberg write/read and source accounting: Node 275,356 rows, Resource 2,023,518, RTQps 5,790,196; no quarantine rows in this sample. Native tables retain missing numeric flags and original lineage. Minute outputs have 144,234 Node rows and 1,059,938 Resource rows.
- Node has 13,112 partial time windows and 13,132 incomplete measurement windows; Resource has 96,358 partial and 96,448 incomplete measurement windows, with zero host-ambiguity windows in the sample. Missing numeric samples and partial endpoint buckets are not silently filled.
- Join diagnostics matched T06: 1,057,824/1,058,011 metric container-minute keys have resources; all 1,059,938 resource-minute rows match nodes. Both left joins preserve row counts (factor 1.0).
- Rerun preserved all eight table counts and join diagnostics. Independent DuckDB checks verified unique keys, every original source offset exactly once, and all minute averages within 1e-12. No byte-identical Parquet claim is made.
- Regression: **122/122 tests passed** on Linux Python 3.12.13. A new failure-path check ensures a replacement run invalidates an old `passed` report before catalog mutation and records failure instead of leaving stale success.
- Full measured evidence, source hashes, transformation commit, and run ID are in `reports/data_quality/t10_silver_acceptance.json` and its Markdown summary. This supersedes the earlier T10 config-only stub status on this branch. It does not mark T13/T14, Gold, models, or full-scale data processing complete.
