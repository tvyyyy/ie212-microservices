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
- **Primary Execution Host & Development Split:** Vy's machine (`>=16 GB RAM`, `>=100 GB free disk`) serves as the primary execution host running **Docker Linux** (`MinIO 9000/9001`, `Iceberg REST Catalog 8181`, `PySpark`); Huy's machine (`7.73 GB RAM`, `38.5 GB free disk`) focuses on modular code development, unit tests, and small-sample validation using **Python 3.12 (`uv`)**.
- **Data Acquisition & Sampling Strategy:**
  - `MSCallGraph`: Acquire 10–25% of trace-complete hash partitions from **Harvard Dataverse CASPER Shuffled v2021** (`doi:10.7910/DVN/RXIC9Z`, Huye et al., ACM/SPEC ICPE 2024) to preserve intact traces without cross-file splitting.
  - `Node`, `MSResource`, `MSRTQps`: Stream-download the full 12-hour archives from Alibaba OSS, record original checksums/manifests, convert immediately to Bronze Parquet, and remove temporary `.tar.gz` archives.
- **Live-Verified Schema Corrections (Alibaba OSS, 25 Nov 2026):**
  - All four raw CSV tables begin with an unnamed index column (`,...`).
  - Both `Node` and `MSResource` record at **30-second intervals** (`ts % 30000 == 0`), not 60s for `MSResource` as stated in the Alibaba README; `MSResource` places `timestamp` as the **last column** (`instance_cpu_usage`, `instance_memory_usage`). Both must be aligned 30s→60s before joining 60s `MSRTQps`.
  - `Node` vs. `MSResource` CPU/memory denominators are incompatible (Alibaba Issue #119); subtracting container CPU from node CPU is prohibited.
  - `MSRTQps` uses singular `metric`, is grouped in metric blocks (prohibiting `head(N)` sampling), and exhibits `HTTP_MCR == HTTP_RT` (Issue #120).
  - `MSCallGraph` header is `,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt` with six lowercase/camelCase `rpctype` values (`rpc`, `mc`, `http`, `userDefined`, `db`, `mq`), negative `mq` latencies, ~12% self-loops, ~17% sentinels, and `(traceid, rpcid)` multiplicities >2.
- **Locked Stack & Anti-Leakage Choices:**
  - Pin `pyspark==4.1.*` (matching Maven `iceberg-spark-runtime-4.1_2.13`) or `pyspark==3.5.*` (matching `3.5_2.12`) with Python 3.11/3.12 and OpenJDK 17+. Added `pyproject.toml` for `src/ie212/` editable installs.
  - Use **NetworKit** for RQ1 & Feature B centrality (`Degree`, `PageRank`, `EstimateBetweenness`, `CoreDecomposition`).
  - Use a dedicated **warm-up window** inside Train for the `train_only` graph snapshot to prevent intra-train leakage.
  - Use **DuckDB `iceberg_scan('s3://...')`** for RQ3 if validated in `T08`, with an explicit Parquet-export fallback (logging export cost separately).
  - Use **Streamlit** (`8501`) for `dashboard/` and **XeLaTeX** for compiling `docs/main.tex`.
