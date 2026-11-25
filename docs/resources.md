# Experimental Resources

Environment inventory and hardware constraints for the IE212 Alibaba Microservices Data Lakehouse project (`T03`, updated **25 November 2026**).

## Host Role Assignment

- **Primary Execution Host (Vy — Windows 11 + Docker Linux / WSL2):** Confirmed primary host to run the Docker Compose lakehouse stack (`MinIO 9000/9001`, `Iceberg REST Catalog 8181`, `PySpark 4.1.4`), full 12-hour Bronze/Silver/Gold pipelines, RQ1 graph centrality, RQ2 training/evaluation, and all RQ3 benchmarks.
- **Development & Unit-Test Host (Huy — Windows 11):** Used for modular pipeline development in `src/ie212/`, `pytest` unit suites, DuckDB local checks, and small-fixture idempotency validation in `uv` Python `3.12`. Full 38–41 GiB downloads and Docker/Spark containers are **not** executed on this host.

## Host Specifications

### Primary Execution Host (Vy)

Measured on Vy's ACER host on **25 November 2026**, confirmed by Vy during PR #3 acceptance. These are observations, not minimum-resource guarantees.

| Component | Exact Measurement |
|---|---|
| Device | Acer Aspire AL15-41P |
| Host OS | Windows 11 Home Single Language, version `10.0.26100` |
| Linux backend | WSL2, Ubuntu 24.04; kernel `6.6.87.2-microsoft-standard-WSL2`; Docker Desktop Linux context `desktop-linux` |
| CPU | AMD Ryzen 7 5700U with Radeon Graphics; 8 physical cores / 16 logical processors |
| Physical RAM | `16,519,819,264` bytes (`15.39 GiB`); OS-visible `15.39 GiB`; available at snapshot `3.88 GiB` |
| WSL memory | `7.4 GiB` total / `6.9 GiB` available; `2.0 GiB` swap at snapshot (Ubuntu `free -h`) |
| Storage | C: `275.42 GiB` total / `21.80 GiB` free; D: `200.00 GiB` total / `154.18 GiB` free at snapshot |
| Docker | Desktop `4.69.0`; CLI `29.4.0`; Compose `v5.1.1`; Linux Engine `29.4.0`, 16 CPUs, `7,998,730,240` bytes memory; startup/acceptance recorded in `decisions.md` |
| Acceptance Python | Separate ignored `artifacts/venv-pr3`, CPython `3.12.16`; existing `.venv` was preserved |

The host has nominal 16 GB RAM but only 15.39 GiB physically addressable and 7.4 GiB currently allocated to WSL. The Spark service's 8 GiB container limit exceeds that WSL memory allocation; this must be recorded as a constraint when interpreting acceptance or benchmark results. D: free space does not imply that Docker's backing virtual disk has the same free capacity; inspect Docker storage before full-data ingestion. Current acceptance uses small samples only.

### Development & Unit-Test Host (Huy — Measured 25 Nov 2026)

| Component | Measured Specification |
|---|---|
| Device Model | `LENOVO 82RC` |
| Operating System | `Microsoft Windows 11 Home Single Language`, Version `10.0.26300`, Build `26300` (`64-bit`) |
| CPU | `12th Gen Intel(R) Core(TM) i5-12500H` (`12` physical cores / `16` logical processors, max `2500 MHz`) |
| RAM | `7.73 GiB` OS-visible (`8.00 GiB` DDR5-4800 single DIMM; `~0.82 GiB` free at snapshot under active desktop load) |
| Storage | `SKHynix_HFS512GEJ9X115N` NVMe SSD (`476.94 GiB`); `C:` `239.97 GiB` total / `45.87 GiB` free (`NTFS`); `D:` `234.21 GiB` total / `38.55 GiB` free (`NTFS`) |
| GPU | `NVIDIA GeForce RTX 3050 Ti Laptop GPU` (`~4.00 GiB` VRAM, driver `32.0.16.2124`) + `Intel(R) UHD Graphics` (`~2.00 GiB`) |
| Python Tooling | `uv 0.11.15`; `Python 3.12.13` via `uv` (`uv venv --python 3.12`); global system `Python 3.14.10` (unused for project due to `networkit` wheel incompatibility) |
| Java / Docker | `Java 1.8.0_511` JRE only (no OpenJDK 17+); Docker CLI/Engine not installed on Windows (Ubuntu registered on WSL2) |
| Git / GitHub CLI | `git 2.56.0.windows.4`, `gh 2.103.3` |

## Software Compatibility Constraints

- **Python:** `3.11` or `3.12` (`pyproject.toml` enforces `>=3.11,<3.13`; `uv venv --python 3.12` verified locally on `3.12.13`).
- **Java (Docker/Spark):** OpenJDK `17` inside `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu`.
- **Lakehouse Runtime Stack:**
  - MinIO: `pgsty/silo:RELEASE.2026-11-12T00-00-00Z` (API `9000`, Console `9001`).
  - Iceberg REST Catalog: `apache/iceberg-rest-fixture@sha256:009f9f6897cc1e8fd6e8133c401c7a3e7c333c71293a621d06a71570ae989c29` (`8181`, SQLite WAL `/data/iceberg_catalog.db`).
  - Spark & Iceberg: `pyspark==4.1.4`, `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, `org.apache.iceberg:iceberg-aws-bundle:1.11.0` (no `hadoop-aws`).
  - DuckDB: `duckdb==2.0.1` (`httpfs` and `iceberg` extensions pre-installed in `ie212-spark` under `HOME=/home/spark`, UID `185:185`).
  - Core Runtime & Test Dependencies: `pandas>=2.2,<3.0`, `pyarrow>=18.0,<27,!=25.0.0`, `pyyaml==6.0.4`, `boto3==1.44.11`, `pytest==9.2.0`.

Sources: Live Windows CIM/CLI measurements (25 Nov 2026) and external workspace context specifications (`metadata.txt` and `agents_ie212.md`).
