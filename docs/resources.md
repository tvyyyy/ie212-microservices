# Experimental Resources

Full resource measurements for the primary execution host (`T03`) are in progress. Pre-implementation environment audit as of **25 November 2026**:

## Host Role Assignment

- **Primary Execution Host (Vy):** Linux/Docker host (`>=16 GB RAM`, `>=100 GB free disk` — exact OS, physical CPU core count, RAM, and disk space to be logged in `T03`). Runs Docker Compose stack (`MinIO 9000/9001`, `Iceberg REST Catalog 8181`, `PySpark`), full 12-hour Bronze/Silver/Gold processing, RQ2 training, and all RQ3 benchmarks.
- **Development & Unit-Test Host (Huy — measured 25 Nov 2026):** Windows, Intel Core i5-12500H (12 cores / 16 threads), RTX 3050 Ti 4 GB, **7.73 GB RAM**, **38.57 GB free on D:** (`46.96 GB` on C:), Python 3.12 via `uv` (note: default Python 3.14 lacks `networkit` wheels; local WSL2/Docker unavailable). Used for modular development in `src/ie212/`, `pytest` fixtures, and small-sample validation.

## Software Compatibility Constraints

- **Python:** `3.11` or `3.12` (`pyproject.toml` restricts `>=3.11,<3.14`).
- **Java (Docker/Spark):** OpenJDK `17+`.
- **Spark & Iceberg:** Pin `pyspark==4.1.*` (`iceberg-spark-runtime-4.1_2.13`) or `pyspark==3.5.*` (`iceberg-spark-runtime-3.5_2.12`).

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).
