# Lakehouse Smoke Test Verification Runbook (`T08`)

Step-by-step verification guide for **Vy's Linux Docker host** covering MinIO (`pgsty/silo`), Iceberg REST Catalog (`apache/iceberg-rest-fixture`), PySpark `4.1.4` + Iceberg `1.11.0`, DuckDB `2.0.1` (`ATTACH ... TYPE iceberg` + S3 secret), and the Parquet export fallback.

The primary host is Windows 11 with Docker Desktop's Linux/WSL2 backend; commands below run against Linux containers. See `resources.md` for measured host and WSL limits. `scripts/run_silver.py` now implements T10 Node/Resource/RTQps normalization; see [Silver metrics runbook](silver_metrics_runbook.md). Calls/trace T13/T14 remain separate work.

> **Synthetic Data Notice:** `scripts/run_smoke_test.py` writes a 4-row **synthetic** test table (`ie212.silver.smoke_test_calls`) aligned with the 8 `MSCallGraph` columns (`traceid` values `smoke_t08_trace_001` / `smoke_t08_trace_002`). It is strictly isolated from Alibaba production/sample results.

---

## 1. Prerequisites & `.env` Setup (Vy Linux Host)

1. Confirm host baseline (`docs/resources.md`):
   - Linux host with **>= 16 GB RAM** and **>= 100 GB free disk**
   - Docker Engine + Docker Compose v2 (`docker compose version`)
2. Create `.env` from `.env.example` (do **not** commit `.env` to Git):

```bash
cp .env.example .env
```

3. Edit `.env` and set valid MinIO credentials:
   - `MINIO_ROOT_USER` (**>= 3 characters**, e.g. `ie212admin`)
   - `MINIO_ROOT_PASSWORD` (**>= 8 characters**)
   - Leave `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` blank to reuse root credentials, or set dedicated S3 keys.
   - Verify `.env` is ignored by Git:

```bash
git status --short .env
```

---

## 2. Build & Start the Lakehouse Stack

Start all services (`minio`, `minio-init`, `iceberg-rest-init`, `iceberg-rest`, `spark`):

```bash
docker compose up -d --build
docker compose ps
```

Expected status:
- `ie212-minio-init`: `Exited (0)`
- `ie212-iceberg-rest-init`: `Exited (0)`
- `ie212-minio`: `Up (healthy)`
- `ie212-iceberg-rest`: `Up (healthy)`
- `ie212-spark`: `Up`

If any init container exited non-zero, inspect its logs:

```bash
docker compose logs minio-init iceberg-rest-init iceberg-rest
```

---

## 3. Infrastructure Health & Permission Checks

### 3.1 MinIO Health & Buckets (`bronze`, `silver`, `gold`)

```bash
curl -fsS http://localhost:9000/minio/health/live && echo "MinIO OK"
docker compose logs minio-init
docker compose exec -T minio sh -c '/usr/bin/mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && /usr/bin/mc ls local'
```

Confirm `minio-init` created the buckets without error and `bronze/`, `silver/`, and `gold/` buckets are listed (or inspect MinIO Console at `http://localhost:9001`).

### 3.2 Iceberg REST Catalog `/v1/config`, `1000:1000` Ownership & SQLite WAL

```bash
curl -fsS http://localhost:8181/v1/config | python3 -m json.tool
docker exec ie212-iceberg-rest ls -la /data
```

Verify:
- `/v1/config` returns JSON with `"defaults"` (including `"warehouse": "s3://silver/warehouse"`).
- `/data` inside `ie212-iceberg-rest` is owned by `1000:1000` and contains `iceberg_catalog.db` (plus `-wal` / `-shm` files under SQLite WAL mode, no `SQLITE_CANTOPEN` errors).

### 3.3 Spark Container (`4.1.4`, Java 17, Python 3.12, Pre-baked Iceberg JARs)

```bash
docker compose exec spark id
docker compose exec spark java -version
docker compose exec spark python --version
docker compose exec spark sh -c 'echo HOME=$HOME && ls -la /home/spark/.duckdb'
docker compose exec spark ls -la /opt/spark/jars/iceberg-*.jar
```

Verify:
- `id` shows `uid=185(spark)` (`USER 185` from `Dockerfile`).
- `HOME=/home/spark` (`185:185`) and `/home/spark/.duckdb` contains pre-installed `httpfs` and `iceberg` extensions (`docs/resources.md`).
- `java -version` shows OpenJDK `17.x`.
- `python --version` shows `3.12.x`.
- `/opt/spark/jars/` contains `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` and `iceberg-aws-bundle-1.11.0.jar`, and **no** `hadoop-aws` JAR.

### 3.4 `USER 185` Volume Permission Note (`artifacts/lakehouse_fallback`)

`docker-compose.yml` bind-mounts `.:/app` from the Linux host (typically UID `1000`), while `ie212-spark` runs as `USER 185`. Before exporting fallback Parquet into `artifacts/lakehouse_fallback` from inside `ie212-spark`, ensure the directory is writable by UID `185`, or pass `--fallback-dir /tmp/ie212_fallback`:

```bash
mkdir -p artifacts/lakehouse_fallback
chmod a+rwx artifacts/lakehouse_fallback
```

---

## 4. Smoke Test Execution Checklist

### 4.1 Config & Dry-Run Plan Check (Zero Side Effects)

```bash
docker compose exec spark python scripts/run_smoke_test.py --check-config
docker compose exec spark python scripts/run_smoke_test.py --mode all --dry-run
```

Confirm:
- Exit code `0`
- `access_key` / `secret_key` appear strictly as `***REDACTED***`
- Spark config uses `org.apache.iceberg.aws.s3.S3FileIO` and omits `hadoop-aws` and `spark.jars.packages` (pre-baked JARs detected in `/opt/spark/jars`).

### 4.2 Spark Iceberg REST Write / Read Verification (`--mode spark`)

```bash
docker compose exec spark python scripts/run_smoke_test.py \
  --mode spark \
  --target container \
  --export-fallback
```

Expected JSON output fields:
- `"status": "passed"`
- `"row_count": 4`, `"distinct_traces": 2`, `"sum_rt": 15.25`, `"rpctype_count": 4`
- `"export_elapsed_seconds"` recorded separately from `"elapsed_seconds"` when `--export-fallback` is set.

### 4.3 DuckDB `2.0.1` Iceberg Verification (`--mode duckdb`)

Run inside `ie212-spark` (against `minio:9000` and `iceberg-rest:8181`):

```bash
docker compose exec spark python scripts/run_smoke_test.py \
  --mode duckdb \
  --target container
```

Or run on Vy's Linux host (with `.venv` active, targeting `localhost:9000` and `localhost:8181`):

```bash
python scripts/run_smoke_test.py --mode duckdb --target local
```

> **Note:** `ie212-spark` pre-installs DuckDB `httpfs` and `iceberg` extensions during image build under `HOME=/home/spark` (`185:185`), so in-container runs succeed via `LOAD httpfs;` / `LOAD iceberg;` with no outbound HTTPS required. When running `--target local` from a host `.venv`, DuckDB attempts `LOAD <ext>;` first and falls back to `INSTALL <ext>; LOAD <ext>;` on first run (requiring outbound HTTPS to DuckDB's extension repository).

### 4.4 Combined End-to-End Run (`--mode all`)

Strict mode (fails non-zero if DuckDB Iceberg `ATTACH`/read fails):

```bash
docker compose exec spark python scripts/run_smoke_test.py \
  --mode all \
  --target container \
  --export-fallback
```

With documented Parquet export fallback enabled (writes Spark snapshot to `--fallback-dir`, tries DuckDB Iceberg first, and falls back to `read_parquet()` if Iceberg `ATTACH`/read hits a compatibility limit):

```bash
docker compose exec spark python scripts/run_smoke_test.py \
  --mode all \
  --target container \
  --export-fallback \
  --fallback-on-error
```

### 4.5 Local Parquet Fallback Check (No Docker / No Credentials Required)

Runs on either Huy's Windows laptop (`.venv`) or Vy's host to verify DuckDB `2.0.1` + PyArrow `read_parquet()` round-trip:

```bash
python scripts/run_smoke_test.py --mode local --fallback-dir /tmp/ie212_smoke_local --cleanup-local
```

---

## 5. Parquet Export Fallback Procedure & Timing Rule

Per project specifications (`configs/benchmarks.yaml` `record_export_cost_separately: true` and `docs/decisions.md` architecture rules):

1. `--mode spark --export-fallback` writes a ZSTD/Snappy Parquet snapshot of `ie212.silver.smoke_test_calls` into `<fallback_dir>/smoke_test_calls/` and reports **`export_elapsed_seconds` separately** from Iceberg write/read `elapsed_seconds`.
2. `--mode duckdb --fallback-on-error` attempts native `ATTACH '' AS ie212 (TYPE iceberg, ENDPOINT '...', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none')` with `CREATE OR REPLACE SECRET minio_s3 (TYPE s3, ..., URL_STYLE 'path', USE_SSL false)` first.
3. Only if Iceberg `ATTACH` or S3 table read fails **and** `--fallback-on-error` is explicitly passed does `smoke_test.py` run DuckDB `read_parquet(...)` on `<fallback_dir>/smoke_test_calls`, recording:
   - `"fallback_used": true`
   - `"primary_error": "<redacted root cause>"`
   - `"fallback_elapsed_seconds"` separately from total/export cost.

---

## 6. Documented Engine Limits & Troubleshooting

| Engine / Component | Observed / Expected Limit | Mitigation / Runbook Action |
|---|---|---|
| **Huy Windows Host (Java 8)** | Host has `Java 1.8.0_511` JRE only and no Docker Engine; PySpark `4.1.4` + Iceberg `1.11.0` requires Java `17+`. | `smoke_test.py` checks `java -version` before starting JVM. Run `--mode local` on Windows; run `--mode spark`/`all` inside `ie212-spark` (OpenJDK 17). |
| **Spark `4.1.4` S3A vs `S3FileIO`** | Including `hadoop-aws` introduces unshaded `software.amazon.awssdk:bundle` conflicts against `iceberg-aws-bundle-1.11.0`. | `Dockerfile` omits `hadoop-aws`; `build_spark_iceberg_conf()` strictly forbids `hadoop-aws` / `software.amazon.awssdk:bundle` and configures `org.apache.iceberg.aws.s3.S3FileIO` with `s3.path-style-access=true`. |
| **Spark Env Var Passthrough** | `docker-compose.yml` passes `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (not `MINIO_ROOT_USER`) to `ie212-spark`. | `resolve_lakehouse_settings()` resolves `minio.access_key`/`secret_key` first and falls back to `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. |
| **DuckDB `2.0.1` Extensions** | `httpfs` and `iceberg` are pre-installed in `ie212-spark` under `HOME=/home/spark` (`185:185`); host `.venv` runs install missing extensions on first `LOAD`. | No internet needed inside `ie212-spark`; for host `.venv` `--target local`, ensure outbound HTTPS on first `LOAD`. |
| **DuckDB S3 + Iceberg REST** | MinIO requires `URL_STYLE 'path'`, `USE_SSL false`, and `host:port` `ENDPOINT`; REST `ATTACH` against `apache/iceberg-rest-fixture` (`JdbcCatalog`) uses `ACCESS_DELEGATION_MODE 'none'` (from `configs/lakehouse.yaml`) to rely on the local `minio_s3` secret instead of vended credentials, and may still hit catalog-prefix limits. | If `ATTACH` fails on live MinIO/Iceberg REST, pass `--export-fallback --fallback-on-error` (and `--attach-target s3://silver/warehouse` if needed), and log the exact `primary_error` in `docs/decisions.md`. |
| **Iceberg REST SQLite `/data`** | `apache/iceberg-rest-fixture` runs as UID `1000:1000`; un-chowned Docker named volumes trigger `SQLITE_CANTOPEN`. | `iceberg-rest-init` runs `chown -R 1000:1000 /data` before `iceberg-rest` starts. |
| **Spark Container UID `185`** | `ie212-spark` runs as `USER 185`; bind-mounted host folders owned by UID `1000` can reject Parquet fallback writes. | `chmod a+rwx artifacts/lakehouse_fallback` on host or pass `--fallback-dir /tmp/ie212_fallback`. |
