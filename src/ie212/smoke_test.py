"""Iceberg REST Catalog (Spark 4.1.4) and DuckDB (2.0.1) lakehouse smoke tests (T08)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ie212.config import ConfigError, parse_dotenv, validate_lakehouse_config

DEFAULT_NAMESPACE = "silver"
DEFAULT_TABLE = "smoke_test_calls"
REDACTED_VALUE = "***REDACTED***"

EXPECTED_COLUMNS: tuple[str, ...] = (
    "traceid",
    "timestamp",
    "rpcid",
    "um",
    "rpctype",
    "dm",
    "interface",
    "rt",
)

# Deterministic synthetic smoke-test rows aligned with the 8 MSCallGraph columns.
# Never mixed with Alibaba production/sample results.
SMOKE_TEST_ROWS: tuple[tuple[str, int, str, str, str, str, str, float], ...] = (
    ("smoke_t08_trace_001", 1700000000000, "0.1", "svc_gateway", "http", "svc_auth", "/v1/token", 12.5),
    ("smoke_t08_trace_001", 1700000000005, "0.1.1", "svc_auth", "rpc", "svc_user", "GetProfile", 4.25),
    ("smoke_t08_trace_002", 1700000060000, "0.1", "svc_order", "db", "mysql_orders", "InsertOrder", 0.0),
    ("smoke_t08_trace_002", 1700000060010, "0.2", "svc_order", "mq", "kafka_events", "OrderCreated", -1.5),
)

EXPECTED_ROW_COUNT = 4
EXPECTED_DISTINCT_TRACES = 2
EXPECTED_SUM_RT = 15.25
EXPECTED_RPCTYPE_COUNT = 4

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FORBIDDEN_PACKAGE_SUBSTRINGS = ("hadoop-aws", "software.amazon.awssdk:bundle")
_SECRET_CONF_KEY_PATTERNS = ("secret", "access-key", "access_key", "password", "token")
_TRUE_BOOL_STRINGS = frozenset({"true", "1", "yes", "on"})
_FALSE_BOOL_STRINGS = frozenset({"false", "0", "no", "off"})
CREDENTIAL_ENV_KEYS: tuple[str, ...] = (
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)


class SmokeTestError(RuntimeError):
    """Base error for T08 smoke test failures."""


class SmokeTestConfigError(ConfigError, SmokeTestError):
    """Raised when smoke test configuration, credentials, or SQL identifiers are invalid."""


class SmokeTestRuntimeError(SmokeTestError):
    """Raised when smoke test execution or verification invariants fail."""


def validate_sql_identifier(value: str, *, label: str = "identifier") -> str:
    """Validate a SQL identifier against ^[A-Za-z_][A-Za-z0-9_]*$."""
    if not isinstance(value, str) or not _SQL_IDENTIFIER_RE.fullmatch(value):
        raise SmokeTestConfigError(
            f"Invalid SQL {label} {value!r}: must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )
    return value


def escape_sql_literal(value: str) -> str:
    """Escape single quotes in a SQL string literal."""
    if not isinstance(value, str):
        raise SmokeTestConfigError(f"SQL literal must be a string, got {type(value).__name__}")
    return value.replace("'", "''")


def strip_url_scheme_for_s3_endpoint(endpoint: str) -> tuple[str, bool]:
    """Strip http(s):// and trailing slashes for DuckDB S3 ENDPOINT, returning (host_port, use_ssl)."""
    cleaned = endpoint.strip().rstrip("/")
    lower = cleaned.lower()
    if lower.startswith("https://"):
        return cleaned[8:].rstrip("/"), True
    if lower.startswith("http://"):
        return cleaned[7:].rstrip("/"), False
    return cleaned, False


def redact_value(value: str | None) -> str:
    """Return a redacted placeholder when value is non-empty."""
    return REDACTED_VALUE if value else ""


def _coerce_bool(value: Any, field_name: str) -> bool:
    """Strictly parse a boolean or boolean-like string configuration value."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_BOOL_STRINGS:
            return True
        if normalized in _FALSE_BOOL_STRINGS:
            return False
        raise SmokeTestConfigError(
            f"Invalid boolean value for {field_name}: {value!r} "
            "(expected true/false, 1/0, yes/no, or on/off)"
        )
    raise SmokeTestConfigError(
        f"Invalid boolean type for {field_name}: expected bool or str, got {type(value).__name__}"
    )


def _secret_variants(
    settings: LakehouseConnectionSettings | None = None,
    *,
    extra_secrets: Iterable[str] = (),
) -> list[str]:
    """Collect raw and SQL-escaped credential variants, ordered longest-first."""
    candidates: list[str] = []
    raw_values: list[str] = [s for s in extra_secrets if isinstance(s, str) and s]
    if settings is not None:
        raw_values.extend([settings.access_key, settings.secret_key])
    for value in raw_values:
        if value:
            candidates.append(value)
            escaped = escape_sql_literal(value)
            if escaped != value:
                candidates.append(escaped)
    return sorted(set(candidates), key=len, reverse=True)


def collect_runtime_secrets(
    settings: LakehouseConnectionSettings | None = None,
    *,
    lakehouse_cfg: Mapping[str, Any] | None = None,
    dotenv_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[str]:
    """Collect raw and SQL-escaped secrets from settings, config, .env, and environment."""
    env = os.environ if environ is None else environ
    extras: list[str] = []
    if isinstance(lakehouse_cfg, Mapping):
        minio_section = lakehouse_cfg.get("minio")
        if isinstance(minio_section, Mapping):
            for key in ("access_key", "secret_key"):
                val = minio_section.get(key)
                if isinstance(val, str) and val:
                    extras.append(val)
    if dotenv_path is not None:
        try:
            dotenv_vars = parse_dotenv(dotenv_path)
        except OSError:
            dotenv_vars = {}
        for key in CREDENTIAL_ENV_KEYS:
            val = dotenv_vars.get(key, "")
            if val:
                extras.append(val)
    for key in CREDENTIAL_ENV_KEYS:
        val = env.get(key, "")
        if val:
            extras.append(val)
    return _secret_variants(settings, extra_secrets=extras)


def sanitize_secret_text(text: str, secrets: Iterable[str]) -> str:
    """Scrub any non-empty secret strings from error or log messages (longest-first)."""
    ordered = sorted({s for s in secrets if s}, key=len, reverse=True)
    if not ordered or not text:
        return text
    pattern = re.compile("|".join(re.escape(s) for s in ordered))
    return pattern.sub(lambda _match: REDACTED_VALUE, text)


def redact_spark_conf(conf: Mapping[str, str]) -> dict[str, str]:
    """Return a copy of Spark configuration with sensitive values redacted."""
    redacted: dict[str, str] = {}
    for key, val in conf.items():
        lower_key = key.lower()
        if any(pattern in lower_key for pattern in _SECRET_CONF_KEY_PATTERNS):
            redacted[key] = redact_value(val)
        else:
            redacted[key] = val
    return redacted


@dataclass(frozen=True)
class LakehouseConnectionSettings:
    """Resolved MinIO, Iceberg REST, Spark, and DuckDB connection settings."""

    target: str
    minio_endpoint: str
    minio_s3_endpoint_host_port: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    region: str
    path_style_access: bool
    use_ssl: bool
    buckets: tuple[str, ...]
    catalog_name: str
    catalog_type: str
    catalog_uri: str
    warehouse: str
    s3_file_io: str
    namespaces: tuple[str, ...]
    spark_master: str
    spark_app_name: str
    driver_memory: str
    executor_memory: str
    iceberg_packages: tuple[str, ...]
    exclude_hadoop_aws: bool
    duckdb_s3_secret_name: str
    duckdb_attach_type: str
    duckdb_authorization_type: str
    duckdb_url_style: str
    parquet_fallback_dir: str
    duckdb_access_delegation_mode: str = "none"

    def to_redacted_dict(self) -> dict[str, Any]:
        """Serialize settings with credentials strictly redacted."""
        data = asdict(self)
        data["access_key"] = redact_value(self.access_key)
        data["secret_key"] = redact_value(self.secret_key)
        data["buckets"] = list(self.buckets)
        data["namespaces"] = list(self.namespaces)
        data["iceberg_packages"] = list(self.iceberg_packages)
        return data


@dataclass
class SmokeTestResult:
    """Structured JSON-serializable result from a smoke test stage."""

    mode: str
    status: str
    engine: str
    row_count: int | None = None
    distinct_traces: int | None = None
    sum_rt: float | None = None
    rpctype_count: int | None = None
    columns: list[str] | None = None
    elapsed_seconds: float | None = None
    export_elapsed_seconds: float | None = None
    fallback_used: bool = False
    fallback_elapsed_seconds: float | None = None
    fallback_parquet_path: str | None = None
    primary_error: str | None = None
    engine_limits: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert result into a JSON-serializable dictionary."""
        return asdict(self)


def normalize_target(target: str) -> str:
    """Normalize target selector ('docker' -> 'container', 'container', 'local')."""
    normalized = target.strip().lower()
    if normalized in ("container", "docker"):
        return "container"
    if normalized == "local":
        return "local"
    raise SmokeTestConfigError(
        f"Invalid target {target!r}: expected 'container', 'docker', or 'local'"
    )


def resolve_lakehouse_settings(
    lakehouse_cfg: Mapping[str, Any],
    *,
    target: str = "container",
    environ: Mapping[str, str] | None = None,
    require_credentials: bool = False,
) -> LakehouseConnectionSettings:
    """Validate and resolve lakehouse settings for container or host-local execution."""
    validate_lakehouse_config(lakehouse_cfg)
    resolved_target = normalize_target(target)
    env = os.environ if environ is None else environ

    minio = lakehouse_cfg["minio"]
    iceberg = lakehouse_cfg["iceberg"]
    spark = lakehouse_cfg["spark"]
    duckdb_cfg = lakehouse_cfg["duckdb"]

    if resolved_target == "container":
        raw_minio_endpoint = str(minio.get("endpoint") or "").strip()
        raw_catalog_uri = str(iceberg.get("catalog_uri") or "").strip()
    else:
        raw_minio_endpoint = str(minio.get("local_endpoint") or minio.get("endpoint") or "").strip()
        raw_catalog_uri = str(
            iceberg.get("local_catalog_uri") or iceberg.get("catalog_uri") or ""
        ).strip()

    if not raw_minio_endpoint:
        raise SmokeTestConfigError("MinIO endpoint must be non-empty")
    if not raw_catalog_uri:
        raise SmokeTestConfigError("Iceberg catalog URI must be non-empty")

    warehouse = str(iceberg.get("warehouse") or "").strip()
    if not warehouse:
        raise SmokeTestConfigError("Iceberg warehouse must be non-empty")

    host_port, inferred_ssl = strip_url_scheme_for_s3_endpoint(raw_minio_endpoint)
    if not host_port:
        raise SmokeTestConfigError(f"Invalid MinIO endpoint: {raw_minio_endpoint!r}")

    access_key = (
        str(minio.get("access_key") or "").strip()
        or str(env.get("AWS_ACCESS_KEY_ID") or "").strip()
        or str(env.get("MINIO_ACCESS_KEY") or "").strip()
        or str(env.get("MINIO_ROOT_USER") or "").strip()
    )
    secret_key = (
        str(minio.get("secret_key") or "").strip()
        or str(env.get("AWS_SECRET_ACCESS_KEY") or "").strip()
        or str(env.get("MINIO_SECRET_KEY") or "").strip()
        or str(env.get("MINIO_ROOT_PASSWORD") or "").strip()
    )

    if require_credentials and (not access_key or not secret_key):
        raise SmokeTestConfigError(
            "Missing MinIO/S3 credentials for live lakehouse access. Copy .env.example to .env "
            "and set MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)."
        )

    catalog_name = validate_sql_identifier(
        str(iceberg.get("catalog_name") or ""), label="catalog_name"
    )
    duckdb_s3_secret_name = validate_sql_identifier(
        str(duckdb_cfg.get("s3_secret_name") or "minio_s3"), label="s3_secret_name"
    )
    duckdb_attach_type = validate_sql_identifier(
        str(duckdb_cfg.get("attach_type") or "iceberg"), label="attach_type"
    )
    duckdb_authorization_type = validate_sql_identifier(
        str(duckdb_cfg.get("authorization_type") or "none"), label="authorization_type"
    )
    duckdb_url_style = str(duckdb_cfg.get("url_style") or "path").strip().lower()
    if duckdb_url_style not in ("path", "vhost"):
        raise SmokeTestConfigError(
            f"Invalid DuckDB url_style {duckdb_url_style!r}: expected 'path' or 'vhost'"
        )
    raw_access_delegation_mode = duckdb_cfg.get("access_delegation_mode")
    if raw_access_delegation_mode is None:
        duckdb_access_delegation_mode = "none"
    elif (
        not isinstance(raw_access_delegation_mode, str)
        or not raw_access_delegation_mode.strip()
    ):
        raise SmokeTestConfigError(
            "Invalid DuckDB access_delegation_mode: expected 'none' or 'vended_credentials'"
        )
    else:
        duckdb_access_delegation_mode = raw_access_delegation_mode.strip().lower()
    if duckdb_access_delegation_mode not in ("none", "vended_credentials"):
        raise SmokeTestConfigError(
            f"Invalid DuckDB access_delegation_mode {duckdb_access_delegation_mode!r}: "
            "expected 'none' or 'vended_credentials'"
        )

    use_ssl_cfg = minio.get("use_ssl")
    if use_ssl_cfg is None:
        use_ssl = inferred_ssl
    else:
        use_ssl = _coerce_bool(use_ssl_cfg, "lakehouse.minio.use_ssl") or inferred_ssl

    path_style_access = _coerce_bool(
        minio.get("path_style_access", True), "lakehouse.minio.path_style_access"
    )
    exclude_hadoop_aws = _coerce_bool(
        spark.get("exclude_hadoop_aws", True), "lakehouse.spark.exclude_hadoop_aws"
    )

    return LakehouseConnectionSettings(
        target=resolved_target,
        minio_endpoint=raw_minio_endpoint.rstrip("/"),
        minio_s3_endpoint_host_port=host_port,
        access_key=access_key,
        secret_key=secret_key,
        region=str(minio.get("region") or env.get("AWS_REGION") or "us-east-1").strip(),
        path_style_access=path_style_access,
        use_ssl=use_ssl,
        buckets=tuple(str(b) for b in minio.get("buckets", ())),
        catalog_name=catalog_name,
        catalog_type=str(iceberg.get("catalog_type") or "rest").strip(),
        catalog_uri=raw_catalog_uri.rstrip("/"),
        warehouse=warehouse,
        s3_file_io=str(
            iceberg.get("s3_file_io") or "org.apache.iceberg.aws.s3.S3FileIO"
        ).strip(),
        namespaces=tuple(str(ns) for ns in iceberg.get("namespaces", ())),
        spark_master=str(spark.get("master") or "local[*]").strip(),
        spark_app_name=str(spark.get("app_name") or "ie212-lakehouse").strip(),
        driver_memory=str(spark.get("driver_memory") or "2g").strip(),
        executor_memory=str(spark.get("executor_memory") or "2g").strip(),
        iceberg_packages=tuple(str(pkg) for pkg in spark.get("iceberg_packages", ())),
        exclude_hadoop_aws=exclude_hadoop_aws,
        duckdb_s3_secret_name=duckdb_s3_secret_name,
        duckdb_attach_type=duckdb_attach_type,
        duckdb_authorization_type=duckdb_authorization_type,
        duckdb_url_style=duckdb_url_style,
        parquet_fallback_dir=str(
            duckdb_cfg.get("parquet_fallback_dir") or "artifacts/lakehouse_fallback"
        ).strip(),
        duckdb_access_delegation_mode=duckdb_access_delegation_mode,
    )


def parse_java_major_version(version_output: str) -> int | None:
    """Parse Java major version from `java -version` output (e.g., 1.8.0_511 -> 8, 17.0.12 -> 17)."""
    match = re.search(r'version\s+"([^"]+)"', version_output)
    if not match:
        return None
    raw_version = match.group(1).strip()
    if raw_version.startswith("1."):
        parts = raw_version.split(".")
        if len(parts) >= 2 and parts[1].isdigit():
            return int(parts[1])
        return None
    head = re.split(r"[._+-]", raw_version, maxsplit=1)[0]
    return int(head) if head.isdigit() else None


def detect_java_major_version() -> int | None:
    """Detect the host JVM major version by invoking `java -version`."""
    try:
        completed = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    combined = "\n".join(part for part in (completed.stderr, completed.stdout) if part)
    return parse_java_major_version(combined)


def check_java_for_spark(min_major: int = 17) -> int:
    """Verify Java >= 17 before launching the Spark 4.1.4 JVM."""
    major = detect_java_major_version()
    if major is None:
        raise SmokeTestRuntimeError(
            "Java runtime not found; PySpark 4.1.4 + Iceberg 1.11.0 requires Java 17+. "
            "Run `--mode spark` inside the Docker Spark container (`docker compose exec spark ...`)."
        )
    if major < min_major:
        raise SmokeTestRuntimeError(
            f"Detected Java {major}, but PySpark 4.1.4 + Iceberg 1.11.0 requires Java {min_major}+. "
            "Run `--mode spark` inside `docker compose exec spark ...` on the Docker host, "
            "or use `--mode local` on this host."
        )
    return major


def has_prebaked_iceberg_jars(jars_dir: str | Path = "/opt/spark/jars") -> bool:
    """Return True when both Iceberg Spark runtime and AWS bundle JARs exist in jars_dir."""
    directory = Path(jars_dir)
    if not directory.is_dir():
        return False
    has_runtime = any(p.is_file() for p in directory.glob("iceberg-spark-runtime-*.jar"))
    has_aws_bundle = any(p.is_file() for p in directory.glob("iceberg-aws-bundle-*.jar"))
    return has_runtime and has_aws_bundle


def build_spark_iceberg_conf(
    settings: LakehouseConnectionSettings,
    *,
    include_packages: bool | None = None,
    include_credentials: bool = True,
    jars_dir: str | Path = "/opt/spark/jars",
) -> dict[str, str]:
    """Build Spark 4.1.4 + Iceberg REST Catalog + S3FileIO configuration."""
    if settings.exclude_hadoop_aws:
        for pkg in settings.iceberg_packages:
            lower_pkg = pkg.lower()
            for forbidden in _FORBIDDEN_PACKAGE_SUBSTRINGS:
                if forbidden in lower_pkg:
                    raise SmokeTestConfigError(
                        f"Forbidden Spark package {pkg!r} when exclude_hadoop_aws=True "
                        "(must not include hadoop-aws or software.amazon.awssdk:bundle)."
                    )

    cat = validate_sql_identifier(settings.catalog_name, label="catalog_name")
    conf: dict[str, str] = {
        "spark.app.name": f"{settings.spark_app_name}-smoke-test",
        "spark.master": settings.spark_master,
        "spark.driver.memory": settings.driver_memory,
        "spark.executor.memory": settings.executor_memory,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": cat,
        f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{cat}.type": settings.catalog_type or "rest",
        f"spark.sql.catalog.{cat}.uri": settings.catalog_uri,
        f"spark.sql.catalog.{cat}.warehouse": settings.warehouse,
        f"spark.sql.catalog.{cat}.io-impl": settings.s3_file_io,
        f"spark.sql.catalog.{cat}.s3.endpoint": settings.minio_endpoint,
        f"spark.sql.catalog.{cat}.s3.path-style-access": str(settings.path_style_access).lower(),
        f"spark.sql.catalog.{cat}.client.region": settings.region,
    }

    if include_credentials:
        if settings.access_key:
            conf[f"spark.sql.catalog.{cat}.s3.access-key-id"] = settings.access_key
        if settings.secret_key:
            conf[f"spark.sql.catalog.{cat}.s3.secret-access-key"] = settings.secret_key

    should_include_packages = (
        (not has_prebaked_iceberg_jars(jars_dir))
        if include_packages is None
        else include_packages
    )
    if should_include_packages and settings.iceberg_packages:
        conf["spark.jars.packages"] = ",".join(settings.iceberg_packages)

    return conf


def build_duckdb_s3_secret_sql(
    settings: LakehouseConnectionSettings,
    *,
    redact: bool = False,
) -> str:
    """Build DuckDB CREATE OR REPLACE SECRET SQL for MinIO path-style S3 access."""
    secret_name = validate_sql_identifier(settings.duckdb_s3_secret_name, label="s3_secret_name")
    url_style = settings.duckdb_url_style
    if url_style not in ("path", "vhost"):
        raise SmokeTestConfigError(
            f"Invalid DuckDB url_style {url_style!r}: expected 'path' or 'vhost'"
        )
    key_id = REDACTED_VALUE if redact else settings.access_key
    secret = REDACTED_VALUE if redact else settings.secret_key
    use_ssl_sql = "true" if settings.use_ssl else "false"
    return (
        f"CREATE OR REPLACE SECRET {secret_name} (\n"
        f"    TYPE s3,\n"
        f"    KEY_ID '{escape_sql_literal(key_id)}',\n"
        f"    SECRET '{escape_sql_literal(secret)}',\n"
        f"    REGION '{escape_sql_literal(settings.region)}',\n"
        f"    ENDPOINT '{escape_sql_literal(settings.minio_s3_endpoint_host_port)}',\n"
        f"    URL_STYLE '{escape_sql_literal(url_style)}',\n"
        f"    USE_SSL {use_ssl_sql}\n"
        f");"
    )


def build_duckdb_attach_sql(
    settings: LakehouseConnectionSettings,
    *,
    alias: str | None = None,
    attach_target: str = "",
) -> str:
    """Build DuckDB ATTACH SQL for Iceberg REST Catalog."""
    catalog_alias = validate_sql_identifier(alias or settings.catalog_name, label="catalog_alias")
    attach_type = validate_sql_identifier(settings.duckdb_attach_type, label="attach_type")
    auth_type = validate_sql_identifier(
        settings.duckdb_authorization_type, label="authorization_type"
    )
    raw_adm = settings.duckdb_access_delegation_mode
    if not isinstance(raw_adm, str):
        raise SmokeTestConfigError(
            f"Invalid DuckDB access_delegation_mode type: {type(raw_adm).__name__}"
        )
    access_delegation_mode = raw_adm.strip().lower()
    if access_delegation_mode not in ("none", "vended_credentials"):
        raise SmokeTestConfigError(
            f"Invalid DuckDB access_delegation_mode {raw_adm!r}: "
            "expected 'none' or 'vended_credentials'"
        )
    return (
        f"ATTACH '{escape_sql_literal(attach_target)}' AS {catalog_alias} (\n"
        f"    TYPE {attach_type},\n"
        f"    ENDPOINT '{escape_sql_literal(settings.catalog_uri)}',\n"
        f"    AUTHORIZATION_TYPE {auth_type},\n"
        f"    ACCESS_DELEGATION_MODE '{escape_sql_literal(access_delegation_mode)}'\n"
        f");"
    )


def get_smoke_test_arrow_table() -> pa.Table:
    """Construct the deterministic 4-row MSCallGraph-aligned Arrow table."""
    schema = pa.schema(
        [
            pa.field("traceid", pa.string(), nullable=False),
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("rpcid", pa.string(), nullable=False),
            pa.field("um", pa.string(), nullable=False),
            pa.field("rpctype", pa.string(), nullable=False),
            pa.field("dm", pa.string(), nullable=False),
            pa.field("interface", pa.string(), nullable=False),
            pa.field("rt", pa.float64(), nullable=False),
        ]
    )
    columns_data = {
        col: [row[idx] for row in SMOKE_TEST_ROWS]
        for idx, col in enumerate(EXPECTED_COLUMNS)
    }
    return pa.Table.from_pydict(columns_data, schema=schema)


def _verify_invariants(
    *,
    row_count: int,
    distinct_traces: int,
    sum_rt: float,
    rpctype_count: int,
    columns: list[str],
    context: str,
) -> None:
    if columns != list(EXPECTED_COLUMNS):
        raise SmokeTestRuntimeError(
            f"{context}: schema mismatch; expected {list(EXPECTED_COLUMNS)}, got {columns}"
        )
    if int(row_count) != EXPECTED_ROW_COUNT:
        raise SmokeTestRuntimeError(
            f"{context}: expected row_count={EXPECTED_ROW_COUNT}, got {row_count}"
        )
    if int(distinct_traces) != EXPECTED_DISTINCT_TRACES:
        raise SmokeTestRuntimeError(
            f"{context}: expected distinct_traces={EXPECTED_DISTINCT_TRACES}, got {distinct_traces}"
        )
    if abs(float(sum_rt) - EXPECTED_SUM_RT) > 1e-6:
        raise SmokeTestRuntimeError(
            f"{context}: expected sum_rt={EXPECTED_SUM_RT}, got {sum_rt}"
        )
    if int(rpctype_count) != EXPECTED_RPCTYPE_COUNT:
        raise SmokeTestRuntimeError(
            f"{context}: expected rpctype_count={EXPECTED_RPCTYPE_COUNT}, got {rpctype_count}"
        )


def _resolve_parquet_read_target(parquet_source: Path) -> str:
    """Return a DuckDB read_parquet path/glob supporting either single file or Spark directory."""
    if parquet_source.is_dir():
        parquet_files = sorted(parquet_source.glob("*.parquet"))
        if not parquet_files:
            raise SmokeTestRuntimeError(
                f"No .parquet files found inside fallback directory: {parquet_source}"
            )
        return (parquet_source / "*.parquet").as_posix()
    if not parquet_source.is_file():
        raise SmokeTestRuntimeError(f"Fallback Parquet file not found: {parquet_source}")
    return parquet_source.as_posix()


def _query_duckdb_parquet_metrics(conn: Any, parquet_source: Path) -> dict[str, Any]:
    """Query a Parquet file or directory via DuckDB read_parquet and validate invariants."""
    read_target = escape_sql_literal(_resolve_parquet_read_target(parquet_source))
    describe_rows = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{read_target}')"
    ).fetchall()
    columns = [str(row[0]) for row in describe_rows]
    row = conn.execute(
        f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT traceid) AS distinct_traces, "
        f"SUM(rt) AS sum_rt, COUNT(DISTINCT rpctype) AS rpctype_count "
        f"FROM read_parquet('{read_target}')"
    ).fetchone()
    if row is None:
        raise SmokeTestRuntimeError(f"DuckDB read_parquet returned no rows for {parquet_source}")

    row_count, distinct_traces, sum_rt, rpctype_count = row
    _verify_invariants(
        row_count=int(row_count),
        distinct_traces=int(distinct_traces),
        sum_rt=float(sum_rt),
        rpctype_count=int(rpctype_count),
        columns=columns,
        context=f"DuckDB read_parquet({parquet_source})",
    )
    return {
        "row_count": int(row_count),
        "distinct_traces": int(distinct_traces),
        "sum_rt": float(sum_rt),
        "rpctype_count": int(rpctype_count),
        "columns": columns,
    }


def _find_fallback_parquet_source(fallback_dir: Path, table: str) -> Path | None:
    """Find <fallback_dir>/<table> directory (preferred) or <fallback_dir>/<table>.parquet."""
    dir_target = fallback_dir / table
    if dir_target.is_dir() and any(p.is_file() for p in dir_target.glob("*.parquet")):
        return dir_target
    single_file = fallback_dir / f"{table}.parquet"
    if single_file.is_file():
        return single_file
    return None


def run_local_parquet_smoke_test(
    fallback_dir: str | Path,
    *,
    namespace: str = DEFAULT_NAMESPACE,
    table: str = DEFAULT_TABLE,
    cleanup: bool = False,
    settings: LakehouseConnectionSettings | None = None,
    dry_run: bool = False,
) -> SmokeTestResult:
    """Run local DuckDB 2.0.1 + PyArrow Parquet write/read smoke verification (no Docker/Java)."""
    valid_ns = validate_sql_identifier(namespace, label="namespace")
    valid_table = validate_sql_identifier(table, label="table")
    target_dir = Path(fallback_dir)
    parquet_path = target_dir / f"{valid_table}.parquet"

    if dry_run:
        details: dict[str, Any] = {
            "namespace": valid_ns,
            "parquet_path": str(parquet_path),
            "compression": "zstd",
            "expected_columns": list(EXPECTED_COLUMNS),
            "expected_row_count": EXPECTED_ROW_COUNT,
            "expected_sum_rt": EXPECTED_SUM_RT,
        }
        if settings is not None:
            details["s3_secret_sql_preview"] = build_duckdb_s3_secret_sql(settings, redact=True)
            details["attach_sql_preview"] = build_duckdb_attach_sql(settings)
        return SmokeTestResult(
            mode="local",
            status="dry_run",
            engine="duckdb-local-parquet",
            details=details,
        )

    start = time.perf_counter()
    target_dir.mkdir(parents=True, exist_ok=True)
    arrow_table = get_smoke_test_arrow_table()
    pq.write_table(arrow_table, parquet_path, compression="zstd")

    conn = duckdb.connect(":memory:")
    try:
        metrics = _query_duckdb_parquet_metrics(conn, parquet_path)
    finally:
        conn.close()

    elapsed = round(time.perf_counter() - start, 6)
    details = {
        "namespace": valid_ns,
        "duckdb_version": duckdb.__version__,
        "pyarrow_version": pa.__version__,
        "cleanup_performed": bool(cleanup),
        "synthetic_dataset": True,
    }
    if settings is not None:
        details["s3_secret_sql_preview"] = build_duckdb_s3_secret_sql(settings, redact=True)
        details["attach_sql_preview"] = build_duckdb_attach_sql(settings)

    result_path: str | None = str(parquet_path)
    if cleanup:
        if parquet_path.exists():
            parquet_path.unlink()
        if target_dir.exists() and not any(target_dir.iterdir()):
            target_dir.rmdir()
        result_path = None

    return SmokeTestResult(
        mode="local",
        status="passed",
        engine="duckdb-local-parquet",
        row_count=metrics["row_count"],
        distinct_traces=metrics["distinct_traces"],
        sum_rt=metrics["sum_rt"],
        rpctype_count=metrics["rpctype_count"],
        columns=metrics["columns"],
        elapsed_seconds=elapsed,
        fallback_used=False,
        fallback_parquet_path=result_path,
        details=details,
    )


def _create_default_spark_session(spark_conf: Mapping[str, str]) -> Any:
    """Instantiate a SparkSession from spark_conf."""
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()
    for key, val in spark_conf.items():
        conf.set(key, val)
    return SparkSession.builder.config(conf=conf).getOrCreate()


def run_spark_smoke_test(
    settings: LakehouseConnectionSettings,
    *,
    namespace: str = DEFAULT_NAMESPACE,
    table: str = DEFAULT_TABLE,
    fallback_dir: str | Path | None = None,
    export_fallback: bool = False,
    dry_run: bool = False,
    spark_session_factory: Callable[[Mapping[str, str]], Any] | None = None,
    skip_java_check: bool = False,
) -> SmokeTestResult:
    """Write and read back an Iceberg table via Spark 4.1.4 + Iceberg REST Catalog."""
    valid_ns = validate_sql_identifier(namespace, label="namespace")
    valid_table = validate_sql_identifier(table, label="table")
    full_table = f"{settings.catalog_name}.{valid_ns}.{valid_table}"
    dest_dir = Path(fallback_dir or settings.parquet_fallback_dir)
    spark_conf = build_spark_iceberg_conf(settings)

    if dry_run:
        return SmokeTestResult(
            mode="spark",
            status="dry_run",
            engine="spark-iceberg-rest",
            details={
                "target": settings.target,
                "target_table": full_table,
                "spark_conf": redact_spark_conf(spark_conf),
                "export_fallback": export_fallback,
                "fallback_dir": str(dest_dir),
                "prebaked_jars_detected": has_prebaked_iceberg_jars(),
            },
        )

    if not settings.access_key or not settings.secret_key:
        raise SmokeTestConfigError(
            "Missing MinIO/S3 credentials for Spark Iceberg smoke test. "
            "Set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD in .env or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        )

    if not skip_java_check and spark_session_factory is None:
        check_java_for_spark(17)

    start = time.perf_counter()
    env_keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")
    previous_env = {k: os.environ.get(k) for k in env_keys}
    os.environ["AWS_ACCESS_KEY_ID"] = settings.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.secret_key
    os.environ["AWS_REGION"] = settings.region

    spark = None
    export_elapsed: float | None = None
    export_path: str | None = None
    try:
        factory = spark_session_factory or _create_default_spark_session
        spark = factory(spark_conf)

        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("traceid", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("rpcid", StringType(), False),
                StructField("um", StringType(), False),
                StructField("rpctype", StringType(), False),
                StructField("dm", StringType(), False),
                StructField("interface", StringType(), False),
                StructField("rt", DoubleType(), False),
            ]
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.catalog_name}.{valid_ns}")
        df = spark.createDataFrame(list(SMOKE_TEST_ROWS), schema=schema)
        df.writeTo(full_table).using("iceberg").createOrReplace()

        read_df = spark.table(full_table)
        read_columns = list(read_df.columns)
        agg_rows = spark.sql(
            f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT traceid) AS distinct_traces, "
            f"SUM(rt) AS sum_rt, COUNT(DISTINCT rpctype) AS rpctype_count "
            f"FROM {full_table}"
        ).collect()
        if not agg_rows:
            raise SmokeTestRuntimeError(f"Spark Iceberg query returned no rows for {full_table}")
        agg = agg_rows[0]
        row_count = int(agg["row_count"])
        distinct_traces = int(agg["distinct_traces"])
        sum_rt = float(agg["sum_rt"])
        rpctype_count = int(agg["rpctype_count"])
        _verify_invariants(
            row_count=row_count,
            distinct_traces=distinct_traces,
            sum_rt=sum_rt,
            rpctype_count=rpctype_count,
            columns=read_columns,
            context=f"Spark Iceberg({full_table})",
        )
        elapsed = round(time.perf_counter() - start, 6)

        if export_fallback:
            export_start = time.perf_counter()
            dest_dir.mkdir(parents=True, exist_ok=True)
            export_dir = dest_dir / valid_table
            if export_dir.exists():
                shutil.rmtree(export_dir)
            read_df.coalesce(1).write.mode("overwrite").parquet(str(export_dir))
            export_elapsed = round(time.perf_counter() - export_start, 6)
            export_path = str(export_dir)
    except SmokeTestError:
        raise
    except Exception as exc:
        sanitized = sanitize_secret_text(str(exc), _secret_variants(settings))
        raise SmokeTestRuntimeError(
            f"Spark Iceberg smoke test failed for {full_table}: {sanitized}"
        ) from exc
    finally:
        for key, prev in previous_env.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev
        if spark is not None:
            spark.stop()

    return SmokeTestResult(
        mode="spark",
        status="passed",
        engine="spark-iceberg-rest",
        row_count=row_count,
        distinct_traces=distinct_traces,
        sum_rt=sum_rt,
        rpctype_count=rpctype_count,
        columns=read_columns,
        elapsed_seconds=elapsed,
        export_elapsed_seconds=export_elapsed,
        fallback_parquet_path=export_path,
        engine_limits=[
            "PySpark 4.1.4 + Iceberg 1.11.0 requires Java 17+ (run inside ie212-spark container).",
            "Uses S3FileIO without hadoop-aws to prevent AWS SDK bundle conflicts.",
        ],
        details={
            "target": settings.target,
            "target_table": full_table,
            "catalog_uri": settings.catalog_uri,
            "minio_endpoint": settings.minio_endpoint,
            "synthetic_dataset": True,
        },
    )


def _load_duckdb_extension(conn: Any, extension: str) -> None:
    """Try LOAD <ext>; first and only run INSTALL <ext>; LOAD <ext>; if LOAD fails."""
    ext = validate_sql_identifier(extension, label="duckdb_extension")
    try:
        conn.execute(f"LOAD {ext};")
    except duckdb.Error:
        conn.execute(f"INSTALL {ext}; LOAD {ext};")


def run_duckdb_iceberg_smoke_test(
    settings: LakehouseConnectionSettings,
    *,
    namespace: str = DEFAULT_NAMESPACE,
    table: str = DEFAULT_TABLE,
    fallback_dir: str | Path | None = None,
    fallback_on_error: bool = False,
    attach_target: str = "",
    dry_run: bool = False,
    connection_factory: Callable[[], Any] | None = None,
) -> SmokeTestResult:
    """Verify Iceberg REST table read via DuckDB 2.0.1, with optional Parquet export fallback."""
    valid_ns = validate_sql_identifier(namespace, label="namespace")
    valid_table = validate_sql_identifier(table, label="table")
    full_table = f"{settings.catalog_name}.{valid_ns}.{valid_table}"
    dest_dir = Path(fallback_dir or settings.parquet_fallback_dir)

    redacted_secret_sql = build_duckdb_s3_secret_sql(settings, redact=True)
    attach_sql = build_duckdb_attach_sql(settings, attach_target=attach_target)
    query_sql = (
        f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT traceid) AS distinct_traces, "
        f"SUM(rt) AS sum_rt, COUNT(DISTINCT rpctype) AS rpctype_count "
        f"FROM {full_table}"
    )

    if dry_run:
        return SmokeTestResult(
            mode="duckdb",
            status="dry_run",
            engine="duckdb-iceberg",
            details={
                "target": settings.target,
                "target_table": full_table,
                "s3_secret_sql": redacted_secret_sql,
                "attach_sql": attach_sql,
                "query_sql": query_sql,
                "fallback_on_error": fallback_on_error,
                "fallback_dir": str(dest_dir),
            },
        )

    if not settings.access_key or not settings.secret_key:
        raise SmokeTestConfigError(
            "Missing MinIO/S3 credentials for DuckDB Iceberg smoke test. "
            "Set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD in .env or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        )

    secret_sql = build_duckdb_s3_secret_sql(settings, redact=False)
    start = time.perf_counter()
    conn = connection_factory() if connection_factory is not None else duckdb.connect(":memory:")
    attached = False
    try:
        _load_duckdb_extension(conn, "httpfs")
        _load_duckdb_extension(conn, "iceberg")
        conn.execute(secret_sql)
        conn.execute(attach_sql)
        attached = True

        describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {full_table}").fetchall()
        columns = [str(row[0]) for row in describe_rows]
        agg_row = conn.execute(query_sql).fetchone()
        if agg_row is None:
            raise SmokeTestRuntimeError(f"DuckDB Iceberg query returned no rows for {full_table}")

        row_count, distinct_traces, sum_rt, rpctype_count = agg_row
        _verify_invariants(
            row_count=int(row_count),
            distinct_traces=int(distinct_traces),
            sum_rt=float(sum_rt),
            rpctype_count=int(rpctype_count),
            columns=columns,
            context=f"DuckDB Iceberg({full_table})",
        )
        elapsed = round(time.perf_counter() - start, 6)
        return SmokeTestResult(
            mode="duckdb",
            status="passed",
            engine="duckdb-iceberg",
            row_count=int(row_count),
            distinct_traces=int(distinct_traces),
            sum_rt=float(sum_rt),
            rpctype_count=int(rpctype_count),
            columns=columns,
            elapsed_seconds=elapsed,
            fallback_used=False,
            engine_limits=[
                "DuckDB httpfs/iceberg extensions are pre-installed in ie212-spark (HOME=/home/spark); host .venv runs download on first LOAD.",
                "MinIO S3 secret requires URL_STYLE 'path' and USE_SSL false.",
            ],
            details={
                "target": settings.target,
                "target_table": full_table,
                "catalog_uri": settings.catalog_uri,
                "minio_endpoint": settings.minio_endpoint,
                "synthetic_dataset": True,
            },
        )
    except (duckdb.Error, OSError, SmokeTestRuntimeError) as exc:
        sanitized_err = sanitize_secret_text(str(exc), _secret_variants(settings))
        if not fallback_on_error:
            raise SmokeTestRuntimeError(
                f"DuckDB Iceberg verification failed for {full_table}: {sanitized_err}"
            ) from exc

        fallback_source = _find_fallback_parquet_source(dest_dir, valid_table)
        if fallback_source is None:
            raise SmokeTestRuntimeError(
                f"DuckDB Iceberg verification failed ({sanitized_err}) and no Parquet fallback "
                f"was found under {dest_dir} for table {valid_table!r}."
            ) from exc

        fb_start = time.perf_counter()
        fb_conn = duckdb.connect(":memory:")
        try:
            fb_metrics = _query_duckdb_parquet_metrics(fb_conn, fallback_source)
        finally:
            fb_conn.close()
        fb_elapsed = round(time.perf_counter() - fb_start, 6)
        total_elapsed = round(time.perf_counter() - start, 6)

        return SmokeTestResult(
            mode="duckdb",
            status="passed",
            engine="duckdb-parquet-fallback",
            row_count=fb_metrics["row_count"],
            distinct_traces=fb_metrics["distinct_traces"],
            sum_rt=fb_metrics["sum_rt"],
            rpctype_count=fb_metrics["rpctype_count"],
            columns=fb_metrics["columns"],
            elapsed_seconds=total_elapsed,
            fallback_used=True,
            fallback_elapsed_seconds=fb_elapsed,
            fallback_parquet_path=str(fallback_source),
            primary_error=sanitized_err,
            engine_limits=[
                "DuckDB Iceberg REST attach/read fell back to exported Parquet via read_parquet().",
                "Parquet fallback timing is recorded separately in fallback_elapsed_seconds.",
            ],
            details={
                "target": settings.target,
                "target_table": full_table,
                "fallback_dir": str(dest_dir),
                "synthetic_dataset": True,
            },
        )
    finally:
        if attached:
            try:
                conn.execute(f"DETACH {settings.catalog_name}")
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def run_smoke_test(
    settings: LakehouseConnectionSettings,
    *,
    mode: str = "local",
    namespace: str = DEFAULT_NAMESPACE,
    table: str = DEFAULT_TABLE,
    fallback_dir: str | Path | None = None,
    export_fallback: bool = False,
    fallback_on_error: bool = False,
    attach_target: str = "",
    dry_run: bool = False,
    cleanup_local: bool = False,
) -> dict[str, Any]:
    """Execute one or more smoke test stages and return a redacted summary payload."""
    normalized_mode = mode.strip().lower()
    if normalized_mode not in ("local", "duckdb", "spark", "all"):
        raise SmokeTestConfigError(
            f"Unsupported smoke test mode {mode!r}: expected 'local', 'duckdb', 'spark', or 'all'"
        )

    valid_ns = validate_sql_identifier(namespace, label="namespace")
    valid_table = validate_sql_identifier(table, label="table")
    effective_fallback_dir = Path(fallback_dir or settings.parquet_fallback_dir)
    results: list[SmokeTestResult] = []

    if normalized_mode == "local":
        results.append(
            run_local_parquet_smoke_test(
                effective_fallback_dir,
                namespace=valid_ns,
                table=valid_table,
                cleanup=cleanup_local,
                settings=settings,
                dry_run=dry_run,
            )
        )
    elif normalized_mode == "spark":
        results.append(
            run_spark_smoke_test(
                settings,
                namespace=valid_ns,
                table=valid_table,
                fallback_dir=effective_fallback_dir,
                export_fallback=export_fallback,
                dry_run=dry_run,
            )
        )
    elif normalized_mode == "duckdb":
        results.append(
            run_duckdb_iceberg_smoke_test(
                settings,
                namespace=valid_ns,
                table=valid_table,
                fallback_dir=effective_fallback_dir,
                fallback_on_error=fallback_on_error,
                attach_target=attach_target,
                dry_run=dry_run,
            )
        )
    else:  # "all"
        should_export = export_fallback or fallback_on_error
        spark_res = run_spark_smoke_test(
            settings,
            namespace=valid_ns,
            table=valid_table,
            fallback_dir=effective_fallback_dir,
            export_fallback=should_export,
            dry_run=dry_run,
        )
        duckdb_res = run_duckdb_iceberg_smoke_test(
            settings,
            namespace=valid_ns,
            table=valid_table,
            fallback_dir=effective_fallback_dir,
            fallback_on_error=fallback_on_error,
            attach_target=attach_target,
            dry_run=dry_run,
        )
        results.extend([spark_res, duckdb_res])

    all_limits: list[str] = []
    for res in results:
        for limit in res.engine_limits:
            if limit not in all_limits:
                all_limits.append(limit)

    overall_status = "dry_run" if dry_run else "passed"
    return {
        "stage": "smoke_test",
        "mode": normalized_mode,
        "target": settings.target,
        "status": overall_status,
        "settings": settings.to_redacted_dict(),
        "results": [res.to_dict() for res in results],
        "engine_limits": all_limits,
    }


__all__ = [
    "CREDENTIAL_ENV_KEYS",
    "DEFAULT_NAMESPACE",
    "DEFAULT_TABLE",
    "EXPECTED_COLUMNS",
    "EXPECTED_DISTINCT_TRACES",
    "EXPECTED_ROW_COUNT",
    "EXPECTED_RPCTYPE_COUNT",
    "EXPECTED_SUM_RT",
    "LakehouseConnectionSettings",
    "REDACTED_VALUE",
    "SMOKE_TEST_ROWS",
    "SmokeTestConfigError",
    "SmokeTestError",
    "SmokeTestResult",
    "SmokeTestRuntimeError",
    "build_duckdb_attach_sql",
    "build_duckdb_s3_secret_sql",
    "build_spark_iceberg_conf",
    "check_java_for_spark",
    "collect_runtime_secrets",
    "detect_java_major_version",
    "escape_sql_literal",
    "get_smoke_test_arrow_table",
    "has_prebaked_iceberg_jars",
    "normalize_target",
    "parse_java_major_version",
    "redact_spark_conf",
    "redact_value",
    "resolve_lakehouse_settings",
    "run_duckdb_iceberg_smoke_test",
    "run_local_parquet_smoke_test",
    "run_smoke_test",
    "run_spark_smoke_test",
    "sanitize_secret_text",
    "strip_url_scheme_for_s3_endpoint",
    "validate_sql_identifier",
]
