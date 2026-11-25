"""Offline-safe unit and local integration tests for T08 smoke tests."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from ie212.config import ConfigError, load_config
from ie212.smoke_test import (
    EXPECTED_COLUMNS,
    EXPECTED_DISTINCT_TRACES,
    EXPECTED_ROW_COUNT,
    EXPECTED_RPCTYPE_COUNT,
    EXPECTED_SUM_RT,
    REDACTED_VALUE,
    LakehouseConnectionSettings,
    SmokeTestConfigError,
    SmokeTestRuntimeError,
    _find_fallback_parquet_source,
    build_duckdb_attach_sql,
    build_duckdb_s3_secret_sql,
    build_spark_iceberg_conf,
    check_java_for_spark,
    escape_sql_literal,
    get_smoke_test_arrow_table,
    has_prebaked_iceberg_jars,
    parse_java_major_version,
    redact_spark_conf,
    resolve_lakehouse_settings,
    run_duckdb_iceberg_smoke_test,
    run_local_parquet_smoke_test,
    run_smoke_test,
    run_spark_smoke_test,
    sanitize_secret_text,
    strip_url_scheme_for_s3_endpoint,
    validate_sql_identifier,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
LAKEHOUSE_YAML = REPO_ROOT / "configs" / "lakehouse.yaml"


def _load_run_smoke_module() -> Any:
    script_path = REPO_ROOT / "scripts" / "run_smoke_test.py"
    spec = importlib.util.spec_from_file_location("run_smoke_test_cli", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_smoke_module = _load_run_smoke_module()
run_smoke_cli = run_smoke_module.main


def _load_sample_cfg(environ: dict[str, str] | None = None) -> dict[str, Any]:
    return load_config(
        LAKEHOUSE_YAML,
        dotenv_path=REPO_ROOT / ".env.nonexistent",
        environ=environ or {},
    )


def test_resolve_settings_container_docker_and_local_targets() -> None:
    cfg = _load_sample_cfg(
        {
            "MINIO_ROOT_USER": "ie212_user",
            "MINIO_ROOT_PASSWORD": "ie212_password_123",
        }
    )

    container_settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)
    docker_settings = resolve_lakehouse_settings(cfg, target="docker", require_credentials=True)
    local_settings = resolve_lakehouse_settings(cfg, target="local", require_credentials=True)

    assert container_settings.target == "container"
    assert docker_settings.target == "container"
    assert container_settings.minio_endpoint == "http://minio:9000"
    assert container_settings.minio_s3_endpoint_host_port == "minio:9000"
    assert container_settings.catalog_uri == "http://iceberg-rest:8181"

    assert local_settings.target == "local"
    assert local_settings.minio_endpoint == "http://localhost:9000"
    assert local_settings.minio_s3_endpoint_host_port == "localhost:9000"
    assert local_settings.catalog_uri == "http://localhost:8181"


def test_resolve_settings_falls_back_to_aws_env_vars_and_redacts_secrets() -> None:
    cfg = _load_sample_cfg({})  # minio.access_key and minio.secret_key evaluate to ""
    settings = resolve_lakehouse_settings(
        cfg,
        target="container",
        environ={
            "AWS_ACCESS_KEY_ID": "aws_key_from_compose",
            "AWS_SECRET_ACCESS_KEY": "aws_secret_from_compose",
        },
        require_credentials=True,
    )

    assert settings.access_key == "aws_key_from_compose"
    assert settings.secret_key == "aws_secret_from_compose"

    redacted = settings.to_redacted_dict()
    assert redacted["access_key"] == REDACTED_VALUE
    assert redacted["secret_key"] == REDACTED_VALUE
    assert "aws_key_from_compose" not in repr(settings)
    assert "aws_secret_from_compose" not in repr(settings)


def test_resolve_settings_missing_credentials_or_endpoint_raises() -> None:
    cfg = _load_sample_cfg({})

    # Non-live mode allows blank credentials
    non_live = resolve_lakehouse_settings(cfg, environ={}, require_credentials=False)
    assert non_live.access_key == ""

    # Live mode requires non-empty credentials
    with pytest.raises(SmokeTestConfigError, match="Missing MinIO/S3 credentials"):
        resolve_lakehouse_settings(cfg, environ={}, require_credentials=True)

    # Empty catalog URI fails validation
    bad_cfg = _load_sample_cfg({"MINIO_ROOT_USER": "user1", "MINIO_ROOT_PASSWORD": "password1"})
    bad_cfg["iceberg"]["catalog_uri"] = ""
    with pytest.raises(SmokeTestConfigError, match="Iceberg catalog URI"):
        resolve_lakehouse_settings(bad_cfg, target="container", require_credentials=True)


def test_build_spark_iceberg_conf_and_prebaked_jar_detection(tmp_path: Path) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "spark_key", "MINIO_SECRET_KEY": "spark_secret_123"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    empty_jars_dir = tmp_path / "empty_jars"
    empty_jars_dir.mkdir()
    assert not has_prebaked_iceberg_jars(empty_jars_dir)

    conf_with_pkgs = build_spark_iceberg_conf(settings, jars_dir=empty_jars_dir)
    assert conf_with_pkgs["spark.sql.extensions"] == (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    assert conf_with_pkgs["spark.sql.catalog.ie212"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf_with_pkgs["spark.sql.catalog.ie212.type"] == "rest"
    assert conf_with_pkgs["spark.sql.catalog.ie212.uri"] == "http://iceberg-rest:8181"
    assert conf_with_pkgs["spark.sql.catalog.ie212.warehouse"] == "s3://silver/warehouse"
    assert conf_with_pkgs["spark.sql.catalog.ie212.io-impl"] == (
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    assert conf_with_pkgs["spark.sql.catalog.ie212.s3.endpoint"] == "http://minio:9000"
    assert conf_with_pkgs["spark.sql.catalog.ie212.s3.path-style-access"] == "true"
    assert conf_with_pkgs["spark.sql.catalog.ie212.client.region"] == "us-east-1"
    assert conf_with_pkgs["spark.sql.defaultCatalog"] == "ie212"
    assert "spark.jars.packages" in conf_with_pkgs
    assert "hadoop-aws" not in conf_with_pkgs["spark.jars.packages"]
    assert "software.amazon.awssdk:bundle" not in conf_with_pkgs["spark.jars.packages"]
    assert not any(k.startswith("spark.hadoop.fs.s3a") for k in conf_with_pkgs)

    # Partial pre-baked JARs (runtime-only or bundle-only) -> still requires spark.jars.packages
    runtime_only_dir = tmp_path / "runtime_only_jars"
    runtime_only_dir.mkdir()
    (runtime_only_dir / "iceberg-spark-runtime-4.1_2.13-1.11.0.jar").write_text("", encoding="utf-8")
    assert not has_prebaked_iceberg_jars(runtime_only_dir)
    assert "spark.jars.packages" in build_spark_iceberg_conf(settings, jars_dir=runtime_only_dir)

    bundle_only_dir = tmp_path / "bundle_only_jars"
    bundle_only_dir.mkdir()
    (bundle_only_dir / "iceberg-aws-bundle-1.11.0.jar").write_text("", encoding="utf-8")
    assert not has_prebaked_iceberg_jars(bundle_only_dir)
    assert "spark.jars.packages" in build_spark_iceberg_conf(settings, jars_dir=bundle_only_dir)

    # Both pre-baked JARs present -> skips spark.jars.packages
    prebaked_dir = tmp_path / "spark_jars"
    prebaked_dir.mkdir()
    (prebaked_dir / "iceberg-spark-runtime-4.1_2.13-1.11.0.jar").write_text("", encoding="utf-8")
    (prebaked_dir / "iceberg-aws-bundle-1.11.0.jar").write_text("", encoding="utf-8")
    assert has_prebaked_iceberg_jars(prebaked_dir)
    conf_prebaked = build_spark_iceberg_conf(settings, jars_dir=prebaked_dir)
    assert "spark.jars.packages" not in conf_prebaked

    # Redaction check on Spark conf
    redacted = redact_spark_conf(conf_prebaked)
    assert redacted["spark.sql.catalog.ie212.s3.access-key-id"] == REDACTED_VALUE
    assert redacted["spark.sql.catalog.ie212.s3.secret-access-key"] == REDACTED_VALUE


def test_build_spark_iceberg_conf_forbids_hadoop_aws_when_excluded() -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "spark_key", "MINIO_SECRET_KEY": "spark_secret_123"}
    )
    cfg["spark"]["iceberg_packages"] = [
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        "org.apache.hadoop:hadoop-aws:3.4.0",
    ]
    cfg["spark"]["exclude_hadoop_aws"] = True
    settings = resolve_lakehouse_settings(cfg, require_credentials=True)

    with pytest.raises(SmokeTestConfigError, match="Forbidden Spark package"):
        build_spark_iceberg_conf(settings, include_packages=True)


def test_java_version_parser_and_host_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    assert parse_java_major_version('java version "1.8.0_511"') == 8
    assert parse_java_major_version('openjdk version "17.0.12" 2024-07-16') == 17
    assert parse_java_major_version('openjdk version "21.0.3"') == 21
    assert parse_java_major_version("not a java version") is None

    monkeypatch.setattr("ie212.smoke_test.detect_java_major_version", lambda: 8)
    with pytest.raises(SmokeTestRuntimeError, match="requires Java 17\\+"):
        check_java_for_spark(17)

    monkeypatch.setattr("ie212.smoke_test.detect_java_major_version", lambda: None)
    with pytest.raises(SmokeTestRuntimeError, match="Java runtime not found"):
        check_java_for_spark(17)

    monkeypatch.setattr("ie212.smoke_test.detect_java_major_version", lambda: 17)
    assert check_java_for_spark(17) == 17


def test_mocked_spark_write_read_and_export_fallback(tmp_path: Path) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "spark_user", "MINIO_SECRET_KEY": "spark_pass_123"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    class FakeParquetWriter:
        def __init__(self) -> None:
            self.written_path: str | None = None

        def mode(self, _mode: str) -> "FakeParquetWriter":
            return self

        def parquet(self, path: str) -> None:
            self.written_path = path
            out_dir = Path(path)
            out_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(get_smoke_test_arrow_table(), out_dir / "part-00000.parquet")

    class FakeWriteTo:
        def __init__(self, parent: "FakeSparkSession", table_name: str) -> None:
            self.parent = parent
            self.table_name = table_name
            self.fmt: str | None = None

        def using(self, fmt: str) -> "FakeWriteTo":
            self.fmt = fmt
            return self

        def createOrReplace(self) -> None:
            self.parent.created_tables[self.table_name] = self.fmt or ""

    class FakeDataFrame:
        def __init__(self, parent: "FakeSparkSession") -> None:
            self.parent = parent
            self.columns = list(EXPECTED_COLUMNS)
            self.write = FakeParquetWriter()

        def writeTo(self, table_name: str) -> FakeWriteTo:
            return FakeWriteTo(self.parent, table_name)

        def coalesce(self, _n: int) -> "FakeDataFrame":
            return self

    class FakeQueryResult:
        def __init__(self, rows: list[dict[str, Any]]) -> None:
            self._rows = rows

        def collect(self) -> list[dict[str, Any]]:
            return self._rows

    class FakeSparkSession:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.created_tables: dict[str, str] = {}
            self.stopped = False

        def sql(self, query: str) -> FakeQueryResult:
            self.queries.append(query)
            if query.startswith("SELECT COUNT(*)"):
                return FakeQueryResult(
                    [
                        {
                            "row_count": EXPECTED_ROW_COUNT,
                            "distinct_traces": EXPECTED_DISTINCT_TRACES,
                            "sum_rt": EXPECTED_SUM_RT,
                            "rpctype_count": EXPECTED_RPCTYPE_COUNT,
                        }
                    ]
                )
            return FakeQueryResult([])

        def createDataFrame(self, _rows: Any, schema: Any = None) -> FakeDataFrame:
            assert len(schema.fields) == 8
            return FakeDataFrame(self)

        def table(self, table_name: str) -> FakeDataFrame:
            assert table_name in self.created_tables
            return FakeDataFrame(self)

        def stop(self) -> None:
            self.stopped = True

    fake_spark = FakeSparkSession()
    fallback_dir = tmp_path / "fallback"
    result = run_spark_smoke_test(
        settings,
        fallback_dir=fallback_dir,
        export_fallback=True,
        spark_session_factory=lambda _conf: fake_spark,
        skip_java_check=True,
    )

    assert result.status == "passed"
    assert result.row_count == EXPECTED_ROW_COUNT
    assert result.distinct_traces == EXPECTED_DISTINCT_TRACES
    assert result.sum_rt == pytest.approx(EXPECTED_SUM_RT)
    assert result.rpctype_count == EXPECTED_RPCTYPE_COUNT
    assert result.export_elapsed_seconds is not None
    assert result.fallback_parquet_path == str(fallback_dir / "smoke_test_calls")
    assert fake_spark.created_tables["ie212.silver.smoke_test_calls"] == "iceberg"
    assert "CREATE NAMESPACE IF NOT EXISTS ie212.silver" in fake_spark.queries
    assert fake_spark.stopped is True


def test_duckdb_sql_builders_escaping_identifiers_and_redaction() -> None:
    assert strip_url_scheme_for_s3_endpoint("http://minio:9000/") == ("minio:9000", False)
    assert strip_url_scheme_for_s3_endpoint("https://minio.local:9443") == ("minio.local:9443", True)
    assert escape_sql_literal("a'b'c") == "a''b''c"

    with pytest.raises(SmokeTestConfigError, match="Invalid SQL"):
        validate_sql_identifier("bad-table; DROP TABLE x")

    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "key'one", "MINIO_SECRET_KEY": "sec'ret_val"}
    )
    settings = resolve_lakehouse_settings(cfg, target="local", require_credentials=True)

    secret_sql = build_duckdb_s3_secret_sql(settings, redact=False)
    assert "CREATE OR REPLACE SECRET minio_s3 (" in secret_sql
    assert "TYPE s3," in secret_sql
    assert "KEY_ID 'key''one'" in secret_sql
    assert "SECRET 'sec''ret_val'" in secret_sql
    assert "ENDPOINT 'localhost:9000'" in secret_sql
    assert "URL_STYLE 'path'" in secret_sql
    assert "USE_SSL false" in secret_sql

    redacted_sql = build_duckdb_s3_secret_sql(settings, redact=True)
    assert f"KEY_ID '{REDACTED_VALUE}'" in redacted_sql
    assert f"SECRET '{REDACTED_VALUE}'" in redacted_sql
    assert "key''one" not in redacted_sql
    assert "sec''ret_val" not in redacted_sql

    # Verify DuckDB 2.0.1 parses the generated CREATE OR REPLACE SECRET syntax cleanly
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(secret_sql)
    finally:
        conn.close()

    attach_sql = build_duckdb_attach_sql(settings)
    assert "ATTACH '' AS ie212 (" in attach_sql
    assert "TYPE iceberg," in attach_sql
    assert "ENDPOINT 'http://localhost:8181'," in attach_sql
    assert "AUTHORIZATION_TYPE none," in attach_sql
    assert "ACCESS_DELEGATION_MODE 'none'" in attach_sql


def test_local_duckdb_parquet_smoke_test_real_execution(tmp_path: Path) -> None:
    fallback_dir = tmp_path / "local_fb"
    result = run_local_parquet_smoke_test(fallback_dir, cleanup=False)

    assert result.status == "passed"
    assert result.engine == "duckdb-local-parquet"
    assert result.row_count == EXPECTED_ROW_COUNT
    assert result.distinct_traces == EXPECTED_DISTINCT_TRACES
    assert result.sum_rt == pytest.approx(EXPECTED_SUM_RT)
    assert result.rpctype_count == EXPECTED_RPCTYPE_COUNT
    assert result.columns == list(EXPECTED_COLUMNS)
    assert Path(result.fallback_parquet_path or "").is_file()

    # With cleanup=True, removes generated Parquet file
    cleanup_dir = tmp_path / "local_fb_cleanup"
    cleanup_res = run_local_parquet_smoke_test(cleanup_dir, cleanup=True)
    assert cleanup_res.status == "passed"
    assert cleanup_res.fallback_parquet_path is None
    assert not cleanup_dir.exists()


def test_duckdb_iceberg_error_fallback_and_strict_failure(tmp_path: Path) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "my_access_key", "MINIO_SECRET_KEY": "my_secret_pass_99"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    # Prepare real Parquet fallback file in tmp_path
    fallback_dir = tmp_path / "fb"
    local_res = run_local_parquet_smoke_test(fallback_dir, cleanup=False)
    assert local_res.status == "passed"

    class FailingIcebergConn:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, sql: str) -> Any:
            if sql.startswith("ATTACH "):
                raise duckdb.IOException(
                    "Could not connect to http://iceberg-rest:8181 using my_secret_pass_99"
                )
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        def close(self) -> None:
            self.closed = True

    # Strict mode (fallback_on_error=False): fails cleanly and sanitizes secrets
    with pytest.raises(SmokeTestRuntimeError) as exc_info:
        run_duckdb_iceberg_smoke_test(
            settings,
            fallback_dir=fallback_dir,
            fallback_on_error=False,
            connection_factory=FailingIcebergConn,
        )
    assert "my_secret_pass_99" not in str(exc_info.value)
    assert REDACTED_VALUE in str(exc_info.value)

    # Fallback mode (fallback_on_error=True): runs real DuckDB read_parquet() fallback
    fb_res = run_duckdb_iceberg_smoke_test(
        settings,
        fallback_dir=fallback_dir,
        fallback_on_error=True,
        connection_factory=FailingIcebergConn,
    )
    assert fb_res.status == "passed"
    assert fb_res.engine == "duckdb-parquet-fallback"
    assert fb_res.fallback_used is True
    assert fb_res.fallback_elapsed_seconds is not None
    assert fb_res.primary_error is not None
    assert "my_secret_pass_99" not in fb_res.primary_error
    assert fb_res.row_count == EXPECTED_ROW_COUNT
    assert fb_res.sum_rt == pytest.approx(EXPECTED_SUM_RT)


def test_duckdb_iceberg_happy_path_with_mocked_connection() -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "my_access_key", "MINIO_SECRET_KEY": "my_secret_pass_99"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    class HappyIcebergConn:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.closed = False

        def execute(self, sql: str) -> Any:
            self.executed.append(sql)
            if sql.startswith("DESCRIBE "):
                return SimpleNamespace(fetchall=lambda: [(col,) for col in EXPECTED_COLUMNS])
            if sql.startswith("SELECT COUNT(*)"):
                return SimpleNamespace(
                    fetchone=lambda: (
                        EXPECTED_ROW_COUNT,
                        EXPECTED_DISTINCT_TRACES,
                        EXPECTED_SUM_RT,
                        EXPECTED_RPCTYPE_COUNT,
                    )
                )
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        def close(self) -> None:
            self.closed = True

    conn = HappyIcebergConn()
    res = run_duckdb_iceberg_smoke_test(
        settings,
        fallback_on_error=False,
        connection_factory=lambda: conn,
    )
    assert res.status == "passed"
    assert res.engine == "duckdb-iceberg"
    assert res.fallback_used is False
    assert res.row_count == EXPECTED_ROW_COUNT
    assert res.sum_rt == pytest.approx(EXPECTED_SUM_RT)
    assert "DETACH ie212" in conn.executed
    assert conn.closed is True


def test_cli_entrypoints_and_missing_credentials_exit_codes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # --help
    with pytest.raises(SystemExit) as help_exc:
        run_smoke_cli(["--help"])
    assert help_exc.value.code == 0
    assert "IE212 Iceberg Spark & DuckDB" in capsys.readouterr().out

    # --check-config
    rc = run_smoke_cli(
        [
            "--config",
            str(LAKEHOUSE_YAML),
            "--config-dir",
            str(REPO_ROOT / "configs"),
            "--dotenv",
            str(tmp_path / "missing.env"),
            "--check-config",
        ]
    )
    assert rc == 0
    check_payload = json.loads(capsys.readouterr().out)
    assert check_payload["status"] == "ok"

    # --dry-run and --print-plan
    for flag in ("--dry-run", "--print-plan"):
        rc = run_smoke_cli(
            [
                "--config",
                str(LAKEHOUSE_YAML),
                "--dotenv",
                str(tmp_path / "missing.env"),
                "--mode",
                "all",
                flag,
            ]
        )
        assert rc == 0
        dry_payload = json.loads(capsys.readouterr().out)
        assert dry_payload["status"] == "dry_run"
        assert len(dry_payload["results"]) == 2

    # --mode local --fallback-dir <tmp_path>
    fb_dir = tmp_path / "cli_local_fb"
    rc = run_smoke_cli(
        [
            "--config",
            str(LAKEHOUSE_YAML),
            "--dotenv",
            str(tmp_path / "missing.env"),
            "--mode",
            "local",
            "--fallback-dir",
            str(fb_dir),
        ]
    )
    assert rc == 0
    local_payload = json.loads(capsys.readouterr().out)
    assert local_payload["status"] == "passed"
    assert local_payload["results"][0]["row_count"] == EXPECTED_ROW_COUNT

    # Live modes without credentials must exit 1 cleanly
    for key in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(key, raising=False)

    for live_mode in ("spark", "duckdb", "all"):
        rc = run_smoke_cli(
            [
                "--config",
                str(LAKEHOUSE_YAML),
                "--dotenv",
                str(tmp_path / "missing.env"),
                "--mode",
                live_mode,
            ]
        )
        assert rc == 1
        err_out = capsys.readouterr().err
        assert "Missing MinIO/S3 credentials" in err_out


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (False, False),
        ("false", False),
        ("FALSE", False),
        (" 0 ", False),
        ("no", False),
        ("Off", False),
        (True, True),
        ("true", True),
        ("1", True),
        ("YES", True),
        (" on ", True),
    ],
)
def test_resolve_settings_strict_bool_coercion_valid(raw_value: Any, expected: bool) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ROOT_USER": "user1", "MINIO_ROOT_PASSWORD": "password_123"}
    )
    cfg["minio"]["use_ssl"] = raw_value
    cfg["minio"]["path_style_access"] = raw_value
    cfg["spark"]["exclude_hadoop_aws"] = raw_value

    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)
    assert settings.use_ssl is expected
    assert settings.path_style_access is expected
    assert settings.exclude_hadoop_aws is expected


@pytest.mark.parametrize("bad_value", ["maybe", "", "2", 123, ["false"]])
def test_resolve_settings_strict_bool_coercion_invalid_raises(bad_value: Any) -> None:
    for section, field in (
        ("minio", "use_ssl"),
        ("minio", "path_style_access"),
        ("spark", "exclude_hadoop_aws"),
    ):
        cfg = _load_sample_cfg(
            {"MINIO_ROOT_USER": "user1", "MINIO_ROOT_PASSWORD": "password_123"}
        )
        cfg[section][field] = bad_value
        with pytest.raises(SmokeTestConfigError, match="Invalid boolean"):
            resolve_lakehouse_settings(cfg, target="container", require_credentials=True)


def test_sql_escaped_secret_scrubbing_longest_first(tmp_path: Path) -> None:
    assert sanitize_secret_text(
        "err raw=sk'ret esc=sk''ret tag=RED",
        ["RED", "sk'ret", "sk''ret"],
    ) == f"err raw={REDACTED_VALUE} esc={REDACTED_VALUE} tag={REDACTED_VALUE}"

    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "ak'one", "MINIO_SECRET_KEY": "sk'with'quotes"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    class EscapedLeakingConn:
        def execute(self, sql: str) -> Any:
            if sql.startswith("ATTACH "):
                raise duckdb.IOException(
                    "Auth failed for KEY_ID='ak''one' SECRET='sk''with''quotes' raw=sk'with'quotes"
                )
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        def close(self) -> None:
            pass

    with pytest.raises(SmokeTestRuntimeError) as duck_exc:
        run_duckdb_iceberg_smoke_test(
            settings,
            fallback_on_error=False,
            connection_factory=EscapedLeakingConn,
        )
    msg = str(duck_exc.value)
    assert "ak'one" not in msg
    assert "ak''one" not in msg
    assert "sk'with'quotes" not in msg
    assert "sk''with''quotes" not in msg
    assert REDACTED_VALUE in msg

    fallback_dir = tmp_path / "fb_escaped"
    run_local_parquet_smoke_test(fallback_dir, cleanup=False)
    fb_res = run_duckdb_iceberg_smoke_test(
        settings,
        fallback_dir=fallback_dir,
        fallback_on_error=True,
        connection_factory=EscapedLeakingConn,
    )
    assert fb_res.primary_error is not None
    assert "sk''with''quotes" not in fb_res.primary_error
    assert "ak''one" not in fb_res.primary_error

    class FailingSpark:
        def sql(self, _query: str) -> Any:
            raise RuntimeError("Spark failed with ak''one and sk''with''quotes")

        def stop(self) -> None:
            pass

    with pytest.raises(SmokeTestRuntimeError) as spark_exc:
        run_spark_smoke_test(
            settings,
            spark_session_factory=lambda _conf: FailingSpark(),
            skip_java_check=True,
        )
    spark_msg = str(spark_exc.value)
    assert "ak''one" not in spark_msg
    assert "sk''with''quotes" not in spark_msg
    assert REDACTED_VALUE in spark_msg


def test_cli_outer_exception_redacts_secrets(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text(
        "MINIO_ROOT_USER=cli_user'1\nMINIO_ROOT_PASSWORD=cli_super'secret_99\n",
        encoding="utf-8",
    )

    def _boom_run(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(
            "unexpected failure with cli_user'1, cli_user''1, cli_super'secret_99, cli_super''secret_99"
        )

    monkeypatch.setattr(run_smoke_module, "run_smoke_test", _boom_run)
    rc = run_smoke_cli(
        [
            "--config",
            str(LAKEHOUSE_YAML),
            "--dotenv",
            str(dotenv_file),
            "--mode",
            "duckdb",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "Unexpected smoke test error" in err
    for leaked in ("cli_user'1", "cli_user''1", "cli_super'secret_99", "cli_super''secret_99"):
        assert leaked not in err
    assert REDACTED_VALUE in err

    # Also test unexpected failure before settings is resolved (env fallback scrubbing)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env_outer'secret")

    def _boom_resolve(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("pre-settings crash env_outer'secret and env_outer''secret")

    monkeypatch.setattr(run_smoke_module, "resolve_lakehouse_settings", _boom_resolve)
    rc_pre = run_smoke_cli(
        [
            "--config",
            str(LAKEHOUSE_YAML),
            "--dotenv",
            str(tmp_path / "missing.env"),
            "--mode",
            "local",
        ]
    )
    assert rc_pre == 1
    err_pre = capsys.readouterr().err
    assert "env_outer'secret" not in err_pre
    assert "env_outer''secret" not in err_pre
    assert REDACTED_VALUE in err_pre


def test_local_mode_validates_namespace_identifier(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SmokeTestConfigError, match="Invalid SQL namespace"):
        run_local_parquet_smoke_test(tmp_path, namespace="bad; DROP TABLE x")

    cfg = _load_sample_cfg({})
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=False)
    with pytest.raises(SmokeTestConfigError, match="Invalid SQL namespace"):
        run_smoke_test(
            settings,
            mode="local",
            namespace="bad; DROP TABLE x",
            fallback_dir=tmp_path,
        )

    rc = run_smoke_cli(
        [
            "--config",
            str(LAKEHOUSE_YAML),
            "--dotenv",
            str(tmp_path / "missing.env"),
            "--mode",
            "local",
            "--namespace",
            "bad; DROP TABLE x",
            "--fallback-dir",
            str(tmp_path / "cli_ns"),
        ]
    )
    assert rc == 1
    assert "Invalid SQL namespace" in capsys.readouterr().err


def test_duckdb_loads_extensions_before_install() -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "my_access_key", "MINIO_SECRET_KEY": "my_secret_pass_99"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    class PreinstalledConn:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def execute(self, sql: str) -> Any:
            self.executed.append(sql)
            if sql.startswith("DESCRIBE "):
                return SimpleNamespace(fetchall=lambda: [(col,) for col in EXPECTED_COLUMNS])
            if sql.startswith("SELECT COUNT(*)"):
                return SimpleNamespace(
                    fetchone=lambda: (
                        EXPECTED_ROW_COUNT,
                        EXPECTED_DISTINCT_TRACES,
                        EXPECTED_SUM_RT,
                        EXPECTED_RPCTYPE_COUNT,
                    )
                )
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        def close(self) -> None:
            pass

    preinstalled = PreinstalledConn()
    res_pre = run_duckdb_iceberg_smoke_test(settings, connection_factory=lambda: preinstalled)
    assert res_pre.status == "passed"
    assert "LOAD httpfs;" in preinstalled.executed
    assert "LOAD iceberg;" in preinstalled.executed
    assert not any("INSTALL " in stmt for stmt in preinstalled.executed)

    class MissingIcebergConn(PreinstalledConn):
        def execute(self, sql: str) -> Any:
            if sql == "LOAD iceberg;":
                self.executed.append(sql)
                raise duckdb.IOException("Extension 'iceberg' not found")
            return super().execute(sql)

    missing_ice = MissingIcebergConn()
    res_miss = run_duckdb_iceberg_smoke_test(settings, connection_factory=lambda: missing_ice)
    assert res_miss.status == "passed"
    assert missing_ice.executed[:3] == ["LOAD httpfs;", "LOAD iceberg;", "INSTALL iceberg; LOAD iceberg;"]


def test_run_spark_smoke_test_excludes_export_and_stop_from_elapsed_seconds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "spark_user", "MINIO_SECRET_KEY": "spark_pass_123"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    clock = {"now": 100.0}
    monkeypatch.setattr(
        "ie212.smoke_test.time",
        SimpleNamespace(perf_counter=lambda: clock["now"]),
    )

    class TimedParquetWriter:
        def mode(self, _mode: str) -> "TimedParquetWriter":
            return self

        def parquet(self, path: str) -> None:
            clock["now"] += 5.0
            out_dir = Path(path)
            out_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(get_smoke_test_arrow_table(), out_dir / "part-00000.parquet")

    class TimedWriteTo:
        def using(self, _fmt: str) -> "TimedWriteTo":
            return self

        def createOrReplace(self) -> None:
            pass

    class TimedDataFrame:
        def __init__(self) -> None:
            self.columns = list(EXPECTED_COLUMNS)
            self.write = TimedParquetWriter()

        def writeTo(self, _table_name: str) -> TimedWriteTo:
            return TimedWriteTo()

        def coalesce(self, _n: int) -> "TimedDataFrame":
            return self

    class TimedQueryResult:
        def __init__(self, rows: list[dict[str, Any]]) -> None:
            self._rows = rows

        def collect(self) -> list[dict[str, Any]]:
            return self._rows

    class TimedSparkSession:
        def sql(self, query: str) -> TimedQueryResult:
            if query.startswith("SELECT COUNT(*)"):
                clock["now"] += 1.25
                return TimedQueryResult(
                    [
                        {
                            "row_count": EXPECTED_ROW_COUNT,
                            "distinct_traces": EXPECTED_DISTINCT_TRACES,
                            "sum_rt": EXPECTED_SUM_RT,
                            "rpctype_count": EXPECTED_RPCTYPE_COUNT,
                        }
                    ]
                )
            return TimedQueryResult([])

        def createDataFrame(self, _rows: Any, schema: Any = None) -> TimedDataFrame:
            assert schema is not None
            return TimedDataFrame()

        def table(self, _table_name: str) -> TimedDataFrame:
            return TimedDataFrame()

        def stop(self) -> None:
            clock["now"] += 10.0

    result = run_spark_smoke_test(
        settings,
        fallback_dir=tmp_path / "timed_fb",
        export_fallback=True,
        spark_session_factory=lambda _conf: TimedSparkSession(),
        skip_java_check=True,
    )
    assert result.elapsed_seconds == pytest.approx(1.25)
    assert result.export_elapsed_seconds == pytest.approx(5.0)


def test_find_fallback_parquet_source_prefers_directory_over_stale_file(tmp_path: Path) -> None:
    cfg = _load_sample_cfg(
        {"MINIO_ACCESS_KEY": "my_access_key", "MINIO_SECRET_KEY": "my_secret_pass_99"}
    )
    settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)

    # Non-existent fallback directory returns None
    assert _find_fallback_parquet_source(tmp_path / "does_not_exist", "smoke_test_calls") is None

    fb_dir = tmp_path / "fb_priority"
    fb_dir.mkdir()
    stale_single = fb_dir / "smoke_test_calls.parquet"
    stale_table = pa.table({"stale_col": pa.array([1], type=pa.int64())})
    pq.write_table(stale_table, stale_single)

    # Empty dir, _SUCCESS-only dir, and nested .parquet/ subdir fall back to single file
    export_dir = fb_dir / "smoke_test_calls"
    export_dir.mkdir()
    assert _find_fallback_parquet_source(fb_dir, "smoke_test_calls") == stale_single

    (export_dir / "_SUCCESS").write_text("", encoding="utf-8")
    (export_dir / "nested.parquet").mkdir()
    assert _find_fallback_parquet_source(fb_dir, "smoke_test_calls") == stale_single

    # Removing single file with no valid parquet files in export_dir returns None
    stale_single.unlink()
    assert _find_fallback_parquet_source(fb_dir, "smoke_test_calls") is None

    # Restore stale single file AND write real Spark-style part-*.parquet in export_dir
    pq.write_table(stale_table, stale_single)
    pq.write_table(get_smoke_test_arrow_table(), export_dir / "part-00000.parquet")
    assert _find_fallback_parquet_source(fb_dir, "smoke_test_calls") == export_dir

    class FailingIcebergConn:
        def execute(self, sql: str) -> Any:
            if sql.startswith("ATTACH "):
                raise duckdb.IOException("Iceberg REST unreachable")
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        def close(self) -> None:
            pass

    fb_res = run_duckdb_iceberg_smoke_test(
        settings,
        fallback_dir=fb_dir,
        fallback_on_error=True,
        connection_factory=FailingIcebergConn,
    )
    assert fb_res.status == "passed"
    assert fb_res.fallback_used is True
    assert fb_res.fallback_parquet_path == str(export_dir)
    assert fb_res.row_count == EXPECTED_ROW_COUNT
    assert fb_res.sum_rt == pytest.approx(EXPECTED_SUM_RT)


def test_duckdb_access_delegation_mode_default_override_and_validation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Default when omitted or None -> "none"
    for mutate in ("omit", "none"):
        cfg = _load_sample_cfg(
            {"MINIO_ACCESS_KEY": "k1", "MINIO_SECRET_KEY": "s1"}
        )
        if mutate == "omit":
            cfg["duckdb"].pop("access_delegation_mode", None)
        else:
            cfg["duckdb"]["access_delegation_mode"] = None
        settings = resolve_lakehouse_settings(cfg, target="container", require_credentials=True)
        assert settings.duckdb_access_delegation_mode == "none"
        assert "ACCESS_DELEGATION_MODE 'none'" in build_duckdb_attach_sql(settings)

    # Valid override (including whitespace/case normalization)
    for raw_mode in ("vended_credentials", " VENDED_CREDENTIALS "):
        cfg = _load_sample_cfg(
            {"MINIO_ACCESS_KEY": "k1", "MINIO_SECRET_KEY": "s1"}
        )
        cfg["duckdb"]["access_delegation_mode"] = raw_mode
        settings = resolve_lakehouse_settings(cfg, target="local", require_credentials=True)
        assert settings.duckdb_access_delegation_mode == "vended_credentials"
        attach_sql = build_duckdb_attach_sql(settings)
        assert "ACCESS_DELEGATION_MODE 'vended_credentials'" in attach_sql

        dry_duck = run_duckdb_iceberg_smoke_test(settings, dry_run=True)
        assert "ACCESS_DELEGATION_MODE 'vended_credentials'" in dry_duck.details["attach_sql"]
        dry_all = run_smoke_test(settings, mode="all", dry_run=True)
        assert dry_all["settings"]["duckdb_access_delegation_mode"] == "vended_credentials"
        assert "ACCESS_DELEGATION_MODE 'vended_credentials'" in dry_all["results"][1]["details"]["attach_sql"]

    # Backward-compatible construction of LakehouseConnectionSettings without explicit field
    base_settings = resolve_lakehouse_settings(_load_sample_cfg({}), require_credentials=False)
    legacy_kwargs = {
        k: v
        for k, v in base_settings.__dict__.items()
        if k != "duckdb_access_delegation_mode"
    }
    legacy_settings = LakehouseConnectionSettings(**legacy_kwargs)
    assert legacy_settings.duckdb_access_delegation_mode == "none"

    # Invalid mode strings / SQL injection payloads raise SmokeTestConfigError
    for bad_str in ("invalid_mode", "auto", "none'; DROP TABLE x;--"):
        cfg = _load_sample_cfg({})
        cfg["duckdb"]["access_delegation_mode"] = bad_str
        with pytest.raises(SmokeTestConfigError, match="access_delegation_mode"):
            resolve_lakehouse_settings(cfg, require_credentials=False)

        tampered = replace(base_settings, duckdb_access_delegation_mode=bad_str)
        with pytest.raises(SmokeTestConfigError, match="access_delegation_mode"):
            build_duckdb_attach_sql(tampered)

    # Tampered non-string type in build_duckdb_attach_sql raises SmokeTestConfigError
    tampered_type = replace(base_settings, duckdb_access_delegation_mode=123)  # type: ignore[arg-type]
    with pytest.raises(SmokeTestConfigError, match="access_delegation_mode"):
        build_duckdb_attach_sql(tampered_type)

    # Empty/whitespace/non-string in resolve_lakehouse_settings raises ConfigError
    for empty_or_bad_type in ("", "   ", 123):
        cfg = _load_sample_cfg({})
        cfg["duckdb"]["access_delegation_mode"] = empty_or_bad_type
        with pytest.raises(ConfigError, match="access_delegation_mode"):
            resolve_lakehouse_settings(cfg, require_credentials=False)

    # CLI rejection for invalid access_delegation_mode in custom config YAML
    bad_cfg = _load_sample_cfg({})
    bad_cfg["duckdb"]["access_delegation_mode"] = "invalid"
    bad_yaml = tmp_path / "bad_lakehouse.yaml"
    bad_yaml.write_text(yaml.safe_dump(bad_cfg), encoding="utf-8")
    rc = run_smoke_cli(
        [
            "--config",
            str(bad_yaml),
            "--dotenv",
            str(tmp_path / "missing.env"),
            "--mode",
            "all",
            "--dry-run",
        ]
    )
    assert rc == 1
    assert "access_delegation_mode" in capsys.readouterr().err


