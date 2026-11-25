"""Unit and CLI tests for ie212.config, package metadata, and stage CLI stubs."""

from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path
from typing import Any

import pytest

import ie212
from ie212.config import (
    ConfigError,
    DEFAULT_CONFIG_FILES,
    interpolate_env_vars,
    load_config,
    load_project_configs,
    parse_dotenv,
    validate_benchmarks_config,
    validate_data_config,
    validate_experiments_config,
    validate_features_config,
    validate_lakehouse_config,
    validate_project_configs,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_DIR = REPO_ROOT / "configs"
NONEXISTENT_DOTENV = REPO_ROOT / ".env.nonexistent"

STUB_CLIS: list[tuple[str, str, str]] = [
    ("run_gold.py", "gold", "configs/lakehouse.yaml"),
    ("run_graphs.py", "graphs", "configs/features.yaml"),
    ("run_features.py", "features", "configs/features.yaml"),
    ("run_train.py", "train", "configs/experiments.yaml"),
    ("run_evaluate.py", "evaluate", "configs/experiments.yaml"),
    ("run_benchmark.py", "benchmark", "configs/benchmarks.yaml"),
]


def _load_script_module(script_name: str) -> Any:
    script_path = REPO_ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(f"test_{script_path.stem}", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_repo_yaml(name: str) -> dict[str, Any]:
    return load_config(
        CONFIGS_DIR / f"{name}.yaml",
        dotenv_path=NONEXISTENT_DOTENV,
        environ={},
    )


def test_package_metadata_version_and_exports() -> None:
    assert ie212.__version__ == "0.1.0"
    assert "__version__" in ie212.__all__


def test_parse_dotenv_handles_quotes_comments_exports_and_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.env"
    assert parse_dotenv(missing) == {}

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# Full-line comment",
                "",
                "INVALID_LINE_WITHOUT_EQUALS",
                "  =missing_key",
                "PLAIN=hello # trailing comment",
                'DOUBLE="a#b \\"quoted\\" \\\\slash" # outside comment',
                "SINGLE='literal # not comment'",
                "export EXPORTED_KEY=exported_val",
                "COMMENT_ONLY=# whole value is comment",
            ]
        ),
        encoding="utf-8",
    )

    before_env = dict(os.environ)
    parsed = parse_dotenv(env_file)
    assert dict(os.environ) == before_env
    assert parsed == {
        "PLAIN": "hello",
        "DOUBLE": 'a#b "quoted" \\slash',
        "SINGLE": "literal # not comment",
        "EXPORTED_KEY": "exported_val",
        "COMMENT_ONLY": "",
    }


def test_interpolate_env_vars_supports_defaults_nesting_and_errors() -> None:
    env = {"HOST": "minio", "EMPTY": "", "INNER": "fallback_val"}
    payload = {
        "url": "http://${HOST}:9000",
        "with_default": "${MISSING:-default_val}",
        "empty_triggers_default": "${EMPTY:-used_default}",
        "nested": "${MISSING:-${EMPTY:-${INNER}}}",
        "items": ["${HOST}", 42, True, None],
    }

    resolved = interpolate_env_vars(payload, env)
    assert resolved == {
        "url": "http://minio:9000",
        "with_default": "default_val",
        "empty_triggers_default": "used_default",
        "nested": "fallback_val",
        "items": ["minio", 42, True, None],
    }

    with pytest.raises(ConfigError, match="Missing required environment variable: REQ_VAR"):
        interpolate_env_vars("${REQ_VAR}", env)

    with pytest.raises(ConfigError, match="Unclosed environment variable placeholder"):
        interpolate_env_vars("${UNCLOSED", env)

    with pytest.raises(ConfigError, match="Unclosed environment variable placeholder"):
        interpolate_env_vars("${VAR:-abc", env)


def test_load_config_yaml_edge_cases(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")

    empty_yaml = tmp_path / "empty.yaml"
    empty_yaml.write_text("", encoding="utf-8")
    assert load_config(empty_yaml, dotenv_path=NONEXISTENT_DOTENV, environ={}) == {}

    list_yaml = tmp_path / "list.yaml"
    list_yaml.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="Expected YAML mapping"):
        load_config(list_yaml, dotenv_path=NONEXISTENT_DOTENV, environ={})


def test_load_config_sibling_dotenv_fallback_and_explicit_custom_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work_dir = tmp_path / "cwd_without_env"
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    # Case 1: sibling .env at config_path.parent / ".env"
    sibling_dir = tmp_path / "sibling_case"
    sibling_dir.mkdir()
    (sibling_dir / ".env").write_text("TEST_SECRET=from_sibling\n", encoding="utf-8")
    sibling_cfg = sibling_dir / "cfg.yaml"
    sibling_cfg.write_text("secret: ${TEST_SECRET}\n", encoding="utf-8")
    assert load_config(sibling_cfg, environ={}) == {"secret": "from_sibling"}

    # Case 2: parent .env at config_path.parent.parent / ".env"
    project_dir = tmp_path / "project_case"
    configs_sub = project_dir / "configs"
    configs_sub.mkdir(parents=True)
    (project_dir / ".env").write_text("TEST_SECRET=from_parent\n", encoding="utf-8")
    nested_cfg = configs_sub / "cfg.yaml"
    nested_cfg.write_text("secret: ${TEST_SECRET}\n", encoding="utf-8")
    assert load_config(nested_cfg, environ={}) == {"secret": "from_parent"}

    # Case 3: explicit custom dotenv_path != ".env" must NOT fall back to sibling/parent .env
    custom_missing = work_dir / "custom_missing.env"
    with pytest.raises(ConfigError, match="Missing required environment variable: TEST_SECRET"):
        load_config(nested_cfg, dotenv_path=custom_missing, environ={})

    custom_subdir = work_dir / "sub"
    custom_subdir.mkdir()
    with pytest.raises(ConfigError, match="Missing required environment variable: TEST_SECRET"):
        load_config(nested_cfg, dotenv_path=custom_subdir / ".env", environ={})

    custom_existing = work_dir / "other.env"
    custom_existing.write_text("TEST_SECRET=from_custom\n", encoding="utf-8")
    assert load_config(nested_cfg, dotenv_path=custom_existing, environ={}) == {
        "secret": "from_custom"
    }


@pytest.mark.parametrize("concurrency", [1, 2])
def test_validate_data_config_accepts_valid_concurrency(concurrency: int) -> None:
    cfg = _load_repo_yaml("data")
    cfg["download"]["max_concurrency"] = concurrency
    validate_data_config(cfg)


@pytest.mark.parametrize("bad_concurrency", [0, -1, 3, True, False, "2", 1.5, None])
def test_validate_data_config_rejects_invalid_concurrency(bad_concurrency: Any) -> None:
    cfg = _load_repo_yaml("data")
    cfg["download"]["max_concurrency"] = bad_concurrency
    with pytest.raises(ConfigError, match="max_concurrency"):
        validate_data_config(cfg)


def test_validate_data_config_dataset_schema_checks() -> None:
    base = _load_repo_yaml("data")
    validate_data_config(base)

    bad_casper_index = copy.deepcopy(base)
    bad_casper_index["datasets"]["casper_mscallgraph"]["has_leading_index"] = True
    with pytest.raises(ConfigError, match="casper_mscallgraph.has_leading_index"):
        validate_data_config(bad_casper_index)

    bad_casper_cols = copy.deepcopy(base)
    bad_casper_cols["datasets"]["casper_mscallgraph"]["columns"] = ["traceid", "timestamp"]
    with pytest.raises(ConfigError, match="casper_mscallgraph.columns"):
        validate_data_config(bad_casper_cols)

    bad_msres_index = copy.deepcopy(base)
    bad_msres_index["datasets"]["oss_msresource"]["has_leading_index"] = False
    with pytest.raises(ConfigError, match="oss_msresource.has_leading_index"):
        validate_data_config(bad_msres_index)

    bad_msres_ts = copy.deepcopy(base)
    bad_msres_ts["datasets"]["oss_msresource"]["columns"][-1] = "nodeid"
    with pytest.raises(ConfigError, match="oss_msresource.columns"):
        validate_data_config(bad_msres_ts)


def test_validate_lakehouse_config_rules() -> None:
    base = _load_repo_yaml("lakehouse")
    assert base["duckdb"]["access_delegation_mode"] == "none"
    validate_lakehouse_config(base)

    # Backward compatibility: access_delegation_mode may be omitted
    without_adm = copy.deepcopy(base)
    del without_adm["duckdb"]["access_delegation_mode"]
    validate_lakehouse_config(without_adm)

    for bad_adm in ("", "   ", 123):
        bad_cfg = copy.deepcopy(base)
        bad_cfg["duckdb"]["access_delegation_mode"] = bad_adm
        with pytest.raises(ConfigError, match="access_delegation_mode"):
            validate_lakehouse_config(bad_cfg)

    bad_endpoint = copy.deepcopy(base)
    bad_endpoint["minio"]["endpoint"] = "  "
    with pytest.raises(ConfigError, match="minio.endpoint"):
        validate_lakehouse_config(bad_endpoint)

    bad_buckets = copy.deepcopy(base)
    bad_buckets["minio"]["buckets"] = ["bronze", "silver"]
    with pytest.raises(ConfigError, match="minio.buckets"):
        validate_lakehouse_config(bad_buckets)

    bad_ns = copy.deepcopy(base)
    bad_ns["iceberg"]["namespaces"] = ["gold"]
    with pytest.raises(ConfigError, match="iceberg.namespaces"):
        validate_lakehouse_config(bad_ns)

    missing_duckdb = copy.deepcopy(base)
    del missing_duckdb["duckdb"]
    with pytest.raises(ConfigError, match="duckdb"):
        validate_lakehouse_config(missing_duckdb)


def test_validate_features_config_rules() -> None:
    base = _load_repo_yaml("features")
    assert base["feature_sets"]["C"]["host_aggregation_key"] == ["nodeid", "minute_bucket"]
    validate_features_config(base)

    for section in ("target", "temporal_split", "feature_sets"):
        missing = copy.deepcopy(base)
        del missing[section]
        with pytest.raises(ConfigError, match=section):
            validate_features_config(missing)

        non_map = copy.deepcopy(base)
        non_map[section] = ["invalid"]
        with pytest.raises(ConfigError, match=f"features.{section}"):
            validate_features_config(non_map)

    for fs_name in ("A", "B", "C"):
        missing_fs = copy.deepcopy(base)
        del missing_fs["feature_sets"][fs_name]
        with pytest.raises(ConfigError, match=fs_name):
            validate_features_config(missing_fs)


def test_validate_experiments_config_rules() -> None:
    base = _load_repo_yaml("experiments")
    validate_experiments_config(base)

    for section in ("run", "models", "evaluation"):
        missing = copy.deepcopy(base)
        del missing[section]
        with pytest.raises(ConfigError, match=section):
            validate_experiments_config(missing)

        non_map = copy.deepcopy(base)
        non_map[section] = "invalid"
        with pytest.raises(ConfigError, match=f"experiments.{section}"):
            validate_experiments_config(non_map)


def test_validate_benchmarks_config_rules() -> None:
    base = _load_repo_yaml("benchmarks")
    assert base["execution_policy"]["warmup_runs"] == 1
    assert base["execution_policy"]["measure_peak_ram"] is True
    assert base["execution_policy"]["include_jvm_memory"] is True
    assert base["execution_policy"]["measure_spill"] is True
    validate_benchmarks_config(base)

    # execution_policy is optional
    without_ep = copy.deepcopy(base)
    del without_ep["execution_policy"]
    validate_benchmarks_config(without_ep)

    bad_ep = copy.deepcopy(base)
    bad_ep["execution_policy"] = "invalid"
    with pytest.raises(ConfigError, match="benchmarks.execution_policy"):
        validate_benchmarks_config(bad_ep)

    for section in ("engines", "workloads", "scales", "repetitions"):
        missing = copy.deepcopy(base)
        del missing[section]
        with pytest.raises(ConfigError, match=section):
            validate_benchmarks_config(missing)

    empty_scales = copy.deepcopy(base)
    empty_scales["scales"] = []
    with pytest.raises(ConfigError, match="benchmarks.scales"):
        validate_benchmarks_config(empty_scales)

    for bad_reps in (0, -1, True, False, "3"):
        bad_rep_cfg = copy.deepcopy(base)
        bad_rep_cfg["repetitions"] = bad_reps
        with pytest.raises(ConfigError, match="benchmarks.repetitions"):
            validate_benchmarks_config(bad_rep_cfg)


def test_load_and_validate_project_configs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configs = load_project_configs(
        CONFIGS_DIR,
        dotenv_path=NONEXISTENT_DOTENV,
        environ={},
    )
    assert set(configs.keys()) == set(DEFAULT_CONFIG_FILES)
    validate_project_configs(configs)

    missing_key = dict(configs)
    del missing_key["benchmarks"]
    with pytest.raises(ConfigError, match="benchmarks"):
        validate_project_configs(missing_key)

    non_mapping = dict(configs)
    non_mapping["features"] = ["not", "a", "mapping"]  # type: ignore[assignment]
    with pytest.raises(ConfigError, match="project_configs.features"):
        validate_project_configs(non_mapping)

    # Verify load_project_configs(".") resolves parent .env when invoked from inside configs/
    project_root = tmp_path / "repo"
    cfg_dir = project_root / "configs"
    cfg_dir.mkdir(parents=True)
    for name in DEFAULT_CONFIG_FILES:
        src_content = (CONFIGS_DIR / f"{name}.yaml").read_text(encoding="utf-8")
        (cfg_dir / f"{name}.yaml").write_text(src_content, encoding="utf-8")
    (project_root / ".env").write_text("MINIO_ROOT_USER=dot_user\n", encoding="utf-8")
    monkeypatch.chdir(cfg_dir)
    loaded_from_dot = load_project_configs(".", environ={})
    assert loaded_from_dot["lakehouse"]["minio"]["access_key"] == "dot_user"


@pytest.mark.parametrize(("script_name", "_stage", "default_cfg"), STUB_CLIS)
def test_stub_cli_help(
    script_name: str,
    _stage: str,
    default_cfg: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mod = _load_script_module(script_name)
    with pytest.raises(SystemExit) as exc_info:
        mod.main(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "--check-config" in captured.out
    assert "--config-dir" in captured.out
    assert default_cfg in captured.out
    assert "planned stub" in captured.out.lower()


@pytest.mark.parametrize(("script_name", "stage", "default_cfg"), STUB_CLIS)
def test_stub_cli_check_config_succeeds(
    script_name: str,
    stage: str,
    default_cfg: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mod = _load_script_module(script_name)
    exit_code = mod.main(
        [
            "--check-config",
            "--config",
            str(REPO_ROOT / default_cfg),
            "--config-dir",
            str(CONFIGS_DIR),
            "--dotenv",
            str(NONEXISTENT_DOTENV),
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["stage"] == stage
    assert payload["status"] == "planned_stub"
    assert payload["check_config"] is True
    assert payload["project_configs"] == sorted(DEFAULT_CONFIG_FILES)


@pytest.mark.parametrize(("script_name", "stage", "_default_cfg"), STUB_CLIS)
def test_stub_cli_rejects_invalid_custom_config(
    script_name: str,
    stage: str,
    _default_cfg: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mod = _load_script_module(script_name)

    bad_schema_yaml = tmp_path / "invalid_schema.yaml"
    bad_schema_yaml.write_text("{}\n", encoding="utf-8")
    exit_code = mod.main(
        [
            "--check-config",
            "--config",
            str(bad_schema_yaml),
            "--config-dir",
            str(CONFIGS_DIR),
            "--dotenv",
            str(NONEXISTENT_DOTENV),
        ]
    )
    assert exit_code == 1
    assert f"Configuration error in stage '{stage}'" in capsys.readouterr().err

    malformed_yaml = tmp_path / "malformed.yaml"
    malformed_yaml.write_text(":\n  - [bad\n", encoding="utf-8")
    exit_code_malformed = mod.main(
        [
            "--check-config",
            "--config",
            str(malformed_yaml),
            "--config-dir",
            str(CONFIGS_DIR),
            "--dotenv",
            str(NONEXISTENT_DOTENV),
        ]
    )
    assert exit_code_malformed == 1
    assert f"Configuration error in stage '{stage}'" in capsys.readouterr().err

    exit_code_missing = mod.main(
        [
            "--check-config",
            "--config",
            str(tmp_path / "missing.yaml"),
            "--config-dir",
            str(CONFIGS_DIR),
            "--dotenv",
            str(NONEXISTENT_DOTENV),
        ]
    )
    assert exit_code_missing == 1
    assert f"Configuration error in stage '{stage}'" in capsys.readouterr().err
