"""YAML and environment configuration loader for IE212."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_FILES: tuple[str, ...] = (
    "data",
    "lakehouse",
    "features",
    "experiments",
    "benchmarks",
)

_VAR_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_EXPORT_PREFIX_RE = re.compile(r"^export\s+")


class ConfigError(ValueError):
    """Raised when configuration loading, interpolation, or validation fails."""


def _strip_quotes_and_inline_comment(raw_val: str) -> str:
    val = raw_val.strip()
    if not val:
        return ""
    if val[0] in ('"', "'"):
        quote = val[0]
        if len(val) >= 2:
            escaped = False
            for idx in range(1, len(val)):
                ch = val[idx]
                if quote == '"' and ch == "\\" and not escaped:
                    escaped = True
                    continue
                if ch == quote and not escaped:
                    inner = val[1:idx]
                    if quote == '"':
                        inner = inner.replace('\\"', '"').replace("\\\\", "\\")
                    return inner
                escaped = False
        return val
    if val.startswith("#"):
        return ""
    return re.split(r"\s+#", val, maxsplit=1)[0].strip()


def parse_dotenv(dotenv_path: str | Path = ".env") -> dict[str, str]:
    """Parse a .env file into a dictionary without mutating os.environ."""
    path = Path(dotenv_path)
    if not path.is_file():
        return {}

    parsed: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        export_match = _EXPORT_PREFIX_RE.match(line)
        if export_match:
            line = line[export_match.end() :]
        if "=" not in line:
            continue
        key, raw_val = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed[key] = _strip_quotes_and_inline_comment(raw_val)
    return parsed


def _find_matching_brace(text: str, start_idx: int) -> int:
    """Find the closing '}' matching an already-opened '{', tracking nested braces."""
    depth = 1
    idx = start_idx
    n = len(text)
    while idx < n:
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return idx
        idx += 1
    return -1


def _interpolate_string(
    text: str,
    lookup: Mapping[str, str],
    depth: int = 0,
    max_depth: int = 32,
) -> str:
    """Resolve ${VAR} and ${VAR:-default} placeholders with arbitrary nesting."""
    if depth > max_depth:
        raise ConfigError(
            f"Environment variable interpolation exceeded maximum nesting depth ({max_depth})"
        )

    out: list[str] = []
    idx = 0
    n = len(text)

    while idx < n:
        dollar_idx = text.find("${", idx)
        if dollar_idx == -1:
            out.append(text[idx:])
            break

        out.append(text[idx:dollar_idx])
        name_start = dollar_idx + 2
        name_match = _VAR_NAME_RE.match(text, name_start)
        if not name_match:
            out.append("${")
            idx = name_start
            continue

        var = name_match.group(0)
        after_name = name_match.end()

        if after_name == n:
            raise ConfigError(
                f"Unclosed environment variable placeholder starting at index {dollar_idx}"
            )

        if text[after_name] == "}":
            if var not in lookup:
                raise ConfigError(f"Missing required environment variable: {var}")
            out.append(lookup[var])
            idx = after_name + 1
            continue

        if text.startswith(":-", after_name):
            default_start = after_name + 2
            close_idx = _find_matching_brace(text, default_start)
            if close_idx == -1:
                raise ConfigError(
                    f"Unclosed environment variable placeholder starting at index {dollar_idx}"
                )
            if var in lookup and lookup[var] != "":
                out.append(lookup[var])
            else:
                default_expr = text[default_start:close_idx]
                out.append(
                    _interpolate_string(
                        default_expr,
                        lookup,
                        depth=depth + 1,
                        max_depth=max_depth,
                    )
                )
            idx = close_idx + 1
            continue

        out.append("${")
        idx = name_start

    return "".join(out)


def interpolate_env_vars(value: Any, env: Mapping[str, str] | None = None) -> Any:
    """Recursively interpolate ${VAR} and ${VAR:-default} placeholders."""
    lookup: Mapping[str, str] = os.environ if env is None else env

    if isinstance(value, Mapping):
        return {k: interpolate_env_vars(v, lookup) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env_vars(item, lookup) for item in value]
    if isinstance(value, str):
        return _interpolate_string(value, lookup)
    return value


def load_config(
    path: str | Path,
    *,
    dotenv_path: str | Path | None = ".env",
    interpolate: bool = True,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Load a single YAML config file with optional .env and env-var interpolation."""
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    raw_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if raw_data is None:
        data: dict[str, Any] = {}
    elif isinstance(raw_data, dict):
        data = raw_data
    else:
        raise ConfigError(
            f"Expected YAML mapping at root of {config_path}, got {type(raw_data).__name__}"
        )

    if not interpolate:
        return data

    merged_env: dict[str, str] = {}
    resolved_dotenv: Path | None = None
    if dotenv_path is not None:
        dotenv_candidate = Path(dotenv_path)
        if dotenv_candidate.is_file():
            resolved_dotenv = dotenv_candidate
        elif dotenv_candidate == Path(".env"):
            resolved_cfg = config_path.resolve()
            for probe in (resolved_cfg.parent / ".env", resolved_cfg.parent.parent / ".env"):
                if probe.is_file():
                    resolved_dotenv = probe
                    break
    if resolved_dotenv is not None:
        merged_env.update(parse_dotenv(resolved_dotenv))
    merged_env.update(os.environ if environ is None else environ)
    return interpolate_env_vars(data, merged_env)


def validate_required_keys(
    config: Mapping[str, Any],
    required_keys: Iterable[str],
    *,
    context: str = "config",
) -> None:
    """Validate that required keys are present in a mapping."""
    if not isinstance(config, Mapping):
        raise ConfigError(f"{context} must be a mapping, got {type(config).__name__}")
    missing = [key for key in required_keys if key not in config]
    if missing:
        raise ConfigError(f"Missing required keys in {context}: {', '.join(missing)}")


def _require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{context} must be a mapping, got {type(value).__name__}")
    return value


def _require_sequence(value: Any, *, context: str) -> list[Any] | tuple[Any, ...] | set[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple, set)):
        raise ConfigError(f"{context} must be a list/sequence, got {type(value).__name__}")
    return value


def validate_data_config(data_cfg: Mapping[str, Any]) -> None:
    """Validate data.yaml structure and dataset schema invariants."""
    validate_required_keys(
        data_cfg,
        ("paths", "sources", "datasets", "download", "bronze"),
        context="data",
    )
    for section in ("paths", "sources", "datasets", "download", "bronze"):
        _require_mapping(data_cfg[section], context=f"data.{section}")

    datasets = data_cfg["datasets"]
    if "casper_mscallgraph" not in datasets:
        raise ConfigError("data.datasets must define 'casper_mscallgraph'")
    casper = _require_mapping(
        datasets["casper_mscallgraph"],
        context="data.datasets.casper_mscallgraph",
    )
    if casper.get("has_leading_index") is not False:
        raise ConfigError("data.datasets.casper_mscallgraph.has_leading_index must be False")
    casper_cols = casper.get("columns")
    if (
        isinstance(casper_cols, (str, bytes))
        or not isinstance(casper_cols, (list, tuple))
        or len(casper_cols) != 8
    ):
        raise ConfigError("data.datasets.casper_mscallgraph.columns must contain exactly 8 columns")

    if "oss_msresource" not in datasets:
        raise ConfigError("data.datasets must define 'oss_msresource'")
    msres = _require_mapping(
        datasets["oss_msresource"],
        context="data.datasets.oss_msresource",
    )
    if msres.get("has_leading_index") is not True:
        raise ConfigError("data.datasets.oss_msresource.has_leading_index must be True")
    if msres.get("timestamp_column_position") != "last":
        raise ConfigError("data.datasets.oss_msresource.timestamp_column_position must be 'last'")
    msres_cols = msres.get("columns")
    if (
        isinstance(msres_cols, (str, bytes))
        or not isinstance(msres_cols, (list, tuple))
        or not msres_cols
        or msres_cols[-1] != "timestamp"
    ):
        raise ConfigError("data.datasets.oss_msresource.columns must end with 'timestamp'")

    download = data_cfg["download"]
    max_concurrency = download.get("max_concurrency")
    if (
        not isinstance(max_concurrency, int)
        or isinstance(max_concurrency, bool)
        or max_concurrency < 1
        or max_concurrency > 2
    ):
        raise ConfigError("data.download.max_concurrency must be an integer between 1 and 2")


def validate_lakehouse_config(lakehouse_cfg: Mapping[str, Any]) -> None:
    """Validate lakehouse.yaml structure, buckets, and Iceberg namespaces."""
    validate_required_keys(
        lakehouse_cfg,
        ("minio", "iceberg", "spark", "duckdb", "tables"),
        context="lakehouse",
    )
    for section in ("minio", "iceberg", "spark", "duckdb", "tables"):
        _require_mapping(lakehouse_cfg[section], context=f"lakehouse.{section}")

    minio = lakehouse_cfg["minio"]
    validate_required_keys(minio, ("endpoint", "buckets"), context="lakehouse.minio")
    endpoint = minio["endpoint"]
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ConfigError("lakehouse.minio.endpoint must be a non-empty string")
    raw_buckets = _require_sequence(minio["buckets"], context="lakehouse.minio.buckets")
    if not all(isinstance(bucket, str) for bucket in raw_buckets):
        raise ConfigError("lakehouse.minio.buckets entries must be strings")
    buckets = set(raw_buckets)
    required_buckets = {"bronze", "silver", "gold"}
    if not required_buckets.issubset(buckets):
        raise ConfigError(
            f"lakehouse.minio.buckets must include {sorted(required_buckets)}, got {sorted(buckets)}"
        )

    iceberg = lakehouse_cfg["iceberg"]
    validate_required_keys(
        iceberg,
        ("catalog_name", "catalog_uri", "warehouse", "namespaces"),
        context="lakehouse.iceberg",
    )
    raw_namespaces = _require_sequence(iceberg["namespaces"], context="lakehouse.iceberg.namespaces")
    if not all(isinstance(ns, str) for ns in raw_namespaces):
        raise ConfigError("lakehouse.iceberg.namespaces entries must be strings")
    namespaces = set(raw_namespaces)
    required_namespaces = {"silver", "gold"}
    if not required_namespaces.issubset(namespaces):
        raise ConfigError(
            f"lakehouse.iceberg.namespaces must include {sorted(required_namespaces)}, got {sorted(namespaces)}"
        )

    duckdb_cfg = lakehouse_cfg["duckdb"]
    if "access_delegation_mode" in duckdb_cfg and duckdb_cfg["access_delegation_mode"] is not None:
        adm = duckdb_cfg["access_delegation_mode"]
        if not isinstance(adm, str) or not adm.strip():
            raise ConfigError("lakehouse.duckdb.access_delegation_mode must be a non-empty string")


def validate_features_config(features_cfg: Mapping[str, Any]) -> None:
    """Validate features.yaml target, temporal split, and A/B/C feature sets."""
    validate_required_keys(
        features_cfg,
        ("target", "temporal_split", "feature_sets"),
        context="features",
    )
    for key in ("target", "temporal_split", "feature_sets"):
        _require_mapping(features_cfg[key], context=f"features.{key}")

    feature_sets = features_cfg["feature_sets"]
    validate_required_keys(
        feature_sets,
        ("A", "B", "C"),
        context="features.feature_sets",
    )
    for key in ("A", "B", "C"):
        _require_mapping(feature_sets[key], context=f"features.feature_sets.{key}")


def validate_experiments_config(experiments_cfg: Mapping[str, Any]) -> None:
    """Validate experiments.yaml run, models, and evaluation sections."""
    validate_required_keys(
        experiments_cfg,
        ("run", "models", "evaluation"),
        context="experiments",
    )
    for key in ("run", "models", "evaluation"):
        _require_mapping(experiments_cfg[key], context=f"experiments.{key}")


def validate_benchmarks_config(benchmarks_cfg: Mapping[str, Any]) -> None:
    """Validate benchmarks.yaml engines, workloads, scales, repetitions, and execution_policy."""
    validate_required_keys(
        benchmarks_cfg,
        ("engines", "workloads", "scales", "repetitions"),
        context="benchmarks",
    )
    for key in ("engines", "workloads"):
        _require_mapping(benchmarks_cfg[key], context=f"benchmarks.{key}")
    scales = _require_sequence(benchmarks_cfg["scales"], context="benchmarks.scales")
    if not scales:
        raise ConfigError("benchmarks.scales must be a non-empty sequence")
    repetitions = benchmarks_cfg["repetitions"]
    if not isinstance(repetitions, int) or isinstance(repetitions, bool) or repetitions < 1:
        raise ConfigError("benchmarks.repetitions must be an integer >= 1")
    if "execution_policy" in benchmarks_cfg:
        _require_mapping(benchmarks_cfg["execution_policy"], context="benchmarks.execution_policy")


def validate_project_configs(configs: Mapping[str, Mapping[str, Any]]) -> None:
    """Validate all five IE212 YAML configuration sections."""
    validate_required_keys(configs, DEFAULT_CONFIG_FILES, context="project_configs")
    for name in DEFAULT_CONFIG_FILES:
        _require_mapping(configs[name], context=f"project_configs.{name}")

    validate_data_config(configs["data"])
    validate_lakehouse_config(configs["lakehouse"])
    validate_features_config(configs["features"])
    validate_experiments_config(configs["experiments"])
    validate_benchmarks_config(configs["benchmarks"])


def load_project_configs(
    config_dir: str | Path = "configs",
    *,
    dotenv_path: str | Path | None = ".env",
    interpolate: bool = True,
    validate: bool = True,
    environ: Mapping[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Load all five project configuration files from config_dir."""
    cfg_dir = Path(config_dir)
    resolved_dotenv: str | Path | None = dotenv_path
    if dotenv_path is not None and Path(dotenv_path) == Path(".env") and not Path(".env").is_file():
        candidate = cfg_dir.resolve().parent / ".env"
        if candidate.is_file():
            resolved_dotenv = candidate

    configs: dict[str, dict[str, Any]] = {}
    for name in DEFAULT_CONFIG_FILES:
        configs[name] = load_config(
            cfg_dir / f"{name}.yaml",
            dotenv_path=resolved_dotenv,
            interpolate=interpolate,
            environ=environ,
        )

    if validate:
        validate_project_configs(configs)
    return configs


__all__ = [
    "ConfigError",
    "DEFAULT_CONFIG_FILES",
    "interpolate_env_vars",
    "load_config",
    "load_project_configs",
    "parse_dotenv",
    "validate_benchmarks_config",
    "validate_data_config",
    "validate_experiments_config",
    "validate_features_config",
    "validate_lakehouse_config",
    "validate_project_configs",
    "validate_required_keys",
]
