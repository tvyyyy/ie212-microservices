"""CLI entrypoint for the T08 Iceberg Spark 4.1.4 & DuckDB 2.0.1 lakehouse smoke test."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

from ie212.config import ConfigError, load_config, load_project_configs
from ie212.smoke_test import (
    DEFAULT_NAMESPACE,
    DEFAULT_TABLE,
    LakehouseConnectionSettings,
    SmokeTestError,
    collect_runtime_secrets,
    resolve_lakehouse_settings,
    run_smoke_test,
    sanitize_secret_text,
)

STAGE = "smoke_test"
DEFAULT_CONFIG = "configs/lakehouse.yaml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IE212 Iceberg Spark & DuckDB lakehouse smoke test CLI (T08).",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to lakehouse YAML configuration (default: {DEFAULT_CONFIG}).",
    )
    parser.add_argument(
        "--config-dir",
        default="configs",
        help="Path to directory containing project YAML configurations (default: configs).",
    )
    parser.add_argument(
        "--dotenv",
        default=".env",
        help="Optional .env file for environment variable interpolation (default: .env).",
    )
    parser.add_argument(
        "--mode",
        choices=("local", "duckdb", "spark", "all"),
        default="local",
        help="Smoke test execution mode (default: local).",
    )
    parser.add_argument(
        "--target",
        choices=("container", "docker", "local"),
        default="container",
        help="Endpoint target: 'container'/'docker' (minio:9000, iceberg-rest:8181) or 'local' (localhost) (default: container).",
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help=f"Target Iceberg namespace (default: {DEFAULT_NAMESPACE}).",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Target smoke test table name (default: {DEFAULT_TABLE}).",
    )
    parser.add_argument(
        "--fallback-dir",
        default=None,
        help="Directory for Parquet fallback export/read (defaults to lakehouse.duckdb.parquet_fallback_dir).",
    )
    parser.add_argument(
        "--export-fallback",
        action="store_true",
        help="Export Parquet snapshot after Spark Iceberg write/read verification.",
    )
    parser.add_argument(
        "--fallback-on-error",
        action="store_true",
        help="In duckdb/all mode, fall back to Parquet read_parquet() if DuckDB Iceberg ATTACH/read fails.",
    )
    parser.add_argument(
        "--attach-target",
        default="",
        help="Optional Iceberg ATTACH catalog database target string (default: '').",
    )
    parser.add_argument(
        "--cleanup-local",
        action="store_true",
        help="Remove generated local Parquet file after --mode local verification completes.",
    )
    parser.add_argument(
        "--dry-run",
        "--print-plan",
        dest="dry_run",
        action="store_true",
        help="Validate configuration and print redacted Spark/DuckDB execution plan without connecting.",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate lakehouse and project YAML configurations and exit.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    lakehouse_cfg: dict[str, Any] | None = None
    settings: LakehouseConnectionSettings | None = None
    try:
        lakehouse_cfg = load_config(args.config, dotenv_path=args.dotenv)
        if args.check_config:
            project_configs = load_project_configs(args.config_dir, dotenv_path=args.dotenv)
            settings = resolve_lakehouse_settings(
                lakehouse_cfg,
                target=args.target,
                require_credentials=False,
            )
            payload = {
                "stage": STAGE,
                "status": "ok",
                "check_config": True,
                "config_loaded": str(Path(args.config)),
                "project_configs": sorted(project_configs.keys()),
                "settings": settings.to_redacted_dict(),
            }
            print(json.dumps(payload, indent=2))
            return 0

        is_live_mode = args.mode in ("duckdb", "spark", "all") and not args.dry_run
        settings = resolve_lakehouse_settings(
            lakehouse_cfg,
            target=args.target,
            require_credentials=is_live_mode,
        )
        payload = run_smoke_test(
            settings,
            mode=args.mode,
            namespace=args.namespace,
            table=args.table,
            fallback_dir=args.fallback_dir,
            export_fallback=args.export_fallback,
            fallback_on_error=args.fallback_on_error,
            attach_target=args.attach_target,
            dry_run=args.dry_run,
            cleanup_local=args.cleanup_local,
        )
    except (FileNotFoundError, ConfigError, SmokeTestError, yaml.YAMLError) as exc:
        secrets = collect_runtime_secrets(
            settings,
            lakehouse_cfg=lakehouse_cfg,
            dotenv_path=args.dotenv,
        )
        safe_msg = sanitize_secret_text(str(exc), secrets)
        print(f"Smoke test error in stage '{STAGE}': {safe_msg}", file=sys.stderr)
        return 1
    except Exception as exc:
        # Defensive catch so unexpected runtime errors still exit cleanly with non-zero status.
        secrets = collect_runtime_secrets(
            settings,
            lakehouse_cfg=lakehouse_cfg,
            dotenv_path=args.dotenv,
        )
        safe_msg = sanitize_secret_text(str(exc), secrets)
        print(f"Unexpected smoke test error in stage '{STAGE}': {safe_msg}", file=sys.stderr)
        return 1

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
