"""CLI entrypoint stub for the Gold lakehouse aggregation stage (planned: T17-T20)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

import yaml

from ie212.config import ConfigError, load_config, load_project_configs, validate_lakehouse_config

STAGE = "gold"
DEFAULT_CONFIG = "configs/lakehouse.yaml"
PLANNED_TASKS = "T17-T20"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IE212 Gold lakehouse aggregation CLI (planned stub).",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to primary stage YAML configuration (default: {DEFAULT_CONFIG}).",
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
        "--check-config",
        action="store_true",
        help="Validate stage and project YAML configurations and exit.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        stage_cfg = load_config(args.config, dotenv_path=args.dotenv)
        validate_lakehouse_config(stage_cfg)
        project_configs = load_project_configs(args.config_dir, dotenv_path=args.dotenv)
    except (FileNotFoundError, ConfigError, yaml.YAMLError) as exc:
        print(f"Configuration error in stage '{STAGE}': {exc}", file=sys.stderr)
        return 1

    payload = {
        "stage": STAGE,
        "status": "planned_stub",
        "planned_tasks": PLANNED_TASKS,
        "config_loaded": str(Path(args.config)),
        "project_configs": sorted(project_configs.keys()),
        "check_config": bool(args.check_config),
        "message": f"Stage '{STAGE}' CLI stub validated configuration; pipeline execution is scheduled for {PLANNED_TASKS}.",
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
