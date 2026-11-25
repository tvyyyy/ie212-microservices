"""CLI entrypoint for the idempotent Bronze ingestion stage (T09)."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from ie212.config import (
    ConfigError,
    load_config,
    load_project_configs,
    validate_data_config,
    validate_lakehouse_config,
)
from ie212.ingestion.bronze import ingest_bronze
from ie212.ingestion.manifest import DEFAULT_BRONZE_MANIFEST_PATH, validate_manifest_path
from ie212.ingestion.schemas import BronzeError

STAGE = "bronze"
DEFAULT_CONFIG = "configs/data.yaml"
DEFAULT_LAKEHOUSE_CONFIG = "configs/lakehouse.yaml"
DATASET_CHOICES = (
    "casper_mscallgraph",
    "oss_node",
    "oss_msresource",
    "oss_msrtqps",
    "oss_mscallgraph",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IE212 Idempotent Bronze Ingestion Pipeline CLI (T09).",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to primary data YAML configuration (default: {DEFAULT_CONFIG}).",
    )
    parser.add_argument(
        "--lakehouse-config",
        default=DEFAULT_LAKEHOUSE_CONFIG,
        help=f"Path to lakehouse YAML configuration (default: {DEFAULT_LAKEHOUSE_CONFIG}).",
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
        help="Validate Bronze and project YAML configurations and exit.",
    )
    parser.add_argument(
        "--dataset",
        choices=DATASET_CHOICES,
        help="Target Bronze dataset key from configs/data.yaml.",
    )
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        nargs="+",
        default=[],
        help="Local .csv, .csv.gz, or .tar.gz input file(s) (repeatable).",
    )
    parser.add_argument(
        "--url",
        dest="urls",
        action="append",
        nargs="+",
        default=[],
        help="Remote HTTP/OSS archive URL(s) to stream with resumable Range downloads (repeatable).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override Bronze Parquet output directory (default from configs/data.yaml).",
    )
    parser.add_argument(
        "--download-dir",
        default=None,
        help="Optional directory for temporary downloaded archives (default: ephemeral temp dir).",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help=(
            "Path to Bronze JSON manifest (default: <bronze.manifest_dir>/bronze_manifest.json "
            "from configs/data.yaml; refuses protected sample_manifest.json)."
        ),
    )
    parser.add_argument(
        "--compression",
        choices=("zstd", "snappy"),
        default=None,
        help="Parquet compression codec (default from configs/data.yaml: zstd).",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=100_000,
        help="Number of CSV rows per streaming chunk (default: 100000).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        choices=(1, 2),
        default=None,
        help="Bounded low-concurrency worker count (1 or 2).",
    )
    parser.add_argument(
        "--expected-sha256",
        default=None,
        help="Optional expected SHA-256 hex digest for single-input verification.",
    )
    parser.add_argument(
        "--expected-md5",
        default=None,
        help="Optional expected MD5 hex digest for single-input verification.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion and atomic partition overwrite even if checksums match.",
    )
    parser.add_argument(
        "--cleanup-temp-archives",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Delete temporary downloaded .tar.gz/.csv.gz archives after ingestion (default from config).",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload partitioned Bronze Parquet files to MinIO/S3 using lakehouse.yaml.",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="Optional MinIO/S3 endpoint URL override when --upload-s3 is enabled.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate schemas/inputs and report planned Bronze outputs without writing Parquet.",
    )
    return parser


def _flatten(nested: Sequence[Sequence[str]]) -> list[str]:
    return [item for group in nested for item in group]


def _resolve_manifest_path(manifest_path: str | None, data_cfg: Mapping[str, Any]) -> Path:
    if manifest_path is not None:
        return Path(manifest_path)
    bronze_cfg = data_cfg.get("bronze", {})
    if isinstance(bronze_cfg, Mapping):
        manifest_dir = bronze_cfg.get("manifest_dir")
        if isinstance(manifest_dir, str) and manifest_dir.strip():
            return Path(manifest_dir.strip()) / "bronze_manifest.json"
    return DEFAULT_BRONZE_MANIFEST_PATH


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        data_cfg = load_config(args.config, dotenv_path=args.dotenv)
        validate_data_config(data_cfg)
        lakehouse_cfg = load_config(args.lakehouse_config, dotenv_path=args.dotenv)
        validate_lakehouse_config(lakehouse_cfg)
        project_configs = load_project_configs(args.config_dir, dotenv_path=args.dotenv)
        effective_manifest_path = _resolve_manifest_path(args.manifest_path, data_cfg)
        validate_manifest_path(effective_manifest_path)
    except (FileNotFoundError, ConfigError, ValueError, yaml.YAMLError) as exc:
        print(f"Configuration error in stage '{STAGE}': {exc}", file=sys.stderr)
        return 1

    if args.check_config:
        payload = {
            "stage": STAGE,
            "status": "config_valid",
            "config_loaded": str(Path(args.config)),
            "lakehouse_config_loaded": str(Path(args.lakehouse_config)),
            "project_configs": sorted(project_configs.keys()),
            "datasets": sorted(data_cfg.get("datasets", {}).keys()),
            "manifest_path": str(effective_manifest_path),
            "check_config": True,
        }
        print(json.dumps(payload, indent=2))
        return 0

    if not args.dataset:
        print("Error: --dataset is required unless --check-config is specified.", file=sys.stderr)
        return 2

    flat_inputs = _flatten(args.inputs)
    flat_urls = _flatten(args.urls)
    if not flat_inputs and not flat_urls:
        print(
            "Error: provide at least one --input file or --url (or pass --check-config).",
            file=sys.stderr,
        )
        return 2

    try:
        summary = ingest_bronze(
            dataset=args.dataset,
            input_paths=flat_inputs,
            urls=flat_urls,
            output_dir=args.output_dir,
            manifest_path=effective_manifest_path,
            download_dir=args.download_dir,
            data_cfg=data_cfg,
            lakehouse_cfg=lakehouse_cfg,
            compression=args.compression,
            chunk_rows=args.chunk_rows,
            concurrency=args.concurrency,
            expected_sha256=args.expected_sha256,
            expected_md5=args.expected_md5,
            force=args.force,
            cleanup_temp_archives=args.cleanup_temp_archives,
            upload_s3=args.upload_s3,
            s3_endpoint=args.s3_endpoint,
            dry_run=args.dry_run,
        )
    except (BronzeError, FileNotFoundError, ValueError, OSError) as exc:
        print(f"Bronze ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
