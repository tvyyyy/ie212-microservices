"""S3 / MinIO upload helpers for Bronze Parquet partitions using path-style boto3."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotocoreConfig

from ie212.ingestion.schemas import BronzeError, DATASET_TO_LAKEHOUSE_TABLE, DatasetSchema


class S3UploadError(BronzeError):
    """Raised when MinIO/S3 configuration or upload fails."""


def resolve_s3_settings(
    lakehouse_cfg: Mapping[str, Any],
    *,
    endpoint_override: str | None = None,
    use_local_endpoint: bool = False,
) -> dict[str, Any]:
    """Extract MinIO/S3 settings from configs/lakehouse.yaml."""
    minio_cfg = lakehouse_cfg.get("minio")
    if not isinstance(minio_cfg, Mapping):
        raise S3UploadError("lakehouse.minio configuration section is missing.")

    endpoint = (
        endpoint_override
        or (minio_cfg.get("local_endpoint") if use_local_endpoint else None)
        or minio_cfg.get("endpoint")
    )
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise S3UploadError("MinIO endpoint URL is missing or empty.")

    access_key = str(minio_cfg.get("access_key", "") or "").strip()
    secret_key = str(minio_cfg.get("secret_key", "") or "").strip()
    region = str(minio_cfg.get("region", "us-east-1") or "us-east-1").strip()
    path_style_access = bool(minio_cfg.get("path_style_access", True))
    use_ssl = bool(minio_cfg.get("use_ssl", False))

    return {
        "endpoint_url": endpoint.strip(),
        "access_key": access_key,
        "secret_key": secret_key,
        "region_name": region,
        "path_style_access": path_style_access,
        "use_ssl": use_ssl,
    }


def build_s3_client(
    lakehouse_cfg: Mapping[str, Any],
    *,
    endpoint_override: str | None = None,
    use_local_endpoint: bool = False,
) -> Any:
    """Create a path-style boto3 S3 client for MinIO."""
    settings = resolve_s3_settings(
        lakehouse_cfg,
        endpoint_override=endpoint_override,
        use_local_endpoint=use_local_endpoint,
    )
    if not settings["access_key"] or not settings["secret_key"]:
        raise S3UploadError(
            "Missing MinIO/S3 credentials: set MINIO_ACCESS_KEY/MINIO_SECRET_KEY "
            "or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD before uploading."
        )

    boto_cfg = BotocoreConfig(
        s3={"addressing_style": "path" if settings["path_style_access"] else "virtual"}
    )
    return boto3.client(
        "s3",
        endpoint_url=settings["endpoint_url"],
        aws_access_key_id=settings["access_key"],
        aws_secret_access_key=settings["secret_key"],
        region_name=settings["region_name"],
        use_ssl=settings["use_ssl"],
        config=boto_cfg,
    )


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Split an s3://bucket/prefix URI into (bucket, key_prefix)."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise S3UploadError(f"Invalid S3 URI '{s3_uri}'. Expected s3://<bucket>/<prefix>.")
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")


def resolve_bronze_s3_target(
    dataset_name: str,
    lakehouse_cfg: Mapping[str, Any],
    schema: DatasetSchema | None = None,
) -> tuple[str, str]:
    """Resolve target (bucket, table_prefix) for a dataset from lakehouse.yaml."""
    if schema is not None and schema.s3_table_uri:
        return parse_s3_uri(schema.s3_table_uri)

    tables_cfg = lakehouse_cfg.get("tables", {})
    bronze_tables = tables_cfg.get("bronze", {}) if isinstance(tables_cfg, Mapping) else {}
    table_key = DATASET_TO_LAKEHOUSE_TABLE.get(dataset_name, dataset_name)
    if isinstance(bronze_tables, Mapping) and table_key in bronze_tables:
        return parse_s3_uri(str(bronze_tables[table_key]))
    return "bronze", table_key


def upload_partition_to_s3(
    local_files: Sequence[str | Path],
    *,
    dataset: str,
    source_file_stem: str,
    lakehouse_cfg: Mapping[str, Any],
    schema: DatasetSchema | None = None,
    endpoint_override: str | None = None,
    use_local_endpoint: bool = False,
    s3_client: Any = None,
) -> list[str]:
    """Upload partitioned Bronze Parquet files to MinIO/S3 using path-style addressing."""
    if not local_files:
        return []

    client = s3_client or build_s3_client(
        lakehouse_cfg,
        endpoint_override=endpoint_override,
        use_local_endpoint=use_local_endpoint,
    )
    bucket, table_prefix = resolve_bronze_s3_target(dataset, lakehouse_cfg, schema=schema)
    uploaded_uris: list[str] = []

    for local_file in local_files:
        path = Path(local_file)
        if not path.is_file():
            raise S3UploadError(f"Cannot upload missing Parquet file: {path}")
        prefix_part = f"{table_prefix}/" if table_prefix else ""
        key = f"{prefix_part}dataset={dataset}/source_file_stem={source_file_stem}/{path.name}"
        client.upload_file(str(path), bucket, key)
        uploaded_uris.append(f"s3://{bucket}/{key}")

    return uploaded_uris


__all__ = [
    "S3UploadError",
    "build_s3_client",
    "parse_s3_uri",
    "resolve_bronze_s3_target",
    "resolve_s3_settings",
    "upload_partition_to_s3",
]
