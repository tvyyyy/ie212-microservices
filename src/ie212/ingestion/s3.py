"""S3 / MinIO upload helpers for Bronze Parquet partitions using path-style boto3."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import BotoCoreError, ClientError

from ie212.ingestion.schemas import BronzeError, DATASET_TO_LAKEHOUSE_TABLE, DatasetSchema

_TRUE_BOOL_STRINGS: frozenset[str] = frozenset({"true", "1", "yes", "on"})
_FALSE_BOOL_STRINGS: frozenset[str] = frozenset({"false", "0", "no", "off"})


class S3UploadError(BronzeError):
    """Raised when MinIO/S3 configuration or upload fails."""


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_BOOL_STRINGS:
            return True
        if normalized in _FALSE_BOOL_STRINGS:
            return False
    raise S3UploadError(
        f"Invalid boolean value for {field_name}: {value!r} "
        "(expected bool or true/false, 1/0, yes/no, on/off)."
    )


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value is not None:
            cleaned = str(value).strip()
            if cleaned:
                return cleaned
    return ""


def resolve_s3_settings(
    lakehouse_cfg: Mapping[str, Any],
    *,
    endpoint_override: str | None = None,
    use_local_endpoint: bool = False,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Extract MinIO/S3 settings from configs/lakehouse.yaml with environment fallbacks."""
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

    env = os.environ if environ is None else environ
    access_key = _first_non_empty(
        minio_cfg.get("access_key"),
        env.get("AWS_ACCESS_KEY_ID"),
        env.get("MINIO_ACCESS_KEY"),
        env.get("MINIO_ROOT_USER"),
    )
    secret_key = _first_non_empty(
        minio_cfg.get("secret_key"),
        env.get("AWS_SECRET_ACCESS_KEY"),
        env.get("MINIO_SECRET_KEY"),
        env.get("MINIO_ROOT_PASSWORD"),
    )
    region = _first_non_empty(minio_cfg.get("region"), env.get("AWS_REGION"), "us-east-1")
    path_style_access = _coerce_bool(minio_cfg.get("path_style_access", True), "path_style_access")
    use_ssl = _coerce_bool(minio_cfg.get("use_ssl", False), "use_ssl")

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
    environ: Mapping[str, str] | None = None,
) -> Any:
    """Create a path-style boto3 S3 client for MinIO."""
    settings = resolve_s3_settings(
        lakehouse_cfg,
        endpoint_override=endpoint_override,
        use_local_endpoint=use_local_endpoint,
        environ=environ,
    )
    if not settings["access_key"] or not settings["secret_key"]:
        raise S3UploadError(
            "Missing MinIO/S3 credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, "
            "MINIO_ACCESS_KEY/MINIO_SECRET_KEY, or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
            "before uploading."
        )

    boto_cfg = BotocoreConfig(
        s3={"addressing_style": "path" if settings["path_style_access"] else "virtual"}
    )
    try:
        return boto3.client(
            "s3",
            endpoint_url=settings["endpoint_url"],
            aws_access_key_id=settings["access_key"],
            aws_secret_access_key=settings["secret_key"],
            region_name=settings["region_name"],
            use_ssl=settings["use_ssl"],
            config=boto_cfg,
        )
    except (BotoCoreError, ClientError, ValueError) as exc:
        raise S3UploadError(f"Failed to initialize MinIO/S3 client: {exc}") from exc


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


def _delete_stale_partition_parts(
    client: Any,
    *,
    bucket: str,
    partition_prefix: str,
    uploaded_keys: set[str],
) -> None:
    if not hasattr(client, "list_objects_v2"):
        return

    stale_keys: list[str] = []
    continuation_token: str | None = None
    try:
        while True:
            params: dict[str, Any] = {"Bucket": bucket, "Prefix": partition_prefix}
            if continuation_token is not None:
                params["ContinuationToken"] = continuation_token
            response = client.list_objects_v2(**params)
            if isinstance(response, Mapping):
                for obj in response.get("Contents", []) or ():
                    if not isinstance(obj, Mapping):
                        continue
                    key = str(obj.get("Key", ""))
                    if not key.startswith(partition_prefix):
                        continue
                    rel = key[len(partition_prefix) :]
                    if (
                        "/" not in rel
                        and rel.startswith("part-")
                        and rel.endswith(".parquet")
                        and key not in uploaded_keys
                    ):
                        stale_keys.append(key)
                if response.get("IsTruncated"):
                    next_token = response.get("NextContinuationToken")
                    if not next_token:
                        break
                    continuation_token = str(next_token)
                else:
                    break
            else:
                break

        if not stale_keys:
            return

        for start in range(0, len(stale_keys), 1000):
            batch = stale_keys[start : start + 1000]
            if hasattr(client, "delete_objects"):
                del_resp = client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                )
                if isinstance(del_resp, Mapping) and del_resp.get("Errors"):
                    raise S3UploadError(
                        f"Failed deleting stale S3 partition objects under s3://{bucket}/{partition_prefix}: "
                        f"{del_resp['Errors']}"
                    )
            elif hasattr(client, "delete_object"):
                for key in batch:
                    client.delete_object(Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as exc:
        raise S3UploadError(
            f"Failed cleaning up stale S3 objects under s3://{bucket}/{partition_prefix}: {exc}"
        ) from exc


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

    resolved_paths = [Path(local_file) for local_file in local_files]
    for path in resolved_paths:
        if not path.is_file():
            raise S3UploadError(f"Cannot upload missing Parquet file: {path}")

    client = s3_client or build_s3_client(
        lakehouse_cfg,
        endpoint_override=endpoint_override,
        use_local_endpoint=use_local_endpoint,
    )
    bucket, table_prefix = resolve_bronze_s3_target(dataset, lakehouse_cfg, schema=schema)
    prefix_part = f"{table_prefix}/" if table_prefix else ""
    partition_prefix = f"{prefix_part}dataset={dataset}/source_file_stem={source_file_stem}/"
    uploaded_uris: list[str] = []
    uploaded_keys: set[str] = set()

    for path in resolved_paths:
        key = f"{partition_prefix}{path.name}"
        try:
            client.upload_file(str(path), bucket, key)
        except (BotoCoreError, ClientError) as exc:
            raise S3UploadError(
                f"Failed uploading {path} to s3://{bucket}/{key}: {exc}"
            ) from exc
        uploaded_keys.add(key)
        uploaded_uris.append(f"s3://{bucket}/{key}")

    _delete_stale_partition_parts(
        client,
        bucket=bucket,
        partition_prefix=partition_prefix,
        uploaded_keys=uploaded_keys,
    )
    return uploaded_uris


__all__ = [
    "S3UploadError",
    "build_s3_client",
    "parse_s3_uri",
    "resolve_bronze_s3_target",
    "resolve_s3_settings",
    "upload_partition_to_s3",
]
