"""Idempotent Bronze ingestion pipeline: streaming CSV/GZIP/TAR.GZ to partitioned Parquet."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
import shutil
import tarfile
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ie212.ingestion.downloader import (
    ChecksumMismatchError,
    ChecksumResult,
    DownloadConfig,
    DownloadTask,
    download_batch,
    validate_concurrency,
    verify_file_checksums,
)
from ie212.ingestion.manifest import (
    DEFAULT_BRONZE_MANIFEST_PATH,
    build_manifest_entry,
    load_manifest,
    make_manifest_key,
    save_manifest_atomic,
    should_skip_entry,
    utc_now_iso,
    validate_manifest_path,
)
from ie212.ingestion.s3 import (
    S3UploadError,
    resolve_bronze_s3_target,
    resolve_s3_settings,
    upload_partition_to_s3,
)
from ie212.ingestion.schemas import (
    PROVENANCE_COLUMNS,
    BronzeError,
    BronzeSchemaError,
    DatasetSchema,
    get_dataset_schema,
    normalize_csv_header,
)

SUPPORTED_CODECS: frozenset[str] = frozenset({"zstd", "snappy", "gzip", "none"})
_ARCHIVE_SUFFIXES: tuple[str, ...] = (".tar.gz", ".csv.gz", ".tgz", ".tar", ".gz", ".csv")
_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9_.\-]+")


@dataclass(frozen=True)
class IngestionFileResult:
    """Outcome of ingesting a single source file or archive into Bronze."""

    dataset: str
    source_file_stem: str
    source_file: str
    source_uri: str
    source_sha256: str
    source_md5: str
    source_size_bytes: int
    status: str  # "completed" | "skipped" | "dry_run"
    row_count: int
    rows_written: int
    parquet_files: tuple[str, ...]
    compression: str
    dropped_leading_index: bool
    timestamp_min: int | None
    timestamp_max: int | None
    null_timestamp_count: int
    ingested_at_utc: str
    s3_uris: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "source_file_stem": self.source_file_stem,
            "source_file": self.source_file,
            "source_uri": self.source_uri,
            "source_sha256": self.source_sha256,
            "source_md5": self.source_md5,
            "source_size_bytes": self.source_size_bytes,
            "status": self.status,
            "row_count": self.row_count,
            "rows_written": self.rows_written,
            "parquet_files": list(self.parquet_files),
            "compression": self.compression,
            "dropped_leading_index": self.dropped_leading_index,
            "timestamp_min": self.timestamp_min,
            "timestamp_max": self.timestamp_max,
            "null_timestamp_count": self.null_timestamp_count,
            "ingested_at_utc": self.ingested_at_utc,
            "s3_uris": list(self.s3_uris),
        }


def resolve_compression(requested: str = "zstd", fallback: str = "snappy") -> str:
    """Validate and resolve a Parquet compression codec with fallback support."""
    req = requested.strip().lower()
    fb = fallback.strip().lower()
    if req not in SUPPORTED_CODECS:
        raise BronzeError(f"Unsupported Parquet compression codec '{requested}'.")
    if req == "none" or pa.Codec.is_available(req):
        return req
    if fb in SUPPORTED_CODECS and (fb == "none" or pa.Codec.is_available(fb)):
        return fb
    raise BronzeError(f"Neither '{requested}' nor fallback '{fallback}' Parquet codec is available.")


def derive_source_file_stem(path: str | Path) -> str:
    """Strip .tar.gz / .csv.gz / .tgz / .csv suffixes to produce a Hive-safe stem."""
    name = Path(path).name
    lower_name = name.lower()
    for suffix in _ARCHIVE_SUFFIXES:
        if lower_name.endswith(suffix):
            name = name[: len(name) - len(suffix)]
            break
    sanitized = _SAFE_STEM_RE.sub("_", name).strip("._")
    if not sanitized:
        raise BronzeError(f"Cannot derive non-empty source_file_stem from '{path}'.")
    return sanitized


def _is_tar_csv_member(member: tarfile.TarInfo) -> bool:
    if not member.isfile():
        return False
    member_path = Path(member.name)
    if "__MACOSX" in member_path.parts:
        return False
    base = member_path.name
    if not base or base.startswith("._"):
        return False
    lower = base.lower()
    return lower.endswith(".csv") or lower.endswith(".csv.gz")


_INT64_MIN_FLOAT = -9_223_372_036_854_775_808.0
_INT64_MAX_EXCLUSIVE_FLOAT = 9_223_372_036_854_775_808.0
_INT64_MAX = 9_223_372_036_854_775_807


def _coerce_nullable_int64(series: pd.Series) -> pd.Series:
    """Safely coerce raw CSV numeric strings into pandas nullable Int64 without overflow."""
    numeric = pd.to_numeric(series, errors="coerce")
    if pd.api.types.is_unsigned_integer_dtype(numeric.dtype):
        return pd.Series(
            [int(val) if int(val) <= _INT64_MAX else pd.NA for val in numeric],
            index=series.index,
            dtype="Int64",
        )
    if pd.api.types.is_integer_dtype(numeric.dtype):
        return numeric.astype("Int64")

    valid = (
        np.isfinite(numeric)
        & (np.floor(numeric) == numeric)
        & (numeric >= _INT64_MIN_FLOAT)
        & (numeric < _INT64_MAX_EXCLUSIVE_FLOAT)
    )
    return numeric.where(valid).astype("Int64")


def _build_chunk_arrow_table(
    chunk: pd.DataFrame,
    *,
    schema: DatasetSchema,
    arrow_schema: pa.Schema,
    source_file: str,
    source_uri: str,
    source_sha256: str,
    source_md5: str,
    ingested_at_utc: str,
    row_offset_start: int = 0,
) -> tuple[pa.Table, int | None, int | None, int]:
    arrays: list[pa.Array] = []
    ts_min: int | None = None
    ts_max: int | None = None
    null_ts_count = 0

    for col in schema.columns:
        arrow_type = schema.column_types[col]
        if pa.types.is_int64(arrow_type):
            nullable_ints = _coerce_nullable_int64(chunk[col])
            if col == schema.timestamp_column:
                null_ts_count = int(nullable_ints.isna().sum())
                valid_ts = nullable_ints.dropna()
                if not valid_ts.empty:
                    ts_min = int(valid_ts.min())
                    ts_max = int(valid_ts.max())
            arrays.append(pa.array(nullable_ints, type=pa.int64(), from_pandas=True))
        elif pa.types.is_float64(arrow_type):
            floats = pd.to_numeric(chunk[col], errors="coerce").astype("float64")
            arrays.append(pa.array(floats, type=pa.float64(), from_pandas=True))
        else:
            arrays.append(pa.array(chunk[col].astype(str), type=pa.string()))

    n_rows = len(chunk)
    provenance_arrays: dict[str, pa.Array] = {
        "source_file": pa.repeat(pa.scalar(source_file, type=pa.string()), n_rows),
        "source_row_offset": pa.array(
            range(row_offset_start, row_offset_start + n_rows),
            type=pa.int64(),
        ),
        "source_uri": pa.repeat(pa.scalar(source_uri, type=pa.string()), n_rows),
        "source_sha256": pa.repeat(pa.scalar(source_sha256, type=pa.string()), n_rows),
        "source_md5": pa.repeat(pa.scalar(source_md5, type=pa.string()), n_rows),
        "ingested_at_utc": pa.repeat(pa.scalar(ingested_at_utc, type=pa.string()), n_rows),
    }
    for prov_col, _ in PROVENANCE_COLUMNS:
        arrays.append(provenance_arrays[prov_col])

    return pa.Table.from_arrays(arrays, schema=arrow_schema), ts_min, ts_max, null_ts_count


def _stream_csv_text_to_parquet(
    text_stream: io.TextIOBase,
    *,
    schema: DatasetSchema,
    parquet_path: Path,
    source_file: str,
    source_uri: str,
    source_sha256: str,
    source_md5: str,
    ingested_at_utc: str,
    compression: str,
    chunk_rows: int,
) -> tuple[int, bool, int | None, int | None, int]:
    header_line = text_stream.readline()
    if not header_line:
        raise BronzeSchemaError(f"CSV stream '{source_file}' is empty (missing header row).")

    raw_header = next(csv.reader([header_line]))
    norm = normalize_csv_header(raw_header, schema)

    metadata = {
        b"ie212.dataset": schema.name.encode("utf-8"),
        b"ie212.source": schema.source.encode("utf-8"),
        b"ie212.source_uri": source_uri.encode("utf-8"),
        b"ie212.source_file": source_file.encode("utf-8"),
        b"ie212.compression": compression.encode("utf-8"),
        b"ie212.dropped_leading_index": str(norm.dropped_leading_index).lower().encode("utf-8"),
    }
    arrow_schema = schema.arrow_schema.with_metadata(metadata)
    pa_compression = None if compression == "none" else compression

    total_rows = 0
    ts_min: int | None = None
    ts_max: int | None = None
    null_ts_count = 0

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(parquet_path, schema=arrow_schema, compression=pa_compression) as writer:
        try:
            reader = pd.read_csv(
                text_stream,
                header=None,
                names=list(norm.columns),
                usecols=list(norm.selected_indices),
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                chunksize=chunk_rows,
            )
            for chunk in reader:
                if chunk.empty:
                    continue
                table, chunk_ts_min, chunk_ts_max, chunk_null_ts = _build_chunk_arrow_table(
                    chunk,
                    schema=schema,
                    arrow_schema=arrow_schema,
                    source_file=source_file,
                    source_uri=source_uri,
                    source_sha256=source_sha256,
                    source_md5=source_md5,
                    ingested_at_utc=ingested_at_utc,
                    row_offset_start=total_rows,
                )
                writer.write_table(table)
                total_rows += len(chunk)
                null_ts_count += chunk_null_ts
                if chunk_ts_min is not None:
                    ts_min = chunk_ts_min if ts_min is None else min(ts_min, chunk_ts_min)
                if chunk_ts_max is not None:
                    ts_max = chunk_ts_max if ts_max is None else max(ts_max, chunk_ts_max)
        except pd.errors.EmptyDataError:
            pass

        if total_rows == 0:
            writer.write_table(arrow_schema.empty_table())

    return total_rows, norm.dropped_leading_index, ts_min, ts_max, null_ts_count


def _detect_source_format(source_path: Path) -> str:
    """Detect source format ('tar_gz', 'gz', or 'csv') via extension or gzip/tar magic bytes."""
    lower_name = source_path.name.lower()
    if lower_name.endswith(".tar.gz") or lower_name.endswith(".tgz"):
        return "tar_gz"
    if lower_name.endswith(".gz"):
        return "gz"
    if lower_name.endswith(".csv"):
        return "csv"

    with source_path.open("rb") as fh:
        magic = fh.read(2)
    if magic == b"\x1f\x8b":
        try:
            with tarfile.open(source_path, mode="r:gz"):
                return "tar_gz"
        except (tarfile.TarError, EOFError, OSError):
            return "gz"
    return "csv"


def _stream_source_to_staging(
    source_path: Path,
    *,
    staging_dir: Path,
    schema: DatasetSchema,
    source_filename: str | None = None,
    source_uri: str,
    source_sha256: str,
    source_md5: str,
    ingested_at_utc: str,
    compression: str,
    chunk_rows: int,
) -> tuple[list[Path], int, bool, int | None, int | None, int]:
    source_format = _detect_source_format(source_path)
    effective_filename = source_filename or source_path.name
    written_files: list[Path] = []
    total_rows = 0
    any_dropped_index = False
    ts_min: int | None = None
    ts_max: int | None = None
    null_ts_count = 0

    if source_format == "tar_gz":
        member_idx = 0
        with tarfile.open(source_path, mode="r:gz") as tar:
            for member in tar:
                if not _is_tar_csv_member(member):
                    continue
                raw_member_stream = tar.extractfile(member)
                if raw_member_stream is None:
                    continue
                part_path = staging_dir / f"part-{member_idx:05d}.parquet"
                member_idx += 1
                source_label = f"{effective_filename}::{member.name}"
                try:
                    if member.name.lower().endswith(".gz"):
                        with gzip.GzipFile(fileobj=raw_member_stream, mode="rb") as gz_stream:
                            with io.TextIOWrapper(
                                gz_stream, encoding="utf-8", errors="replace", newline=""
                            ) as text_stream:
                                rows, dropped, m_min, m_max, m_nulls = _stream_csv_text_to_parquet(
                                    text_stream,
                                    schema=schema,
                                    parquet_path=part_path,
                                    source_file=source_label,
                                    source_uri=source_uri,
                                    source_sha256=source_sha256,
                                    source_md5=source_md5,
                                    ingested_at_utc=ingested_at_utc,
                                    compression=compression,
                                    chunk_rows=chunk_rows,
                                )
                    else:
                        with io.TextIOWrapper(
                            raw_member_stream, encoding="utf-8", errors="replace", newline=""
                        ) as text_stream:
                            rows, dropped, m_min, m_max, m_nulls = _stream_csv_text_to_parquet(
                                text_stream,
                                schema=schema,
                                parquet_path=part_path,
                                source_file=source_label,
                                source_uri=source_uri,
                                source_sha256=source_sha256,
                                source_md5=source_md5,
                                ingested_at_utc=ingested_at_utc,
                                compression=compression,
                                chunk_rows=chunk_rows,
                            )
                finally:
                    raw_member_stream.close()

                written_files.append(part_path)
                total_rows += rows
                any_dropped_index = any_dropped_index or dropped
                null_ts_count += m_nulls
                if m_min is not None:
                    ts_min = m_min if ts_min is None else min(ts_min, m_min)
                if m_max is not None:
                    ts_max = m_max if ts_max is None else max(ts_max, m_max)

        if not written_files:
            raise BronzeError(f"Archive '{source_path}' contained no valid CSV members.")
    elif source_format == "gz":
        part_path = staging_dir / "part-00000.parquet"
        with gzip.open(source_path, mode="rt", encoding="utf-8", errors="replace", newline="") as fh:
            rows, dropped, m_min, m_max, m_nulls = _stream_csv_text_to_parquet(
                fh,
                schema=schema,
                parquet_path=part_path,
                source_file=effective_filename,
                source_uri=source_uri,
                source_sha256=source_sha256,
                source_md5=source_md5,
                ingested_at_utc=ingested_at_utc,
                compression=compression,
                chunk_rows=chunk_rows,
            )
        written_files.append(part_path)
        total_rows = rows
        any_dropped_index = dropped
        ts_min, ts_max, null_ts_count = m_min, m_max, m_nulls
    else:
        part_path = staging_dir / "part-00000.parquet"
        with source_path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            rows, dropped, m_min, m_max, m_nulls = _stream_csv_text_to_parquet(
                fh,
                schema=schema,
                parquet_path=part_path,
                source_file=effective_filename,
                source_uri=source_uri,
                source_sha256=source_sha256,
                source_md5=source_md5,
                ingested_at_utc=ingested_at_utc,
                compression=compression,
                chunk_rows=chunk_rows,
            )
        written_files.append(part_path)
        total_rows = rows
        any_dropped_index = dropped
        ts_min, ts_max, null_ts_count = m_min, m_max, m_nulls

    return written_files, total_rows, any_dropped_index, ts_min, ts_max, null_ts_count


def _atomic_swap_directory(staging_dir: Path, target_dir: Path) -> None:
    """Replace target_dir with staging_dir safely on both Windows and POSIX."""
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir: Path | None = None

    try:
        if target_dir.exists():
            backup_dir = target_dir.parent / f".backup_{target_dir.name}_{uuid.uuid4().hex[:8]}"
            os.replace(target_dir, backup_dir)
        os.replace(staging_dir, target_dir)
    except Exception:
        if not target_dir.exists() and backup_dir is not None and backup_dir.exists():
            os.replace(backup_dir, target_dir)
        raise
    else:
        if backup_dir is not None and backup_dir.exists():
            shutil.rmtree(backup_dir)


def _canonical_local_origin(path: Path) -> str:
    return path.resolve().as_uri()


def _canonical_url_origin(url: str) -> str:
    return url.split("#", 1)[0]


def _find_manifest_entry_by_origin(
    manifest: Mapping[str, Any],
    dataset: str,
    canonical_origin: str,
) -> tuple[str, dict[str, Any]] | None:
    entries = manifest.get("entries")
    if not isinstance(entries, Mapping):
        return None
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        if entry.get("dataset") != dataset:
            continue
        recorded_origin = entry.get("source_origin")
        if recorded_origin == canonical_origin:
            return str(key), entry
        if recorded_origin is None and entry.get("source_uri") == canonical_origin:
            return str(key), entry
    return None


def _derive_output_stem(
    base_stem: str,
    *,
    dataset: str,
    canonical_origin: str,
    manifest: Mapping[str, Any],
    batch_colliding_stems: set[str] | frozenset[str] = frozenset(),
) -> str:
    existing_match = _find_manifest_entry_by_origin(manifest, dataset, canonical_origin)
    if existing_match is not None:
        return str(existing_match[1]["source_file_stem"])

    entries = manifest.get("entries", {})
    base_key = make_manifest_key(dataset, base_stem)
    base_taken = isinstance(entries, Mapping) and base_key in entries
    if base_stem not in batch_colliding_stems and not base_taken:
        return base_stem

    digest = hashlib.sha256(canonical_origin.encode("utf-8")).hexdigest()
    for prefix_len in (8, 12, 16, 24, 32, len(digest)):
        candidate = f"{base_stem}_{digest[:prefix_len]}"
        candidate_key = make_manifest_key(dataset, candidate)
        if not isinstance(entries, Mapping) or candidate_key not in entries:
            return candidate
        existing_entry = entries[candidate_key]
        if isinstance(existing_entry, Mapping) and (
            existing_entry.get("source_origin") == canonical_origin
            or (
                existing_entry.get("source_origin") is None
                and existing_entry.get("source_uri") == canonical_origin
            )
        ):
            return candidate
    return f"{base_stem}_{digest[:16]}"


def _validate_upload_s3_preconditions(
    dataset: str,
    schema: DatasetSchema,
    lakehouse_cfg: Mapping[str, Any] | None,
    *,
    s3_endpoint: str | None,
    s3_client: Any,
) -> None:
    if lakehouse_cfg is None:
        raise BronzeError("lakehouse_cfg is required when upload_s3=True.")
    resolve_bronze_s3_target(dataset, lakehouse_cfg, schema=schema)
    if s3_client is None:
        settings = resolve_s3_settings(lakehouse_cfg, endpoint_override=s3_endpoint)
        if not settings["access_key"] or not settings["secret_key"]:
            raise S3UploadError(
                "Missing MinIO/S3 credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, "
                "MINIO_ACCESS_KEY/MINIO_SECRET_KEY, or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
                "before uploading."
            )


def _validate_precomputed_checksums(
    checksums: ChecksumResult,
    source_path: Path,
    *,
    expected_sha256: str | None,
    expected_md5: str | None,
) -> ChecksumResult:
    if expected_sha256 is not None and checksums.sha256.lower() != expected_sha256.strip().lower():
        raise ChecksumMismatchError(
            f"SHA-256 mismatch for {source_path}: expected {expected_sha256.strip().lower()}, "
            f"got {checksums.sha256}."
        )
    if expected_md5 is not None and checksums.md5.lower() != expected_md5.strip().lower():
        raise ChecksumMismatchError(
            f"MD5 mismatch for {source_path}: expected {expected_md5.strip().lower()}, "
            f"got {checksums.md5}."
        )
    return checksums


def ingest_local_file(
    source_path: str | Path,
    *,
    dataset: str,
    output_dir: str | Path = "data/raw/bronze",
    manifest_path: str | Path = DEFAULT_BRONZE_MANIFEST_PATH,
    data_cfg: Mapping[str, Any] | None = None,
    lakehouse_cfg: Mapping[str, Any] | None = None,
    source_uri: str | None = None,
    source_filename: str | None = None,
    canonical_origin: str | None = None,
    precomputed_checksums: ChecksumResult | None = None,
    force_disambiguate_stems: frozenset[str] = frozenset(),
    compression: str = "zstd",
    fallback_compression: str = "snappy",
    chunk_rows: int = 100_000,
    expected_sha256: str | None = None,
    expected_md5: str | None = None,
    force: bool = False,
    upload_s3: bool = False,
    s3_endpoint: str | None = None,
    s3_client: Any = None,
    dry_run: bool = False,
    is_temp_download: bool = False,
    cleanup_temp_archives: bool = True,
) -> IngestionFileResult:
    """Ingest a single local CSV/CSV.GZ/TAR.GZ file into partitioned Bronze Parquet."""
    if chunk_rows < 1:
        raise ValueError(f"chunk_rows must be >= 1, got {chunk_rows}.")

    src = Path(source_path)
    if not src.is_file():
        raise FileNotFoundError(f"Bronze input file not found: {src}")

    validate_manifest_path(manifest_path)
    schema = get_dataset_schema(dataset, data_cfg=data_cfg, lakehouse_cfg=lakehouse_cfg)
    if upload_s3 and not dry_run:
        _validate_upload_s3_preconditions(
            dataset,
            schema,
            lakehouse_cfg,
            s3_endpoint=s3_endpoint,
            s3_client=s3_client,
        )

    resolved_codec = resolve_compression(compression, fallback=fallback_compression)
    out_root = Path(output_dir)
    effective_filename = source_filename or src.name
    base_stem = derive_source_file_stem(effective_filename)
    origin = canonical_origin or (
        _canonical_url_origin(source_uri)
        if (is_temp_download and source_uri)
        else _canonical_local_origin(src)
    )
    effective_uri = source_uri or schema.doi_or_url or _canonical_local_origin(src)

    if precomputed_checksums is not None:
        checksums = _validate_precomputed_checksums(
            precomputed_checksums,
            src,
            expected_sha256=expected_sha256,
            expected_md5=expected_md5,
        )
    else:
        checksums = verify_file_checksums(
            src,
            expected_sha256=expected_sha256,
            expected_md5=expected_md5,
        )

    manifest = load_manifest(manifest_path)
    stem = _derive_output_stem(
        base_stem,
        dataset=dataset,
        canonical_origin=origin,
        manifest=manifest,
        batch_colliding_stems=force_disambiguate_stems,
    )
    partition_rel = Path(f"dataset={dataset}") / f"source_file_stem={stem}"
    target_dir = out_root / partition_rel
    manifest_key = make_manifest_key(dataset, stem)

    if should_skip_entry(
        manifest,
        dataset=dataset,
        source_file_stem=stem,
        sha256=checksums.sha256,
        md5=checksums.md5,
        output_dir=out_root,
        force=force,
        source_origin=origin,
    ):
        existing = manifest["entries"][manifest_key]
        existing_files = [
            (out_root / pf if not Path(pf).is_absolute() else Path(pf))
            for pf in existing["parquet_files"]
        ]
        s3_uris = tuple(existing.get("s3_uris", ()))
        if upload_s3 and not s3_uris and not dry_run:
            assert lakehouse_cfg is not None
            uploaded = upload_partition_to_s3(
                existing_files,
                dataset=dataset,
                source_file_stem=stem,
                lakehouse_cfg=lakehouse_cfg,
                schema=schema,
                endpoint_override=s3_endpoint,
                s3_client=s3_client,
            )
            s3_uris = tuple(uploaded)
            existing["s3_uris"] = uploaded
            save_manifest_atomic(manifest_path, manifest)

        if is_temp_download and cleanup_temp_archives and src.exists():
            src.unlink(missing_ok=True)

        return IngestionFileResult(
            dataset=dataset,
            source_file_stem=stem,
            source_file=str(existing.get("source_file", effective_filename)),
            source_uri=str(existing.get("source_uri", effective_uri)),
            source_sha256=checksums.sha256,
            source_md5=checksums.md5,
            source_size_bytes=checksums.size_bytes,
            status="skipped",
            row_count=int(existing.get("row_count", 0)),
            rows_written=0,
            parquet_files=tuple(p.as_posix() for p in existing_files),
            compression=str(existing.get("compression", resolved_codec)),
            dropped_leading_index=bool(existing.get("dropped_leading_index", False)),
            timestamp_min=existing.get("timestamp_min"),
            timestamp_max=existing.get("timestamp_max"),
            null_timestamp_count=int(existing.get("null_timestamp_count", 0)),
            ingested_at_utc=str(existing.get("ingested_at_utc", "")),
            s3_uris=s3_uris,
        )

    if dry_run:
        return IngestionFileResult(
            dataset=dataset,
            source_file_stem=stem,
            source_file=effective_filename,
            source_uri=effective_uri,
            source_sha256=checksums.sha256,
            source_md5=checksums.md5,
            source_size_bytes=checksums.size_bytes,
            status="dry_run",
            row_count=0,
            rows_written=0,
            parquet_files=((target_dir / "part-00000.parquet").as_posix(),),
            compression=resolved_codec,
            dropped_leading_index=schema.has_leading_index,
            timestamp_min=None,
            timestamp_max=None,
            null_timestamp_count=0,
            ingested_at_utc=utc_now_iso(),
        )

    out_root.mkdir(parents=True, exist_ok=True)
    staging_dir = out_root / f".staging_{dataset}_{stem}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    ingested_at = utc_now_iso()

    try:
        (
            staged_files,
            total_rows,
            dropped_leading_index,
            ts_min,
            ts_max,
            null_ts_count,
        ) = _stream_source_to_staging(
            src,
            staging_dir=staging_dir,
            schema=schema,
            source_filename=effective_filename,
            source_uri=effective_uri,
            source_sha256=checksums.sha256,
            source_md5=checksums.md5,
            ingested_at_utc=ingested_at,
            compression=resolved_codec,
            chunk_rows=chunk_rows,
        )
        _atomic_swap_directory(staging_dir, target_dir)
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

    final_files = [target_dir / staged.name for staged in staged_files]
    rel_parquet_files = [(partition_rel / staged.name).as_posix() for staged in staged_files]

    s3_uris: list[str] = []
    if upload_s3:
        assert lakehouse_cfg is not None
        s3_uris = upload_partition_to_s3(
            final_files,
            dataset=dataset,
            source_file_stem=stem,
            lakehouse_cfg=lakehouse_cfg,
            schema=schema,
            endpoint_override=s3_endpoint,
            s3_client=s3_client,
        )

    manifest = load_manifest(manifest_path)
    manifest["entries"][manifest_key] = build_manifest_entry(
        dataset=dataset,
        source_file_stem=stem,
        source_file=effective_filename,
        source_uri=effective_uri,
        source_sha256=checksums.sha256,
        source_md5=checksums.md5,
        source_size_bytes=checksums.size_bytes,
        row_count=total_rows,
        parquet_files=rel_parquet_files,
        compression=resolved_codec,
        dropped_leading_index=dropped_leading_index,
        timestamp_min=ts_min,
        timestamp_max=ts_max,
        null_timestamp_count=null_ts_count,
        ingested_at_utc=ingested_at,
        status="completed",
        s3_uris=s3_uris,
        source_origin=origin,
    )
    save_manifest_atomic(manifest_path, manifest)

    if is_temp_download and cleanup_temp_archives and src.exists():
        src.unlink(missing_ok=True)

    return IngestionFileResult(
        dataset=dataset,
        source_file_stem=stem,
        source_file=effective_filename,
        source_uri=effective_uri,
        source_sha256=checksums.sha256,
        source_md5=checksums.md5,
        source_size_bytes=checksums.size_bytes,
        status="completed",
        row_count=total_rows,
        rows_written=total_rows,
        parquet_files=tuple(p.as_posix() for p in final_files),
        compression=resolved_codec,
        dropped_leading_index=dropped_leading_index,
        timestamp_min=ts_min,
        timestamp_max=ts_max,
        null_timestamp_count=null_ts_count,
        ingested_at_utc=ingested_at,
        s3_uris=tuple(s3_uris),
    )


def _derive_filename_from_url(url: str, fallback_idx: int) -> str:
    parsed = urlparse(url)
    unquoted = unquote(parsed.path).rstrip("/")
    name = Path(unquoted).name
    if not name or name in {".", ".."}:
        return f"download_{fallback_idx}.tar.gz"
    return name


def ingest_bronze(
    *,
    dataset: str,
    input_paths: Sequence[str | Path] = (),
    urls: Sequence[str] = (),
    output_dir: str | Path | None = None,
    manifest_path: str | Path | None = None,
    download_dir: str | Path | None = None,
    data_cfg: Mapping[str, Any] | None = None,
    lakehouse_cfg: Mapping[str, Any] | None = None,
    compression: str | None = None,
    fallback_compression: str | None = None,
    chunk_rows: int = 100_000,
    concurrency: int | None = None,
    expected_sha256: str | None = None,
    expected_md5: str | None = None,
    force: bool = False,
    cleanup_temp_archives: bool | None = None,
    upload_s3: bool = False,
    s3_endpoint: str | None = None,
    s3_client: Any = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run idempotent Bronze ingestion across local input files and/or remote URLs."""
    if not input_paths and not urls:
        raise BronzeError("Provide at least one local --input file or remote --url.")
    if (expected_sha256 or expected_md5) and (len(input_paths) + len(urls)) > 1:
        raise BronzeError(
            "expected_sha256 / expected_md5 can only be applied to a single input file or URL."
        )

    bronze_cfg = data_cfg.get("bronze", {}) if isinstance(data_cfg, Mapping) else {}
    cfg_output_dir = bronze_cfg.get("output_dir") if isinstance(bronze_cfg, Mapping) else None
    resolved_output_dir = Path(
        output_dir
        or (
            cfg_output_dir
            if isinstance(cfg_output_dir, str) and cfg_output_dir.strip()
            else "data/raw/bronze"
        )
    )
    cfg_manifest_dir = bronze_cfg.get("manifest_dir") if isinstance(bronze_cfg, Mapping) else None
    default_manifest_path = (
        Path(cfg_manifest_dir.strip()) / "bronze_manifest.json"
        if isinstance(cfg_manifest_dir, str) and cfg_manifest_dir.strip()
        else DEFAULT_BRONZE_MANIFEST_PATH
    )
    resolved_manifest_path = Path(manifest_path) if manifest_path is not None else default_manifest_path
    validate_manifest_path(resolved_manifest_path)

    dl_cfg = DownloadConfig.from_data_config(data_cfg)
    effective_concurrency = dl_cfg.max_concurrency if concurrency is None else concurrency
    validate_concurrency(effective_concurrency, max_allowed=dl_cfg.max_concurrency)

    effective_cleanup = (
        dl_cfg.cleanup_temp_archives
        if cleanup_temp_archives is None
        else bool(cleanup_temp_archives)
    )
    req_codec = compression or str(
        bronze_cfg.get("compression", "zstd") if isinstance(bronze_cfg, Mapping) else "zstd"
    )
    fb_codec = fallback_compression or str(
        bronze_cfg.get("fallback_compression", "snappy")
        if isinstance(bronze_cfg, Mapping)
        else "snappy"
    )

    schema = get_dataset_schema(dataset, data_cfg=data_cfg, lakehouse_cfg=lakehouse_cfg)
    if upload_s3 and not dry_run:
        _validate_upload_s3_preconditions(
            dataset,
            schema,
            lakehouse_cfg,
            s3_endpoint=s3_endpoint,
            s3_client=s3_client,
        )

    # Detect within-batch source_file_stem collisions across both --input and --url inputs.
    batch_origins_by_stem: dict[str, set[str]] = {}
    for local_input in input_paths:
        local_path = Path(local_input)
        base_stem = derive_source_file_stem(local_path)
        batch_origins_by_stem.setdefault(base_stem, set()).add(_canonical_local_origin(local_path))
    for idx, url in enumerate(urls):
        fname = _derive_filename_from_url(url, idx)
        base_stem = derive_source_file_stem(fname)
        batch_origins_by_stem.setdefault(base_stem, set()).add(_canonical_url_origin(url))
    colliding_stems = frozenset(
        stem_name for stem_name, origins in batch_origins_by_stem.items() if len(origins) > 1
    )

    results: list[IngestionFileResult] = []

    for local_input in input_paths:
        res = ingest_local_file(
            local_input,
            dataset=dataset,
            output_dir=resolved_output_dir,
            manifest_path=resolved_manifest_path,
            data_cfg=data_cfg,
            lakehouse_cfg=lakehouse_cfg,
            source_uri=schema.doi_or_url,
            force_disambiguate_stems=colliding_stems,
            compression=req_codec,
            fallback_compression=fb_codec,
            chunk_rows=chunk_rows,
            expected_sha256=expected_sha256,
            expected_md5=expected_md5,
            force=force,
            upload_s3=upload_s3,
            s3_endpoint=s3_endpoint,
            s3_client=s3_client,
            dry_run=dry_run,
            is_temp_download=False,
            cleanup_temp_archives=False,
        )
        results.append(res)

    if urls:
        resolved_codec = resolve_compression(req_codec, fallback=fb_codec)
        if dry_run:
            manifest = load_manifest(resolved_manifest_path)
            for idx, url in enumerate(urls):
                fname = _derive_filename_from_url(url, idx)
                base_stem = derive_source_file_stem(fname)
                stem = _derive_output_stem(
                    base_stem,
                    dataset=dataset,
                    canonical_origin=_canonical_url_origin(url),
                    manifest=manifest,
                    batch_colliding_stems=colliding_stems,
                )
                results.append(
                    IngestionFileResult(
                        dataset=dataset,
                        source_file_stem=stem,
                        source_file=fname,
                        source_uri=url,
                        source_sha256="",
                        source_md5="",
                        source_size_bytes=0,
                        status="dry_run",
                        row_count=0,
                        rows_written=0,
                        parquet_files=(
                            (
                                resolved_output_dir
                                / f"dataset={dataset}"
                                / f"source_file_stem={stem}"
                                / "part-00000.parquet"
                            ).as_posix(),
                        ),
                        compression=resolved_codec,
                        dropped_leading_index=schema.has_leading_index,
                        timestamp_min=None,
                        timestamp_max=None,
                        null_timestamp_count=0,
                        ingested_at_utc=utc_now_iso(),
                    )
                )
        else:
            manifest = load_manifest(resolved_manifest_path)
            url_results: dict[int, IngestionFileResult] = {}
            pending_urls: list[tuple[int, str]] = []

            for idx, url in enumerate(urls):
                origin = _canonical_url_origin(url)
                existing_match = _find_manifest_entry_by_origin(manifest, dataset, origin)
                can_skip = False
                if not force and existing_match is not None:
                    _entry_key, existing = existing_match
                    rec_sha256 = str(existing.get("source_sha256", ""))
                    rec_md5 = str(existing.get("source_md5", ""))
                    rec_stem = str(existing.get("source_file_stem", ""))
                    sha_ok = (
                        expected_sha256 is None
                        or rec_sha256.lower() == expected_sha256.strip().lower()
                    )
                    md5_ok = (
                        expected_md5 is None
                        or rec_md5.lower() == expected_md5.strip().lower()
                    )
                    if (
                        sha_ok
                        and md5_ok
                        and rec_sha256
                        and rec_md5
                        and rec_stem
                        and should_skip_entry(
                            manifest,
                            dataset=dataset,
                            source_file_stem=rec_stem,
                            sha256=rec_sha256,
                            md5=rec_md5,
                            output_dir=resolved_output_dir,
                            force=False,
                            source_origin=origin,
                        )
                    ):
                        can_skip = True
                        existing_files = [
                            (
                                resolved_output_dir / pf
                                if not Path(pf).is_absolute()
                                else Path(pf)
                            )
                            for pf in existing["parquet_files"]
                        ]
                        s3_uris = tuple(existing.get("s3_uris", ()))
                        if upload_s3 and not s3_uris:
                            assert lakehouse_cfg is not None
                            uploaded = upload_partition_to_s3(
                                existing_files,
                                dataset=dataset,
                                source_file_stem=rec_stem,
                                lakehouse_cfg=lakehouse_cfg,
                                schema=schema,
                                endpoint_override=s3_endpoint,
                                s3_client=s3_client,
                            )
                            s3_uris = tuple(uploaded)
                            existing["s3_uris"] = uploaded
                            save_manifest_atomic(resolved_manifest_path, manifest)

                        url_results[idx] = IngestionFileResult(
                            dataset=dataset,
                            source_file_stem=rec_stem,
                            source_file=str(
                                existing.get("source_file", _derive_filename_from_url(url, idx))
                            ),
                            source_uri=str(existing.get("source_uri", url)),
                            source_sha256=rec_sha256,
                            source_md5=rec_md5,
                            source_size_bytes=int(existing.get("source_size_bytes", 0)),
                            status="skipped",
                            row_count=int(existing.get("row_count", 0)),
                            rows_written=0,
                            parquet_files=tuple(p.as_posix() for p in existing_files),
                            compression=str(existing.get("compression", resolved_codec)),
                            dropped_leading_index=bool(existing.get("dropped_leading_index", False)),
                            timestamp_min=existing.get("timestamp_min"),
                            timestamp_max=existing.get("timestamp_max"),
                            null_timestamp_count=int(existing.get("null_timestamp_count", 0)),
                            ingested_at_utc=str(existing.get("ingested_at_utc", "")),
                            s3_uris=s3_uris,
                        )

                if not can_skip:
                    pending_urls.append((idx, url))

            if pending_urls:
                created_ephemeral_dir = download_dir is None
                dl_root = (
                    Path(tempfile.mkdtemp(prefix="ie212_bronze_dl_"))
                    if created_ephemeral_dir
                    else Path(download_dir)
                )
                dl_root.mkdir(parents=True, exist_ok=True)

                for start in range(0, len(pending_urls), effective_concurrency):
                    chunk = pending_urls[start : start + effective_concurrency]
                    chunk_tasks = [
                        DownloadTask(
                            url=url,
                            dest_path=dl_root / f"{orig_idx:04d}" / _derive_filename_from_url(url, orig_idx),
                            expected_sha256=expected_sha256,
                            expected_md5=expected_md5,
                            prefer_content_disposition=True,
                        )
                        for orig_idx, url in chunk
                    ]
                    downloaded = download_batch(
                        chunk_tasks,
                        config=dl_cfg,
                        concurrency=effective_concurrency,
                    )
                    for (orig_idx, _url), dl_res in zip(chunk, downloaded):
                        res = ingest_local_file(
                            dl_res.dest_path,
                            dataset=dataset,
                            output_dir=resolved_output_dir,
                            manifest_path=resolved_manifest_path,
                            data_cfg=data_cfg,
                            lakehouse_cfg=lakehouse_cfg,
                            source_uri=dl_res.url,
                            source_filename=dl_res.content_disposition_filename,
                            canonical_origin=_canonical_url_origin(dl_res.url),
                            precomputed_checksums=ChecksumResult(
                                sha256=dl_res.sha256,
                                md5=dl_res.md5,
                                size_bytes=dl_res.size_bytes,
                            ),
                            force_disambiguate_stems=colliding_stems,
                            compression=req_codec,
                            fallback_compression=fb_codec,
                            chunk_rows=chunk_rows,
                            expected_sha256=expected_sha256,
                            expected_md5=expected_md5,
                            force=force,
                            upload_s3=upload_s3,
                            s3_endpoint=s3_endpoint,
                            s3_client=s3_client,
                            dry_run=False,
                            is_temp_download=True,
                            cleanup_temp_archives=effective_cleanup,
                        )
                        if effective_cleanup:
                            try:
                                dl_res.dest_path.parent.rmdir()
                            except OSError:
                                pass
                        url_results[orig_idx] = res

                if created_ephemeral_dir and effective_cleanup and dl_root.exists():
                    shutil.rmtree(dl_root, ignore_errors=True)

            for idx in range(len(urls)):
                results.append(url_results[idx])

    return {
        "stage": "bronze",
        "dataset": dataset,
        "status": "dry_run" if dry_run else "ok",
        "output_dir": resolved_output_dir.as_posix(),
        "manifest_path": resolved_manifest_path.as_posix(),
        "concurrency": effective_concurrency,
        "cleanup_temp_archives": effective_cleanup,
        "files_total": len(results),
        "files_completed": sum(1 for r in results if r.status == "completed"),
        "files_skipped": sum(1 for r in results if r.status == "skipped"),
        "total_rows": sum(r.row_count for r in results),
        "rows_written": sum(r.rows_written for r in results),
        "results": [r.to_dict() for r in results],
    }


__all__ = [
    "IngestionFileResult",
    "SUPPORTED_CODECS",
    "derive_source_file_stem",
    "ingest_bronze",
    "ingest_local_file",
    "resolve_compression",
]
