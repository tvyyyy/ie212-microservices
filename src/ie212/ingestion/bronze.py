"""Idempotent Bronze ingestion pipeline: streaming CSV/GZIP/TAR.GZ to partitioned Parquet."""

from __future__ import annotations

import csv
import gzip
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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ie212.ingestion.downloader import (
    DownloadConfig,
    DownloadTask,
    compute_file_checksums,
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
from ie212.ingestion.s3 import upload_partition_to_s3
from ie212.ingestion.schemas import (
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
) -> tuple[pa.Table, int | None, int | None, int]:
    arrays: list[pa.Array] = []
    ts_min: int | None = None
    ts_max: int | None = None
    null_ts_count = 0

    for col in schema.columns:
        arrow_type = schema.column_types[col]
        if pa.types.is_int64(arrow_type):
            numeric = pd.to_numeric(chunk[col], errors="coerce")
            nullable_ints = pd.Series(numeric, dtype="Int64")
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
    provenance_values = (
        source_file,
        source_uri,
        source_sha256,
        source_md5,
        ingested_at_utc,
    )
    for prov_val in provenance_values:
        arrays.append(pa.repeat(pa.scalar(prov_val, type=pa.string()), n_rows))

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
    source_uri: str,
    source_sha256: str,
    source_md5: str,
    ingested_at_utc: str,
    compression: str,
    chunk_rows: int,
) -> tuple[list[Path], int, bool, int | None, int | None, int]:
    source_format = _detect_source_format(source_path)
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
                source_label = f"{source_path.name}::{member.name}"
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
                source_file=source_path.name,
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
                source_file=source_path.name,
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


def ingest_local_file(
    source_path: str | Path,
    *,
    dataset: str,
    output_dir: str | Path = "data/raw/bronze",
    manifest_path: str | Path = DEFAULT_BRONZE_MANIFEST_PATH,
    data_cfg: Mapping[str, Any] | None = None,
    lakehouse_cfg: Mapping[str, Any] | None = None,
    source_uri: str | None = None,
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
    resolved_codec = resolve_compression(compression, fallback=fallback_compression)
    out_root = Path(output_dir)
    stem = derive_source_file_stem(src)
    effective_uri = source_uri or schema.doi_or_url or src.resolve().as_uri()
    partition_rel = Path(f"dataset={dataset}") / f"source_file_stem={stem}"
    target_dir = out_root / partition_rel

    try:
        checksums = verify_file_checksums(
            src,
            expected_sha256=expected_sha256,
            expected_md5=expected_md5,
        )
        manifest = load_manifest(manifest_path)
        manifest_key = make_manifest_key(dataset, stem)

        if should_skip_entry(
            manifest,
            dataset=dataset,
            source_file_stem=stem,
            sha256=checksums.sha256,
            md5=checksums.md5,
            output_dir=out_root,
            force=force,
        ):
            existing = manifest["entries"][manifest_key]
            existing_files = [
                (out_root / pf if not Path(pf).is_absolute() else Path(pf))
                for pf in existing["parquet_files"]
            ]
            s3_uris = tuple(existing.get("s3_uris", ()))
            if upload_s3 and not s3_uris and not dry_run:
                if lakehouse_cfg is None:
                    raise BronzeError("lakehouse_cfg is required when upload_s3=True.")
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

            return IngestionFileResult(
                dataset=dataset,
                source_file_stem=stem,
                source_file=src.name,
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
                source_file=src.name,
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
            if lakehouse_cfg is None:
                raise BronzeError("lakehouse_cfg is required when upload_s3=True.")
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
            source_file=src.name,
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
        )
        save_manifest_atomic(manifest_path, manifest)

        return IngestionFileResult(
            dataset=dataset,
            source_file_stem=stem,
            source_file=src.name,
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
    finally:
        # NEVER delete user-supplied --input files; clean up only temporary downloaded archives.
        if is_temp_download and cleanup_temp_archives and src.exists():
            src.unlink(missing_ok=True)


def _derive_filename_from_url(url: str, fallback_idx: int) -> str:
    clean = url.split("?", 1)[0].rstrip("/")
    name = Path(clean).name
    if not name:
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
    resolved_output_dir = Path(
        output_dir or bronze_cfg.get("output_dir", "data/raw/bronze")
    )
    resolved_manifest_path = Path(
        manifest_path
        or (Path(bronze_cfg["manifest_dir"]) / "bronze_manifest.json" if "manifest_dir" in bronze_cfg else DEFAULT_BRONZE_MANIFEST_PATH)
    )
    validate_manifest_path(resolved_manifest_path)

    dl_cfg = DownloadConfig.from_data_config(data_cfg)
    effective_concurrency = dl_cfg.max_concurrency if concurrency is None else concurrency
    validate_concurrency(effective_concurrency, max_allowed=dl_cfg.max_concurrency)

    effective_cleanup = (
        dl_cfg.cleanup_temp_archives
        if cleanup_temp_archives is None
        else bool(cleanup_temp_archives)
    )
    req_codec = compression or str(bronze_cfg.get("compression", "zstd"))
    fb_codec = fallback_compression or str(bronze_cfg.get("fallback_compression", "snappy"))

    schema = get_dataset_schema(dataset, data_cfg=data_cfg, lakehouse_cfg=lakehouse_cfg)
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
        if dry_run:
            resolved_codec = resolve_compression(req_codec, fallback=fb_codec)
            for idx, url in enumerate(urls):
                fname = _derive_filename_from_url(url, idx)
                stem = derive_source_file_stem(fname)
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
            created_ephemeral_dir = download_dir is None
            dl_root = (
                Path(tempfile.mkdtemp(prefix="ie212_bronze_dl_"))
                if created_ephemeral_dir
                else Path(download_dir)
            )
            dl_root.mkdir(parents=True, exist_ok=True)
            try:
                tasks = [
                    DownloadTask(
                        url=url,
                        dest_path=dl_root / f"{idx:04d}" / _derive_filename_from_url(url, idx),
                        expected_sha256=expected_sha256,
                        expected_md5=expected_md5,
                    )
                    for idx, url in enumerate(urls)
                ]
                downloaded = download_batch(
                    tasks,
                    config=dl_cfg,
                    concurrency=effective_concurrency,
                )
                for dl_res in downloaded:
                    res = ingest_local_file(
                        dl_res.dest_path,
                        dataset=dataset,
                        output_dir=resolved_output_dir,
                        manifest_path=resolved_manifest_path,
                        data_cfg=data_cfg,
                        lakehouse_cfg=lakehouse_cfg,
                        source_uri=dl_res.url,
                        compression=req_codec,
                        fallback_compression=fb_codec,
                        chunk_rows=chunk_rows,
                        expected_sha256=dl_res.sha256,
                        expected_md5=dl_res.md5,
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
                    results.append(res)
            finally:
                if created_ephemeral_dir and effective_cleanup and dl_root.exists():
                    shutil.rmtree(dl_root, ignore_errors=True)

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
