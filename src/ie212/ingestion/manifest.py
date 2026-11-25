"""Bronze manifest persistence, provenance tracking, and idempotent skip detection."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

PROTECTED_MANIFEST_NAMES: frozenset[str] = frozenset({"sample_manifest.json"})
DEFAULT_BRONZE_MANIFEST_PATH = Path("artifacts/manifest/bronze_manifest.json")


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_manifest_path(path: str | Path) -> Path:
    """Reject protected manifest filenames (such as Vy's sample_manifest.json)."""
    resolved = Path(path)
    if resolved.name.lower() in PROTECTED_MANIFEST_NAMES:
        raise ValueError(
            f"Refusing to access protected manifest path '{resolved}'. "
            "Use 'bronze_manifest.json' or a custom path instead."
        )
    return resolved


def make_manifest_key(dataset: str, source_file_stem: str) -> str:
    """Build deterministic manifest dictionary key for a partitioned source file."""
    return f"{dataset}::{source_file_stem}"


def load_manifest(path: str | Path = DEFAULT_BRONZE_MANIFEST_PATH) -> dict[str, Any]:
    """Load Bronze manifest JSON from disk, or initialize an empty manifest structure."""
    target = validate_manifest_path(path)
    if not target.is_file():
        return {
            "schema_version": 1,
            "pipeline": "ie212_bronze",
            "updated_at_utc": None,
            "entries": {},
        }

    data = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in manifest {target}, got {type(data).__name__}.")
    entries = data.get("entries")
    if entries is None:
        data["entries"] = {}
    elif not isinstance(entries, dict):
        raise ValueError(f"Expected 'entries' object in manifest {target}.")
    data.setdefault("schema_version", 1)
    data.setdefault("pipeline", "ie212_bronze")
    return data


def save_manifest_atomic(path: str | Path, manifest: Mapping[str, Any]) -> Path:
    """Atomically persist manifest JSON using a temporary sibling file and os.replace."""
    target = validate_manifest_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    payload = dict(manifest)
    payload["updated_at_utc"] = utc_now_iso()

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.stem}_",
        suffix=".tmp",
        dir=str(target.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return target


def _resolve_parquet_path(raw_path: str, output_dir: Path | None) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute() or output_dir is None:
        return candidate
    return output_dir / candidate


def should_skip_entry(
    manifest: Mapping[str, Any],
    *,
    dataset: str,
    source_file_stem: str,
    sha256: str,
    md5: str,
    output_dir: str | Path | None = None,
    force: bool = False,
) -> bool:
    """Return True if an identical completed Bronze partition already exists on disk."""
    if force:
        return False

    entries = manifest.get("entries")
    if not isinstance(entries, Mapping):
        return False

    key = make_manifest_key(dataset, source_file_stem)
    entry = entries.get(key)
    if not isinstance(entry, Mapping):
        return False
    if entry.get("status") != "completed":
        return False
    if entry.get("source_sha256") != sha256 or entry.get("source_md5") != md5:
        return False

    parquet_files = entry.get("parquet_files")
    if not isinstance(parquet_files, Sequence) or isinstance(parquet_files, (str, bytes)) or not parquet_files:
        return False

    base_out = Path(output_dir) if output_dir is not None else None
    observed_rows = 0
    for raw_file in parquet_files:
        resolved = _resolve_parquet_path(str(raw_file), base_out)
        if not resolved.is_file() or resolved.stat().st_size == 0:
            return False
        try:
            meta = pq.read_metadata(resolved)
            observed_rows += meta.num_rows
        except Exception:
            return False

    expected_rows = entry.get("row_count")
    if isinstance(expected_rows, int) and observed_rows != expected_rows:
        return False

    return True


def build_manifest_entry(
    *,
    dataset: str,
    source_file_stem: str,
    source_file: str,
    source_uri: str,
    source_sha256: str,
    source_md5: str,
    source_size_bytes: int,
    row_count: int,
    parquet_files: Sequence[str],
    compression: str,
    dropped_leading_index: bool,
    timestamp_min: int | None,
    timestamp_max: int | None,
    null_timestamp_count: int,
    ingested_at_utc: str,
    status: str = "completed",
    s3_uris: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Create a normalized manifest entry dictionary."""
    entry: dict[str, Any] = {
        "dataset": dataset,
        "source_file_stem": source_file_stem,
        "source_file": source_file,
        "source_uri": source_uri,
        "source_sha256": source_sha256,
        "source_md5": source_md5,
        "source_size_bytes": source_size_bytes,
        "row_count": row_count,
        "parquet_files": list(parquet_files),
        "compression": compression,
        "dropped_leading_index": dropped_leading_index,
        "timestamp_min": timestamp_min,
        "timestamp_max": timestamp_max,
        "null_timestamp_count": null_timestamp_count,
        "status": status,
        "ingested_at_utc": ingested_at_utc,
    }
    if s3_uris:
        entry["s3_uris"] = list(s3_uris)
    return entry


__all__ = [
    "DEFAULT_BRONZE_MANIFEST_PATH",
    "PROTECTED_MANIFEST_NAMES",
    "build_manifest_entry",
    "load_manifest",
    "make_manifest_key",
    "save_manifest_atomic",
    "should_skip_entry",
    "utc_now_iso",
    "validate_manifest_path",
]
