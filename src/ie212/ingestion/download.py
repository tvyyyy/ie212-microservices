"""Reproducible Alibaba v2021 & CASPER Shuffled v2021 acquisition and manifest builder (T04, T07).

Implements docs/sampling_plan.md & agents_ie212.md §4:
- Deterministic hash-partition sampling of CASPER Shuffled v2021 (`doi:10.7910/DVN/RXIC9Z`)
  using `seed` so selected `traceid` partitions remain complete without cross-file fragmentation.
- Resumable/checksum-verified downloads from Alibaba OSS (`Node`, `MSResource`, `MSRTQps`)
  and Harvard Dataverse (`MSCallGraph`).
- Time-window dev-sample extraction for `Node`, `MSResource`, and `MSRTQps` (never `head(N)`
  on `MSRTQps`, preserving complete metric blocks per `(msname, msinstanceid, timestamp)`).
- Full SHA-256 / MD5 / row-count / time-range manifest output to `artifacts/manifest/`.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import codecs
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import random
import shutil
import tarfile
import time
from typing import Any, Iterable, Mapping
import urllib.error
import urllib.request

import yaml

SUBFOLDERS: tuple[str, ...] = ("node", "msresource", "msrtqps", "mscallgraph")


def load_data_config(config_path: Path) -> dict[str, Any]:
    """Load `configs/data.yaml`."""
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def ensure_data_directories(repo_root: Path, cfg: Mapping[str, Any]) -> dict[str, Path]:
    """Create `data/raw/*`, `data/sample/*`, and `artifacts/manifest/` directories."""
    paths_cfg = cfg["paths"]
    subfolders = tuple(paths_cfg.get("subfolders", SUBFOLDERS))
    created: dict[str, Path] = {}

    raw_root = repo_root / paths_cfg["raw_root"]
    sample_root = repo_root / paths_cfg["sample_root"]
    manifest_dir = repo_root / paths_cfg["manifest_dir"]
    manifest_dir.mkdir(parents=True, exist_ok=True)
    created["manifest"] = manifest_dir

    for sub in subfolders:
        r_dir = raw_root / sub
        s_dir = sample_root / sub
        r_dir.mkdir(parents=True, exist_ok=True)
        s_dir.mkdir(parents=True, exist_ok=True)
        created[f"raw/{sub}"] = r_dir
        created[f"sample/{sub}"] = s_dir

    return created


def compute_file_hashes(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, str, int]:
    """Return `(sha256_hex, md5_hex, size_bytes)` for a file."""
    h_sha256 = hashlib.sha256()
    h_md5 = hashlib.md5()
    total = 0
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h_sha256.update(chunk)
            h_md5.update(chunk)
            total += len(chunk)
    return h_sha256.hexdigest(), h_md5.hexdigest(), total


def download_with_retry(
    url: str,
    dest_path: Path,
    expected_md5: str | None = None,
    expected_size: int | None = None,
    max_retries: int = 4,
    timeout_s: int = 120,
) -> dict[str, Any]:
    """Download URL atomically via `.part` file, skipping if already valid on disk."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        actual_size = dest_path.stat().st_size
        if expected_size is None or actual_size == expected_size:
            sha256, md5, size = compute_file_hashes(dest_path)
            if expected_md5 is None or md5.lower() == expected_md5.lower():
                return {
                    "status": "cached",
                    "size_bytes": size,
                    "sha256": sha256,
                    "md5": md5,
                    "md5_verified": expected_md5 is not None,
                }

    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    last_err: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "ie212-microservices-acquisition/1.0"}
            )
            with urllib.request.urlopen(req, timeout=timeout_s) as resp, tmp_path.open(
                "wb"
            ) as out_fh:
                shutil.copyfileobj(resp, out_fh, length=1024 * 1024)

            sha256, md5, size = compute_file_hashes(tmp_path)
            if expected_size is not None and size != expected_size:
                raise ValueError(
                    f"Size mismatch for {dest_path.name}: got {size}, expected {expected_size}"
                )
            if expected_md5 is not None and md5.lower() != expected_md5.lower():
                raise ValueError(
                    f"MD5 mismatch for {dest_path.name}: got {md5}, expected {expected_md5}"
                )
            tmp_path.replace(dest_path)
            return {
                "status": "downloaded",
                "size_bytes": size,
                "sha256": sha256,
                "md5": md5,
                "md5_verified": expected_md5 is not None,
            }
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
            last_err = exc
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            if attempt < max_retries:
                time.sleep(min(2**attempt, 16))

    raise RuntimeError(f"Failed downloading {url} after {max_retries} attempts: {last_err}")


def load_or_fetch_casper_metadata(
    api_url: str,
    cache_path: Path,
) -> list[dict[str, Any]]:
    """Load Dataverse dataset file list from cache or fetch live via API."""
    if cache_path.exists():
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        req = urllib.request.Request(
            api_url, headers={"User-Agent": "ie212-microservices-acquisition/1.0"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw_bytes = resp.read()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(raw_bytes)
        payload = json.loads(raw_bytes.decode("utf-8"))

    raw_files = payload["data"]["latestVersion"]["files"]
    entries = []
    for item in raw_files:
        df = item["dataFile"]
        entries.append(
            {
                "id": int(df["id"]),
                "filename": str(df["filename"]),
                "filesize": int(df["filesize"]),
                "md5": str(df.get("md5") or ""),
            }
        )
    return sorted(entries, key=lambda x: x["filename"])


def select_deterministic_casper_partitions(
    files: list[dict[str, Any]],
    fraction: float,
    seed: int,
) -> list[dict[str, Any]]:
    """Select deterministic subset of CASPER `partition_id=*.csv.gz` files."""
    if not 0.0 < fraction <= 1.0:
        raise ValueError("fraction must be in (0, 1]")
    ordered = sorted(files, key=lambda x: x["filename"])
    target_count = max(1, round(len(ordered) * fraction))
    rng = random.Random(seed)
    indices = sorted(rng.sample(range(len(ordered)), target_count))
    return [ordered[i] for i in indices]


def download_casper_partitions(
    selected_files: list[dict[str, Any]],
    url_template: str,
    dest_dir: Path,
    repo_root: Path,
    doi: str,
    max_workers: int = 4,
) -> list[dict[str, Any]]:
    """Download selected CASPER partitions concurrently and return manifest records."""
    records: list[dict[str, Any]] = []

    def _task(entry: dict[str, Any]) -> dict[str, Any]:
        url = url_template.format(file_id=entry["id"])
        dest = dest_dir / entry["filename"]
        info = download_with_retry(
            url=url,
            dest_path=dest,
            expected_md5=entry["md5"] or None,
            expected_size=entry["filesize"],
        )
        return {
            "table": "MSCallGraph",
            "tier": "raw",
            "filename": entry["filename"],
            "relative_path": dest.relative_to(repo_root).as_posix(),
            "source_url": url,
            "doi": doi,
            "dataverse_file_id": entry["id"],
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            **info,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_task, entry): entry for entry in selected_files}
        done_count = 0
        total = len(selected_files)
        for fut in as_completed(futures):
            rec = fut.result()
            records.append(rec)
            done_count += 1
            if done_count % 25 == 0 or done_count == total:
                print(f"[CASPER] {done_count}/{total} partitions ready ({rec['filename']})")

    return sorted(records, key=lambda r: r["filename"])


def extract_time_window_sample_from_targz(
    tar_gz_path: Path,
    out_csv_gz_path: Path,
    timestamp_col_index: int,
    max_timestamp_ms: int = 600_000,
    min_timestamp_ms: int = 0,
) -> dict[str, Any]:
    """Stream a `.tar.gz` CSV and write rows in `[min_timestamp_ms, max_timestamp_ms]` to `.csv.gz`.

    Filters strictly by timestamp window rather than `head(N)` so metric blocks in
    `MSRTQps` and half-windows in `Node`/`MSResource` stay intact.
    """
    out_csv_gz_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    kept_rows = 0
    obs_min_ts: int | None = None
    obs_max_ts: int | None = None

    with tarfile.open(tar_gz_path, mode="r|gz") as tf:
        for member in tf:
            if not member.isfile():
                continue
            raw_stream = tf.extractfile(member)
            if raw_stream is None:
                continue
            text_in = codecs.getreader("utf-8")(raw_stream, errors="replace")
            reader = csv.reader(text_in)
            header = next(reader, None)
            if header is None:
                continue

            with gzip.open(out_csv_gz_path, mode="wt", encoding="utf-8", newline="") as gz_out:
                writer = csv.writer(gz_out)
                writer.writerow(header)
                for row in reader:
                    if not row:
                        continue
                    total_rows += 1
                    try:
                        ts = int(row[timestamp_col_index])
                    except (IndexError, ValueError):
                        continue
                    if min_timestamp_ms <= ts <= max_timestamp_ms:
                        writer.writerow(row)
                        kept_rows += 1
                        if obs_min_ts is None or ts < obs_min_ts:
                            obs_min_ts = ts
                        if obs_max_ts is None or ts > obs_max_ts:
                            obs_max_ts = ts
            break

    sha256, md5, size = compute_file_hashes(out_csv_gz_path)
    return {
        "source_archive_rows_scanned": total_rows,
        "row_count": kept_rows,
        "min_timestamp_ms": obs_min_ts,
        "max_timestamp_ms": obs_max_ts,
        "size_bytes": size,
        "sha256": sha256,
        "md5": md5,
    }


def inspect_gzip_csv_stats(csv_gz_path: Path, timestamp_col_index: int) -> dict[str, Any]:
    """Count rows and min/max timestamp in a `.csv.gz` file."""
    row_count = 0
    obs_min: int | None = None
    obs_max: int | None = None
    with gzip.open(csv_gz_path, mode="rt", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # skip header
        for row in reader:
            if not row:
                continue
            row_count += 1
            try:
                ts = int(row[timestamp_col_index])
                if obs_min is None or ts < obs_min:
                    obs_min = ts
                if obs_max is None or ts > obs_max:
                    obs_max = ts
            except (IndexError, ValueError):
                continue
    return {
        "row_count": row_count,
        "min_timestamp_ms": obs_min,
        "max_timestamp_ms": obs_max,
    }


def write_manifest(manifest_path: Path, manifest_payload: Mapping[str, Any]) -> None:
    """Persist manifest JSON cleanly."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
