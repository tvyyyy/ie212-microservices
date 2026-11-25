"""CLI entrypoint to prepare data folders, download Alibaba v2021 datasets, and write manifest."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

from ie212.ingestion.download import (
    compute_file_hashes,
    download_casper_partitions,
    download_with_retry,
    ensure_data_directories,
    extract_time_window_sample_from_targz,
    inspect_gzip_csv_stats,
    load_data_config,
    load_or_fetch_casper_metadata,
    select_deterministic_casper_partitions,
    write_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Alibaba v2021 (OSS slices + CASPER Shuffled MSCallGraph) and generate manifest."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/data.yaml"),
        help="Path to configs/data.yaml",
    )
    parser.add_argument(
        "--casper-metadata-cache",
        type=Path,
        default=Path("artifacts/manifest/casper_dataverse_metadata.json"),
        help="Local JSON cache for Harvard Dataverse file listing",
    )
    parser.add_argument(
        "--casper-fraction",
        type=float,
        default=None,
        help="Override sampling.casper_raw_fraction (default from configs/data.yaml)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Concurrent workers for CASPER partition downloads",
    )
    parser.add_argument(
        "--sample-window-ms",
        type=int,
        default=600_000,
        help="Max timestamp (ms) extracted into data/sample/ for Node/MSResource/MSRTQps",
    )
    parser.add_argument(
        "--mode",
        choices=("dev", "full-oss"),
        default="dev",
        help="dev: Node_0 + MSResource_0 + MSRTQps_0 + CASPER fraction; full-oss: all OSS indices + CASPER fraction",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path.cwd()
    cfg = load_data_config(args.config)
    dirs = ensure_data_directories(repo_root, cfg)

    seed = int(cfg["dataset"]["seed"])
    base_url = str(cfg["sources"]["alibaba_oss_base_url"]).rstrip("/")
    casper_cfg = cfg["sources"]["casper_dataverse"]
    sampling_cfg = cfg["sampling"]

    fraction = (
        args.casper_fraction
        if args.casper_fraction is not None
        else float(sampling_cfg["casper_raw_fraction"])
    )
    dev_casper_n = int(sampling_cfg.get("casper_dev_sample_count", 5))

    manifest_files: list[dict[str, Any]] = []

    # 1. Alibaba OSS: Node, MSResource, MSRTQps
    if args.mode == "dev":
        node_files = ["node/Node_0.tar.gz"]
        res_indices = list(sampling_cfg["oss_dev_slice_indices"]["msresource"])
        rtqps_indices = list(sampling_cfg["oss_dev_slice_indices"]["msrtqps"])
    else:
        node_files = list(cfg["sources"]["oss_files"]["node"])
        res_indices = list(cfg["sources"]["oss_files"]["msresource_indices"])
        rtqps_indices = list(cfg["sources"]["oss_files"]["msrtqps_indices"])

    oss_targets: list[tuple[str, str, Path, int]] = []
    for rel in node_files:
        oss_targets.append(("Node", rel, dirs["raw/node"] / Path(rel).name, 1))
    for i in res_indices:
        rel = f"MSResource/MSResource_{i}.tar.gz"
        # Verified live schema: timestamp is LAST column (index 6 after leading index column)
        oss_targets.append(("MSResource", rel, dirs["raw/msresource"] / Path(rel).name, 6))
    for i in rtqps_indices:
        rel = f"MSRTQps/MSRTQps_{i}.tar.gz"
        oss_targets.append(("MSRTQps", rel, dirs["raw/msrtqps"] / Path(rel).name, 1))

    for table, rel_url, dest_path, ts_col in oss_targets:
        url = f"{base_url}/{rel_url}"
        print(f"[OSS] Downloading {table}: {url} -> {dest_path}")
        info = download_with_retry(url=url, dest_path=dest_path, timeout_s=300)
        manifest_files.append(
            {
                "table": table,
                "tier": "raw",
                "filename": dest_path.name,
                "relative_path": dest_path.relative_to(repo_root).as_posix(),
                "source_url": url,
                "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                **info,
            }
        )

    # Extract coherent time-window dev samples for Node_0, MSResource_0, MSRTQps_0
    sample_specs = [
        ("Node", dirs["raw/node"] / "Node_0.tar.gz", dirs["sample/node"] / "Node_sample_0_10m.csv.gz", 1),
        (
            "MSResource",
            dirs["raw/msresource"] / "MSResource_0.tar.gz",
            dirs["sample/msresource"] / "MSResource_sample_0_10m.csv.gz",
            6,
        ),
        (
            "MSRTQps",
            dirs["raw/msrtqps"] / "MSRTQps_0.tar.gz",
            dirs["sample/msrtqps"] / "MSRTQps_sample_0_10m.csv.gz",
            1,
        ),
    ]
    for table, src_tar, out_gz, ts_col in sample_specs:
        if src_tar.exists():
            print(f"[SAMPLE] Extracting [0, {args.sample_window_ms}] ms from {src_tar.name} -> {out_gz.name}")
            s_info = extract_time_window_sample_from_targz(
                tar_gz_path=src_tar,
                out_csv_gz_path=out_gz,
                timestamp_col_index=ts_col,
                max_timestamp_ms=args.sample_window_ms,
            )
            manifest_files.append(
                {
                    "table": table,
                    "tier": "sample",
                    "filename": out_gz.name,
                    "relative_path": out_gz.relative_to(repo_root).as_posix(),
                    "derived_from": src_tar.relative_to(repo_root).as_posix(),
                    "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                    **s_info,
                }
            )

    # 2. Harvard Dataverse CASPER Shuffled v2021 MSCallGraph
    casper_all = load_or_fetch_casper_metadata(
        api_url=casper_cfg["api_dataset_url"],
        cache_path=args.casper_metadata_cache,
    )
    selected = select_deterministic_casper_partitions(casper_all, fraction=fraction, seed=seed)
    print(
        f"[CASPER] Selected {len(selected)}/{len(casper_all)} partitions "
        f"({sum(f['filesize'] for f in selected) / (1024**3):.2f} GiB compressed, seed={seed})"
    )
    casper_records = download_casper_partitions(
        selected_files=selected,
        url_template=casper_cfg["datafile_url_template"],
        dest_dir=dirs["raw/mscallgraph"],
        repo_root=repo_root,
        doi=casper_cfg["doi"],
        max_workers=args.max_workers,
    )
    manifest_files.extend(casper_records)

    # Copy first N deterministic CASPER partitions into data/sample/mscallgraph/ and inspect stats
    for rec in casper_records[:dev_casper_n]:
        src_p = repo_root / rec["relative_path"]
        dst_p = dirs["sample/mscallgraph"] / rec["filename"]
        shutil.copy2(src_p, dst_p)
        sha256, md5, size = compute_file_hashes(dst_p)
        # Verified live schema: traceid=1, timestamp=2 (0 is unnamed index)
        g_stats = inspect_gzip_csv_stats(dst_p, timestamp_col_index=2)
        manifest_files.append(
            {
                "table": "MSCallGraph",
                "tier": "sample",
                "filename": dst_p.name,
                "relative_path": dst_p.relative_to(repo_root).as_posix(),
                "derived_from": rec["relative_path"],
                "doi": casper_cfg["doi"],
                "size_bytes": size,
                "sha256": sha256,
                "md5": md5,
                "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                **g_stats,
            }
        )

    manifest = {
        "manifest_version": "1.0",
        "dataset": cfg["dataset"]["name"],
        "seed": seed,
        "mode": args.mode,
        "scope_note": (
            f"MSCallGraph: {len(selected)}/{len(casper_all)} deterministic CASPER v2021 partitions "
            f"(seed={seed}); Node: {len(node_files)}/1 archive; "
            f"MSResource: {len(res_indices)}/12 archives; MSRTQps: {len(rtqps_indices)}/25 archives."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_count": len(manifest_files),
        "files": manifest_files,
    }
    manifest_out = dirs["manifest"] / "manifest.json"
    write_manifest(manifest_out, manifest)
    print(f"[MANIFEST] Wrote {manifest_out} ({len(manifest_files)} entries)")


if __name__ == "__main__":
    main()
