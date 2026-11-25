"""T07 audit acquisition selection, physical files, and development-sample scope."""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from ie212.config import load_config
from ie212.ingestion.schemas import select_casper_partitions


def audit(config, manifest_path: Path, metadata_path: Path, output: Path):
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))["data"]["latestVersion"]
    listing = [f["dataFile"] for f in metadata["files"] if f["dataFile"]["filename"].startswith("partition_id=")]
    cfg = config["datasets"]["casper_mscallgraph"]
    if cfg["sampling"]["seed"] != config["seed"]:
        raise ValueError("CASPER and global seeds differ")
    if len(listing) != cfg["total_partitions"]:
        raise ValueError("Dataverse partition population differs from config")
    selected = select_casper_partitions([f["filename"] for f in listing], ratio=cfg["sampling"]["ratio"], seed=config["seed"])
    raw = [f for f in manifest["files"] if f["table"] == "MSCallGraph" and f["tier"] == "raw"]
    all_raw = [f for f in manifest["files"] if f["tier"] == "raw"]
    samples = [f for f in manifest["files"] if f["tier"] == "sample"]
    inventory = []
    upstream_md5 = {f["filename"]: f["md5"] for f in listing}
    for entry in all_raw + samples:
        path = Path(entry["relative_path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        sha, md5 = hashlib.sha256(), hashlib.md5()
        with path.open("rb") as fh:
            for data in iter(lambda: fh.read(1024 * 1024), b""):
                sha.update(data)
                md5.update(data)
        if sha.hexdigest() != entry["sha256"]:
            raise ValueError(f"Manifest SHA-256 mismatch: {path}")
        if entry["table"] == "MSCallGraph" and md5.hexdigest() != upstream_md5[path.name]:
            raise ValueError(f"Dataverse MD5 mismatch: {path}")
        inventory.append({"file": path.name, "table": entry["table"], "tier": entry["tier"], "bytes": path.stat().st_size,
                          "sha256_verified": True, "upstream_md5_verified": entry["table"] == "MSCallGraph"})
    observed = sorted(e["filename"] for e in raw)
    if selected != observed:
        raise ValueError("Acquired partitions differ from the canonical deterministic selection")
    if manifest["seed"] != config["seed"]:
        raise ValueError("Acquisition and canonical configuration seeds differ")
    report = {
        "task": "T07", "status": "passed", "seed": config["seed"],
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "doi": "10.7910/DVN/RXIC9Z", "dataverse_version": f"{metadata['versionNumber']}.{metadata['versionMinorNumber']}",
        "dataverse_release": metadata["releaseTime"], "population_partitions": len(listing),
        "raw_partitions": len(raw), "raw_partition_fraction": len(raw) / len(listing),
        "raw_casper_bytes": sum(e["size_bytes"] for e in raw),
        "population_casper_bytes": sum(e["filesize"] for e in listing),
        "raw_oss_archives": {table: sum(e["table"] == table for e in all_raw) for table in ("Node", "MSResource", "MSRTQps")},
        "selected_partitions": selected,
        "development_casper_files": [e["filename"] for e in samples if e["table"] == "MSCallGraph"],
        "development_oss_window": "0 <= timestamp <= 600000 ms; 11 minute buckets, last partial",
        "development_bias": "CASPER development files are the first 5 lexicographically sorted acquired partitions, not a new independent random sample.",
        "trace_policy": "Select whole CASPER partitions; hash locality does not imply valid/complete call topology or unbiased service coverage.",
        "manifest": manifest_path.as_posix(), "files_verified": inventory,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k:v for k,v in report.items() if k not in ("selected_partitions", "files_verified")}, indent=2))
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="configs/data.yaml")
    p.add_argument("--manifest", default="artifacts/manifest/manifest.json")
    p.add_argument("--metadata", default="artifacts/manifest/casper_dataverse_metadata.json")
    p.add_argument("--output", default="reports/data_quality/t07_sampling_audit.json")
    args = p.parse_args()
    audit(load_config(args.config), Path(args.manifest), Path(args.metadata), Path(args.output))
