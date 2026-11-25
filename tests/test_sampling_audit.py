"""Synthetic metadata and files: selected cohort and integrity must agree."""
import hashlib
import importlib.util
import json
from pathlib import Path
import pytest
from ie212.ingestion.schemas import select_casper_partitions


def test_sampling_audit_checks_physical_hashes(tmp_path):
    spec = importlib.util.spec_from_file_location("audit_sampling", Path(__file__).parents[1] / "scripts/audit_sampling.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    filenames = [f"partition_id={i}.csv.gz" for i in range(10)]
    payload = b"synthetic fixture, not Alibaba data"
    chosen = select_casper_partitions(filenames, ratio=0.1, seed=1217)[0]
    path = tmp_path / chosen
    path.write_bytes(payload)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"seed": 1217, "files": [{
        "table": "MSCallGraph", "tier": "raw", "filename": chosen,
        "relative_path": str(path), "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }]}))
    metadata = tmp_path / "metadata.json"
    metadata.write_text(json.dumps({"data": {"latestVersion": {
        "versionNumber": 1, "versionMinorNumber": 0, "releaseTime": "synthetic",
        "files": [{"dataFile": {"filename": name, "filesize": len(payload), "md5": hashlib.md5(payload).hexdigest()}} for name in filenames],
    }}}))
    config = {"seed": 1217, "datasets": {"casper_mscallgraph": {
        "total_partitions": 10, "sampling": {"ratio": 0.1, "seed": 1217},
    }}}
    result = module.audit(config, manifest, metadata, tmp_path / "result.json")
    assert result["selected_partitions"] == [chosen]
    path.write_bytes(b"tampered synthetic fixture")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        module.audit(config, manifest, metadata, tmp_path / "result.json")
