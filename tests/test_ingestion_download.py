"""Unit tests for deterministic CASPER selection and data folder layout (T04, T07)."""

from pathlib import Path
import gzip
import io
import tarfile
import pytest

from ie212.ingestion.download import (
    ensure_data_directories,
    extract_time_window_sample_from_targz,
    load_data_config,
    inspect_gzip_csv_stats,
    select_deterministic_casper_partitions,
)
from ie212.ingestion.schemas import select_casper_partitions


def test_canonical_config_and_sampling_match_bronze():
    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_data_config(repo_root / "configs" / "data.yaml")
    assert cfg["seed"] == 1217
    sampling = cfg["datasets"]["casper_mscallgraph"]["sampling"]
    assert sampling["seed"] == cfg["seed"]
    files = [{"filename": f"partition_id={i}.csv.gz"} for i in range(-999, 1000)]
    selected = select_deterministic_casper_partitions(files, sampling["ratio"], cfg["seed"])
    assert [f["filename"] for f in selected] == select_casper_partitions(
        1999, ratio=sampling["ratio"], seed=cfg["seed"]
    )


@pytest.mark.parametrize("leading_index", [False, True])
def test_timestamp_stats_resolve_header_and_reject_wrong_index(tmp_path: Path, leading_index: bool):
    path = tmp_path / "calls.csv.gz"
    header = "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
    rows = ["t1,419,0,a,rpc,b,fn,1\n", "t2,43197984,0.1,a,rpc,b,fn,2\n"]
    if leading_index:
        header = "," + header
        rows = [f"{i}," + row for i, row in enumerate(rows)]
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(header + "".join(rows))
    assert inspect_gzip_csv_stats(path) == {
        "row_count": 2, "min_timestamp_ms": 419, "max_timestamp_ms": 43197984,
    }
    with pytest.raises(ValueError, match="does not match CSV header"):
        inspect_gzip_csv_stats(path, timestamp_col_index=1 if leading_index else 2)


def test_ensure_data_directories_creates_expected_tree(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_data_config(repo_root / "configs" / "data.yaml")
    created = ensure_data_directories(tmp_path, cfg)
    for sub in ("node", "msresource", "msrtqps", "mscallgraph"):
        assert (tmp_path / "data" / "raw" / sub).is_dir()
        assert (tmp_path / "data" / "sample" / sub).is_dir()
        assert f"raw/{sub}" in created
        assert f"sample/{sub}" in created
    assert (tmp_path / "artifacts" / "manifest").is_dir()


def test_deterministic_casper_selection_is_reproducible():
    fake_files = [
        {"id": i, "filename": f"partition_id={i}.csv.gz", "filesize": 1000, "md5": "abc"}
        for i in range(100)
    ]
    sel1 = select_deterministic_casper_partitions(fake_files, fraction=0.10, seed=1217)
    sel2 = select_deterministic_casper_partitions(fake_files, fraction=0.10, seed=1217)
    sel_other = select_deterministic_casper_partitions(fake_files, fraction=0.10, seed=99)
    assert len(sel1) == 10
    assert sel1 == sel2
    assert sel1 != sel_other


def test_extract_time_window_sample_from_targz(tmp_path: Path):
    csv_bytes = (
        ",timestamp,msname,msinstanceid,metric,value\n"
        "0,0,svc_a,c1,providerRPC_MCR,10\n"
        "1,600000,svc_a,c1,providerRPC_RT,25\n"
        "2,660000,svc_a,c1,providerRPC_RT,99\n"
    ).encode("utf-8")
    tar_path = tmp_path / "sample.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tf:
        info = tarfile.TarInfo("sample.csv")
        info.size = len(csv_bytes)
        tf.addfile(info, io.BytesIO(csv_bytes))

    out_gz = tmp_path / "out.csv.gz"
    stats = extract_time_window_sample_from_targz(
        tar_gz_path=tar_path,
        out_csv_gz_path=out_gz,
        timestamp_col_index=1,
        max_timestamp_ms=600_000,
    )
    assert stats["source_archive_rows_scanned"] == 3
    assert stats["row_count"] == 2
    assert stats["min_timestamp_ms"] == 0
    assert stats["max_timestamp_ms"] == 600_000
    with gzip.open(out_gz, "rt", encoding="utf-8") as fh:
        lines = [line.strip() for line in fh if line.strip()]
    assert len(lines) == 3
