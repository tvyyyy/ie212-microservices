"""Unit and integration tests for the IE212 Bronze ingestion pipeline (T09)."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest

from ie212.config import load_project_configs
from ie212.ingestion import (
    BronzeError,
    BronzeSchemaError,
    ChecksumMismatchError,
    DownloadConfig,
    DownloadTask,
    S3UploadError,
    build_s3_client,
    compute_file_checksums,
    download_batch,
    download_file_resumable,
    get_dataset_schema,
    ingest_bronze,
    ingest_local_file,
    load_manifest,
    normalize_csv_header,
    save_manifest_atomic,
    select_casper_partitions,
    should_skip_entry,
    validate_manifest_path,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_csv_gz(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as fh:
        fh.write(text)
    return path


def _add_tar_bytes(tar: tarfile.TarFile, name: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(payload)
    tar.addfile(info, io.BytesIO(payload))


def _write_tar_gz(path: Path, members: dict[str, bytes]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, mode="w:gz") as tar:
        for member_name, payload in members.items():
            _add_tar_bytes(tar, member_name, payload)
    return path


def _gzip_bytes(text: str) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(text.encode("utf-8"))
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 1. CASPER 8-column header preservation & partition subset selection
# ---------------------------------------------------------------------------


def test_casper_8_col_preserves_traceid_and_sentinels(tmp_path: Path) -> None:
    casper_csv = (
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "tr_001,1627804800100,0,svc_a,rpc,svc_b,getUser,12.5\n"
        "tr_002,1627804800200,0.1,svc_b,mq,(?),NAN,-4.0\n"
        "tr_003,1627804800300,0.2,svc_c,http,svc_d,,0.0\n"
    )
    src = _write_csv_gz(tmp_path / "partition_id=00001.csv.gz", casper_csv)
    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "manifest" / "bronze_manifest.json"

    result = ingest_local_file(
        src,
        dataset="casper_mscallgraph",
        output_dir=out_dir,
        manifest_path=manifest_path,
    )

    assert result.status == "completed"
    assert result.row_count == 3
    assert result.dropped_leading_index is False
    assert result.source_file_stem == "partition_id_00001"
    assert result.source_uri == "doi:10.7910/DVN/RXIC9Z"

    parquet_path = out_dir / "dataset=casper_mscallgraph" / "source_file_stem=partition_id_00001" / "part-00000.parquet"
    assert parquet_path.is_file()

    table = pq.read_table(parquet_path)
    # Hive keys must NOT be duplicated inside Parquet data columns.
    assert "dataset" not in table.column_names
    assert "source_file_stem" not in table.column_names
    assert table.column_names[:8] == [
        "traceid",
        "timestamp",
        "rpcid",
        "um",
        "rpctype",
        "dm",
        "interface",
        "rt",
    ]
    assert table.column_names[8:] == [
        "source_file",
        "source_uri",
        "source_sha256",
        "source_md5",
        "ingested_at_utc",
    ]

    rows = table.to_pylist()
    assert [r["traceid"] for r in rows] == ["tr_001", "tr_002", "tr_003"]
    assert rows[1]["rt"] == -4.0  # Negative mq rt preserved
    assert rows[1]["dm"] == "(?)"  # Sentinel preserved verbatim
    assert rows[1]["interface"] == "NAN"
    assert rows[2]["interface"] == ""

    glob_pat = (out_dir / "dataset=casper_mscallgraph" / "source_file_stem=*" / "*.parquet").as_posix()
    con = duckdb.connect()
    try:
        duck_rows = con.execute(
            f"SELECT dataset, source_file_stem, COUNT(*) FROM read_parquet('{glob_pat}', hive_partitioning=true) GROUP BY 1, 2"
        ).fetchall()
    finally:
        con.close()
    assert duck_rows == [("casper_mscallgraph", "partition_id_00001", 3)]


def test_duckdb_hive_cross_dataset_casper_and_oss_mscallgraph(tmp_path: Path) -> None:
    casper_src = _write_csv_gz(
        tmp_path / "partition_id=00001.csv.gz",
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "c_1,1000,0,svc_a,rpc,svc_b,op1,1.5\n"
        "c_2,2000,0.1,svc_b,mq,(?),NAN,-2.0\n",
    )
    oss_csv = (
        ",traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "0,o_1,3000,0,svc_c,http,svc_d,op2,4.5\n"
        "1,o_2,4000,0.1,svc_d,db,db1,query,7.5\n"
    ).encode("utf-8")
    oss_src = _write_tar_gz(tmp_path / "MSCallGraph_0.tar.gz", {"MSCallGraph_0.csv": oss_csv})

    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "bronze_manifest.json"
    ingest_local_file(casper_src, dataset="casper_mscallgraph", output_dir=out_dir, manifest_path=manifest_path)
    ingest_local_file(oss_src, dataset="oss_mscallgraph", output_dir=out_dir, manifest_path=manifest_path)

    glob_pat = (out_dir / "dataset=*" / "source_file_stem=*" / "*.parquet").as_posix()
    con = duckdb.connect()
    try:
        duck_rows = con.execute(
            f"SELECT dataset, source_file_stem, COUNT(*) FROM read_parquet('{glob_pat}', hive_partitioning=true) "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        ).fetchall()
    finally:
        con.close()
    assert duck_rows == [
        ("casper_mscallgraph", "partition_id_00001", 2),
        ("oss_mscallgraph", "MSCallGraph_0", 2),
    ]


def test_casper_rejects_leading_unnamed_index_and_selects_partitions() -> None:
    schema = get_dataset_schema("casper_mscallgraph")
    with pytest.raises(BronzeSchemaError, match="must not have a leading unnamed index"):
        normalize_csv_header(
            ["", "traceid", "timestamp", "rpcid", "um", "rpctype", "dm", "interface", "rt"],
            schema,
        )

    subset_a = select_casper_partitions(1999, ratio=0.10, seed=42)
    subset_b = select_casper_partitions(1999, ratio=0.10, seed=42)
    assert subset_a == subset_b
    assert len(subset_a) == round(1999 * 0.10)
    assert all(item.startswith("partition_id=") and item.endswith(".csv.gz") for item in subset_a)

    with pytest.raises(ValueError):
        select_casper_partitions(1999, ratio=0.05, seed=42)
    with pytest.raises(ValueError):
        select_casper_partitions(1999, ratio=0.30, seed=42)


# ---------------------------------------------------------------------------
# 2. All 4 OSS dataset schemas & optional leading index normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dataset_name", "domain_cols", "sample_row"),
    [
        ("oss_node", ["timestamp", "nodeid", "node_cpu_usage", "node_memory_usage"], "30000,node_1,0.42,0.55"),
        (
            "oss_msresource",
            ["msname", "msinstanceid", "nodeid", "instance_cpu_usage", "instance_memory_usage", "timestamp"],
            "ms_a,inst_1,node_1,0.25,0.50,30000",
        ),
        ("oss_msrtqps", ["timestamp", "msname", "msinstanceid", "metric", "value"], "60000,ms_a,inst_1,HTTP_MCR,12.3"),
        (
            "oss_mscallgraph",
            ["traceid", "timestamp", "rpcid", "um", "rpctype", "dm", "interface", "rt"],
            "tr_1,60000,0,svc_a,rpc,svc_b,ping,3.2",
        ),
    ],
)
def test_oss_headers_all_four_datasets(
    tmp_path: Path,
    dataset_name: str,
    domain_cols: list[str],
    sample_row: str,
) -> None:
    # With leading unnamed ',' index column
    with_idx_csv = f",{','.join(domain_cols)}\n0,{sample_row}\n"
    src_with_idx = tmp_path / f"{dataset_name}_with_idx.csv"
    src_with_idx.write_text(with_idx_csv, encoding="utf-8")

    res_idx = ingest_local_file(
        src_with_idx,
        dataset=dataset_name,
        output_dir=tmp_path / "bronze",
        manifest_path=tmp_path / "manifest.json",
    )
    assert res_idx.dropped_leading_index is True
    assert res_idx.row_count == 1
    tbl_idx = pq.read_table(res_idx.parquet_files[0])
    assert tbl_idx.column_names[: len(domain_cols)] == domain_cols

    # Pre-stripped OSS CSV (no leading index) should also work without dropping col 0
    no_idx_csv = f"{','.join(domain_cols)}\n{sample_row}\n"
    src_no_idx = tmp_path / f"{dataset_name}_no_idx.csv"
    src_no_idx.write_text(no_idx_csv, encoding="utf-8")

    res_no_idx = ingest_local_file(
        src_no_idx,
        dataset=dataset_name,
        output_dir=tmp_path / "bronze",
        manifest_path=tmp_path / "manifest.json",
    )
    assert res_no_idx.dropped_leading_index is False
    assert res_no_idx.row_count == 1


def test_oss_rejects_misaligned_columns() -> None:
    msresource_schema = get_dataset_schema("oss_msresource")
    # timestamp erroneously placed first instead of last
    with pytest.raises(BronzeSchemaError, match="Header mismatch"):
        normalize_csv_header(
            ["", "timestamp", "msname", "msinstanceid", "nodeid", "instance_cpu_usage", "instance_memory_usage"],
            msresource_schema,
        )

    msrtqps_schema = get_dataset_schema("oss_msrtqps")
    # plural 'metrics' instead of singular 'metric'
    with pytest.raises(BronzeSchemaError, match="Header mismatch"):
        normalize_csv_header(
            ["", "timestamp", "msname", "msinstanceid", "metrics", "value"],
            msrtqps_schema,
        )


# ---------------------------------------------------------------------------
# 3. .tar.gz multi-member chunked streaming, codecs, and Hive readback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("codec", ["zstd", "snappy"])
def test_tar_gz_multi_member_chunked_streaming(tmp_path: Path, codec: str) -> None:
    csv_a = (
        ",msname,msinstanceid,nodeid,instance_cpu_usage,instance_memory_usage,timestamp\n"
        "0,ms_1,inst_1,node_1,0.1,0.2,30000\n"
        "1,ms_1,inst_2,node_1,0.3,0.4,60000\n"
        "2,ms_2,inst_3,node_2,0.5,0.6,90000\n"
    )
    csv_b = (
        "Unnamed: 0,msname,msinstanceid,nodeid,instance_cpu_usage,instance_memory_usage,timestamp\n"
        "0,ms_3,inst_4,node_3,0.7,0.8,120000\n"
        "1,ms_3,inst_5,node_3,0.9,0.1,150000\n"
    )
    archive = _write_tar_gz(
        tmp_path / "MSResource_0.tar.gz",
        {
            "MSResource/._MSResource_0.csv": b"appledouble junk",
            "__MACOSX/MSResource/._MSResource_0.csv": b"appledouble junk",
            "MSResource/README.txt": b"ignore non-csv",
            "MSResource/part_a.csv": csv_a.encode("utf-8"),
            "MSResource/part_b.csv.gz": _gzip_bytes(csv_b),
        },
    )

    out_dir = tmp_path / "bronze"
    res = ingest_local_file(
        archive,
        dataset="oss_msresource",
        output_dir=out_dir,
        manifest_path=tmp_path / "bronze_manifest.json",
        compression=codec,
        chunk_rows=2,
    )

    assert res.status == "completed"
    assert res.row_count == 5
    assert res.timestamp_min == 30000
    assert res.timestamp_max == 150000
    assert len(res.parquet_files) == 2

    meta = pq.ParquetFile(res.parquet_files[0]).metadata
    assert meta.row_group(0).column(0).compression.lower() == codec

    # Hive partition readback via PyArrow and DuckDB
    hive_table = pq.read_table(out_dir)
    assert hive_table.num_rows == 5

    glob_pat = (out_dir / "dataset=*" / "source_file_stem=*" / "*.parquet").as_posix()
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT dataset, source_file_stem, COUNT(*) FROM read_parquet('{glob_pat}', hive_partitioning=true) GROUP BY 1, 2"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("oss_msresource", "MSResource_0", 5)]


# ---------------------------------------------------------------------------
# 4. Idempotent rerun, --force overwrite, and modified input detection
# ---------------------------------------------------------------------------


def test_idempotent_rerun_and_force_overwrite(tmp_path: Path) -> None:
    src = _write_csv_gz(
        tmp_path / "partition_id=00007.csv.gz",
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "t1,1000,0,a,rpc,b,fn1,1.0\n"
        "t2,2000,0,a,rpc,b,fn2,2.0\n",
    )
    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "bronze_manifest.json"

    r1 = ingest_bronze(
        dataset="casper_mscallgraph",
        input_paths=[src],
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert r1["files_completed"] == 1
    assert r1["files_skipped"] == 0
    assert r1["rows_written"] == 2
    assert pq.read_table(out_dir).num_rows == 2

    # Second run -> skipped cleanly, zero duplicates
    r2 = ingest_bronze(
        dataset="casper_mscallgraph",
        input_paths=[src],
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert r2["files_completed"] == 0
    assert r2["files_skipped"] == 1
    assert r2["rows_written"] == 0
    assert r2["total_rows"] == 2
    assert pq.read_table(out_dir).num_rows == 2

    # --force overwrites partition atomically without duplicating rows
    r3 = ingest_bronze(
        dataset="casper_mscallgraph",
        input_paths=[src],
        output_dir=out_dir,
        manifest_path=manifest_path,
        force=True,
    )
    assert r3["files_completed"] == 1
    assert r3["rows_written"] == 2
    assert pq.read_table(out_dir).num_rows == 2

    # Modified source content -> checksum changes -> re-ingests updated rows
    _write_csv_gz(
        src,
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "t1,1000,0,a,rpc,b,fn1,1.0\n"
        "t2,2000,0,a,rpc,b,fn2,2.0\n"
        "t3,3000,0,a,rpc,b,fn3,3.0\n",
    )
    r4 = ingest_bronze(
        dataset="casper_mscallgraph",
        input_paths=[src],
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert r4["files_completed"] == 1
    assert r4["rows_written"] == 3
    assert pq.read_table(out_dir).num_rows == 3


# ---------------------------------------------------------------------------
# 5. Checksums, manifest provenance, and sample_manifest.json protection
# ---------------------------------------------------------------------------


def test_checksums_manifest_provenance_and_sample_manifest_guard(tmp_path: Path) -> None:
    src = tmp_path / "Node_0.csv"
    src.write_text(
        ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
        "0,30000,n1,0.11,0.22\n"
        "1,60000,n2,0.33,0.44\n",
        encoding="utf-8",
    )
    expected = compute_file_checksums(src)
    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "bronze_manifest.json"

    res = ingest_local_file(
        src,
        dataset="oss_node",
        output_dir=out_dir,
        manifest_path=manifest_path,
        expected_sha256=expected.sha256,
        expected_md5=expected.md5,
    )
    assert res.source_sha256 == expected.sha256
    assert res.source_md5 == expected.md5
    assert res.timestamp_min == 30000
    assert res.timestamp_max == 60000

    manifest = load_manifest(manifest_path)
    entry = manifest["entries"]["oss_node::Node_0"]
    assert entry["source_sha256"] == expected.sha256
    assert entry["source_md5"] == expected.md5
    assert entry["row_count"] == 2
    assert entry["timestamp_min"] == 30000
    assert entry["timestamp_max"] == 60000

    # Protected manifest guard
    protected = tmp_path / "sample_manifest.json"
    with pytest.raises(ValueError, match="protected manifest"):
        validate_manifest_path(protected)
    with pytest.raises(ValueError, match="protected manifest"):
        save_manifest_atomic(protected, manifest)
    with pytest.raises(ValueError, match="protected manifest"):
        ingest_local_file(src, dataset="oss_node", output_dir=out_dir, manifest_path=protected)

    # Checksum mismatch must fail before writing Parquet or manifest
    bad_out = tmp_path / "bad_bronze"
    bad_manifest = tmp_path / "bad_manifest.json"
    with pytest.raises(ChecksumMismatchError):
        ingest_local_file(
            src,
            dataset="oss_node",
            output_dir=bad_out,
            manifest_path=bad_manifest,
            expected_sha256="0" * 64,
        )
    assert not bad_out.exists()
    assert not bad_manifest.exists()


# ---------------------------------------------------------------------------
# 6. Resumable HTTP Range downloads (206 / 200 / 416) & exponential backoff
# ---------------------------------------------------------------------------


def test_resumable_range_download_206_200_416_and_backoff(tmp_path: Path) -> None:
    payload = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" * 50
    expected_sha256 = hashlib.sha256(payload).hexdigest()
    state: dict[str, Any] = {
        "range_reqs": 0,
        "backoff_reqs": 0,
        "active": 0,
        "max_active": 0,
    }
    lock = threading.Lock()

    class TestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            if self.path == "/resume206":
                with lock:
                    state["range_reqs"] += 1
                    attempt = state["range_reqs"]
                range_hdr = self.headers.get("Range")
                if attempt == 1 and range_hdr is None:
                    split_at = len(payload) // 2
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload[:split_at])
                    self.wfile.flush()
                    return
                if range_hdr and range_hdr.startswith("bytes="):
                    offset = int(range_hdr.split("=", 1)[1].rstrip("-"))
                    rem = payload[offset:]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {offset}-{len(payload) - 1}/{len(payload)}")
                    self.send_header("Content-Length", str(len(rem)))
                    self.end_headers()
                    self.wfile.write(rem)
                    return

            if self.path == "/ignore_range_200":
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/complete_416":
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(payload)}")
                self.end_headers()
                return

            if self.path == "/backoff_503":
                with lock:
                    state["backoff_reqs"] += 1
                    req_num = state["backoff_reqs"]
                if req_num <= 2:
                    self.send_response(503)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            self.send_response(404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), TestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"

    try:
        cfg = DownloadConfig(max_concurrency=2, retries=4, backoff_seconds=1.0, backoff_multiplier=2.0)

        # 206 partial resume after truncated first response
        sleeps_206: list[float] = []
        dest_206 = tmp_path / "a_206.bin"
        res_206 = download_file_resumable(
            f"{base}/resume206",
            dest_206,
            config=cfg,
            expected_sha256=expected_sha256,
            sleep_fn=sleeps_206.append,
        )
        assert res_206.resumed is True
        assert res_206.attempts == 2
        assert dest_206.read_bytes() == payload
        assert sleeps_206 == [1.0]

        # 200 OK when .part exists: must truncate and overwrite without corrupting
        dest_200 = tmp_path / "b_200.bin"
        Path(f"{dest_200}.part").write_bytes(b"STALE_PREFIX")
        res_200 = download_file_resumable(
            f"{base}/ignore_range_200",
            dest_200,
            config=cfg,
            expected_sha256=expected_sha256,
            sleep_fn=lambda _: None,
        )
        assert res_200.resumed is False
        assert dest_200.read_bytes() == payload

        # 416 Range Not Satisfiable when .part is already exact length
        dest_416 = tmp_path / "c_416.bin"
        Path(f"{dest_416}.part").write_bytes(payload)
        res_416 = download_file_resumable(
            f"{base}/complete_416",
            dest_416,
            config=cfg,
            expected_sha256=expected_sha256,
            sleep_fn=lambda _: None,
        )
        assert res_416.resumed is True
        assert dest_416.read_bytes() == payload

        # 503 exponential backoff: 1.0 * 2**0 = 1.0, 1.0 * 2**1 = 2.0
        sleeps_503: list[float] = []
        dest_503 = tmp_path / "d_503.bin"
        res_503 = download_file_resumable(
            f"{base}/backoff_503",
            dest_503,
            config=cfg,
            expected_sha256=expected_sha256,
            sleep_fn=sleeps_503.append,
        )
        assert res_503.attempts == 3
        assert sleeps_503 == [1.0, 2.0]
        assert dest_503.read_bytes() == payload
    finally:
        server.shutdown()
        server.server_close()


# ---------------------------------------------------------------------------
# 7. Low concurrency strictly bounded 1..2
# ---------------------------------------------------------------------------


def test_concurrency_bounded_1_to_2(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="concurrency"):
        DownloadConfig(max_concurrency=3)
    with pytest.raises(ValueError, match="concurrency"):
        DownloadConfig(max_concurrency=0)

    cfg = DownloadConfig(max_concurrency=2, retries=1, backoff_seconds=0.0)
    with pytest.raises(ValueError, match="concurrency"):
        download_batch([], config=cfg, concurrency=3)

    lock = threading.Lock()
    active = 0
    max_active = 0
    body = b"hello-bronze-concurrency"

    class ConcurrencyHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), ConcurrencyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"

    try:
        tasks = [
            DownloadTask(url=f"{base}/file_{i}.bin", dest_path=tmp_path / f"file_{i}.bin")
            for i in range(4)
        ]
        results = download_batch(tasks, config=cfg, concurrency=2)
        assert len(results) == 4
        assert 1 <= max_active <= 2
    finally:
        server.shutdown()
        server.server_close()


# ---------------------------------------------------------------------------
# 8. Temporary .tar.gz cleanup vs preserving user-supplied --input
# ---------------------------------------------------------------------------


def test_temp_tar_gz_cleanup_safety(tmp_path: Path) -> None:
    csv_bytes = (
        ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
        "0,30000,n1,0.5,0.6\n"
    ).encode("utf-8")
    archive_path = _write_tar_gz(tmp_path / "server_src" / "Node_0.tar.gz", {"Node_0.csv": csv_bytes})
    archive_bytes = archive_path.read_bytes()

    class ArchiveHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(archive_bytes)))
            self.end_headers()
            self.wfile.write(archive_bytes)

    server = ThreadingHTTPServer(("127.0.0.1", 0), ArchiveHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/Node_0.tar.gz"

    try:
        dl_dir = tmp_path / "dl_cache"
        out_dir = tmp_path / "bronze_remote"
        manifest_path = tmp_path / "manifest_remote.json"
        summary = ingest_bronze(
            dataset="oss_node",
            urls=[url],
            download_dir=dl_dir,
            output_dir=out_dir,
            manifest_path=manifest_path,
            cleanup_temp_archives=True,
        )
        assert summary["files_completed"] == 1
        assert summary["total_rows"] == 1
        # Temporary downloaded .tar.gz must be cleaned up after conversion
        assert not (dl_dir / "Node_0.tar.gz").exists()
        assert not (dl_dir / "0000" / "Node_0.tar.gz").exists()
        assert pq.read_table(out_dir).num_rows == 1
    finally:
        server.shutdown()
        server.server_close()

    # User-supplied local --input archive must NEVER be deleted
    user_input = _write_tar_gz(tmp_path / "user_data" / "Node_0.tar.gz", {"Node_0.csv": csv_bytes})
    summary_local = ingest_bronze(
        dataset="oss_node",
        input_paths=[user_input],
        output_dir=tmp_path / "bronze_local",
        manifest_path=tmp_path / "manifest_local.json",
        cleanup_temp_archives=True,
    )
    assert summary_local["files_completed"] == 1
    assert user_input.is_file()


# ---------------------------------------------------------------------------
# 9. S3 / MinIO path-style upload and credential validation
# ---------------------------------------------------------------------------


def test_s3_upload_path_style_and_missing_creds(tmp_path: Path) -> None:
    configs = load_project_configs(REPO_ROOT / "configs", environ={})
    lakehouse_no_creds = json.loads(json.dumps(configs["lakehouse"]))
    lakehouse_no_creds["minio"]["access_key"] = ""
    lakehouse_no_creds["minio"]["secret_key"] = ""

    with pytest.raises(S3UploadError, match="Missing MinIO/S3 credentials"):
        build_s3_client(lakehouse_no_creds)

    lakehouse_with_creds = json.loads(json.dumps(lakehouse_no_creds))
    lakehouse_with_creds["minio"]["access_key"] = "minioadmin"
    lakehouse_with_creds["minio"]["secret_key"] = "minioadmin123"
    client = build_s3_client(lakehouse_with_creds)
    assert client._client_config.s3["addressing_style"] == "path"

    class StubS3Client:
        def __init__(self) -> None:
            self.uploads: list[tuple[str, str, str]] = []

        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            self.uploads.append((filename, bucket, key))

    src = tmp_path / "MSResource_1.csv"
    src.write_text(
        ",msname,msinstanceid,nodeid,instance_cpu_usage,instance_memory_usage,timestamp\n"
        "0,ms_1,i1,n1,0.2,0.3,30000\n",
        encoding="utf-8",
    )
    stub = StubS3Client()
    manifest_path = tmp_path / "s3_manifest.json"
    res = ingest_local_file(
        src,
        dataset="oss_msresource",
        output_dir=tmp_path / "bronze",
        manifest_path=manifest_path,
        data_cfg=configs["data"],
        lakehouse_cfg=lakehouse_with_creds,
        upload_s3=True,
        s3_client=stub,
    )
    assert len(stub.uploads) == 1
    _, bucket, key = stub.uploads[0]
    assert bucket == "bronze"
    assert key == "ms_resource/dataset=oss_msresource/source_file_stem=MSResource_1/part-00000.parquet"
    assert res.s3_uris == (f"s3://{bucket}/{key}",)
    manifest = load_manifest(manifest_path)
    assert manifest["entries"]["oss_msresource::MSResource_1"]["s3_uris"] == [f"s3://{bucket}/{key}"]


# ---------------------------------------------------------------------------
# 10. CLI scripts/run_bronze.py (--help, --check-config, 2x idempotent run)
# ---------------------------------------------------------------------------


def test_cli_run_bronze_help_check_config_and_2x_idempotent(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    help_proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "run_bronze.py"), "--help"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert help_proc.returncode == 0
    assert "--dataset" in help_proc.stdout
    assert "--manifest-path" in help_proc.stdout

    cfg_proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "run_bronze.py"), "--check-config"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert cfg_proc.returncode == 0
    cfg_payload = json.loads(cfg_proc.stdout)
    assert cfg_payload["status"] == "config_valid"
    assert "casper_mscallgraph" in cfg_payload["datasets"]

    # Reject protected sample_manifest.json via CLI
    bad_manifest_proc = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_bronze.py"),
            "--check-config",
            "--manifest-path",
            "artifacts/manifest/sample_manifest.json",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad_manifest_proc.returncode != 0

    casper_fixture = _write_csv_gz(
        tmp_path / "partition_id=00042.csv.gz",
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "tr_a,1000,0,svc1,rpc,svc2,opA,5.0\n"
        "tr_b,2000,0.1,svc2,mq,(?),NAN,-2.0\n",
    )
    out_dir = tmp_path / "cli_bronze"
    manifest_path = tmp_path / "cli_manifest.json"
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "run_bronze.py"),
        "--dataset",
        "casper_mscallgraph",
        "--input",
        str(casper_fixture),
        "--output-dir",
        str(out_dir),
        "--manifest-path",
        str(manifest_path),
        "--compression",
        "zstd",
    ]

    run1 = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, check=False)
    assert run1.returncode == 0, run1.stderr
    p1 = json.loads(run1.stdout)
    assert p1["files_completed"] == 1
    assert p1["rows_written"] == 2

    run2 = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, check=False)
    assert run2.returncode == 0, run2.stderr
    p2 = json.loads(run2.stdout)
    assert p2["files_skipped"] == 1
    assert p2["rows_written"] == 0
    assert p2["total_rows"] == 2

    tbl = pq.read_table(out_dir)
    assert tbl.num_rows == 2
    assert not (REPO_ROOT / "artifacts" / "manifest" / "sample_manifest.json").exists()


# ---------------------------------------------------------------------------
# 11. Iteration 2 hardening: manifest path resolution, skip S3, URL isolation, magic bytes
# ---------------------------------------------------------------------------


def test_manifest_resolve_parquet_path_no_cwd_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cwd_dir = tmp_path / "cwd_bronze"
    other_out = tmp_path / "target_bronze"
    other_out.mkdir(parents=True, exist_ok=True)

    src = tmp_path / "Node_0.csv"
    src.write_text(
        ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
        "0,30000,n1,0.1,0.2\n",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "manifest.json"
    res = ingest_local_file(
        src,
        dataset="oss_node",
        output_dir=cwd_dir,
        manifest_path=manifest_path,
    )
    manifest = load_manifest(manifest_path)

    # Even when cwd_dir contains the relative Parquet path, should_skip_entry for
    # other_out (which lacks the file) must NOT fall back to CWD.
    monkeypatch.chdir(cwd_dir)
    assert (
        should_skip_entry(
            manifest,
            dataset="oss_node",
            source_file_stem="Node_0",
            sha256=res.source_sha256,
            md5=res.source_md5,
            output_dir=other_out,
        )
        is False
    )


def test_skip_branch_raises_when_upload_s3_missing_lakehouse_cfg(tmp_path: Path) -> None:
    src = tmp_path / "Node_0.csv"
    src.write_text(
        ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
        "0,30000,n1,0.1,0.2\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "manifest.json"

    first = ingest_local_file(
        src,
        dataset="oss_node",
        output_dir=out_dir,
        manifest_path=manifest_path,
        upload_s3=False,
    )
    assert first.status == "completed"

    with pytest.raises(BronzeError, match="lakehouse_cfg is required"):
        ingest_local_file(
            src,
            dataset="oss_node",
            output_dir=out_dir,
            manifest_path=manifest_path,
            upload_s3=True,
            lakehouse_cfg=None,
        )


def test_url_download_isolates_duplicate_basenames_under_concurrency_2(tmp_path: Path) -> None:
    payload_a = _gzip_bytes(
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "tr_a,1000,0,svc_a,rpc,svc_b,opA,1.0\n"
    )
    payload_b = _gzip_bytes(
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "tr_b,2000,0,svc_a,rpc,svc_b,opB,2.0\n"
        "tr_c,3000,0,svc_a,rpc,svc_b,opC,3.0\n"
    )

    class MirrorHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            if self.path == "/mirror1/shared.csv.gz":
                time.sleep(0.05)
                body = payload_a
            elif self.path == "/mirror2/shared.csv.gz":
                time.sleep(0.05)
                body = payload_b
            else:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), MirrorHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"

    try:
        dl_dir = tmp_path / "dl_iso"
        out_dir = tmp_path / "bronze"
        manifest_path = tmp_path / "manifest.json"
        summary = ingest_bronze(
            dataset="casper_mscallgraph",
            urls=[f"{base}/mirror1/shared.csv.gz", f"{base}/mirror2/shared.csv.gz"],
            download_dir=dl_dir,
            output_dir=out_dir,
            manifest_path=manifest_path,
            concurrency=2,
            cleanup_temp_archives=True,
        )
        assert summary["files_completed"] == 2
        hashes = {r["source_sha256"] for r in summary["results"]}
        assert hashes == {
            hashlib.sha256(payload_a).hexdigest(),
            hashlib.sha256(payload_b).hexdigest(),
        }
        assert not (dl_dir / "0000" / "shared.csv.gz").exists()
        assert not (dl_dir / "0001" / "shared.csv.gz").exists()
    finally:
        server.shutdown()
        server.server_close()


def test_extensionless_file_magic_byte_detection(tmp_path: Path) -> None:
    out_dir = tmp_path / "bronze"
    manifest_path = tmp_path / "manifest.json"

    # 1. Extensionless gzipped CSV (e.g., Dataverse /api/access/datafile/12345)
    gz_csv_no_ext = tmp_path / "12345"
    _write_csv_gz(
        gz_csv_no_ext,
        "traceid,timestamp,rpcid,um,rpctype,dm,interface,rt\n"
        "t_gz,1000,0,u1,rpc,d1,fn1,3.5\n",
    )
    res_gz = ingest_local_file(
        gz_csv_no_ext,
        dataset="casper_mscallgraph",
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert res_gz.status == "completed"
    assert res_gz.row_count == 1
    assert res_gz.source_file_stem == "12345"

    # 2. Extensionless tar.gz archive
    tar_no_ext = tmp_path / "67890"
    _write_tar_gz(
        tar_no_ext,
        {
            "Node/Node_0.csv": (
                ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
                "0,30000,node_x,0.4,0.6\n"
            ).encode("utf-8")
        },
    )
    res_tar = ingest_local_file(
        tar_no_ext,
        dataset="oss_node",
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert res_tar.status == "completed"
    assert res_tar.row_count == 1
    assert res_tar.source_file_stem == "67890"

    # 3. Extensionless plain CSV
    plain_no_ext = tmp_path / "99999"
    plain_no_ext.write_text(
        ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
        "0,60000,node_y,0.7,0.8\n",
        encoding="utf-8",
    )
    res_plain = ingest_local_file(
        plain_no_ext,
        dataset="oss_node",
        output_dir=out_dir,
        manifest_path=manifest_path,
    )
    assert res_plain.status == "completed"
    assert res_plain.row_count == 1
    assert res_plain.source_file_stem == "99999"
