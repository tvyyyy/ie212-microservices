"""Bounded-memory, exact raw CSV profiling for T02/T06/T07.

Raw strings are retained before numeric coercion. Source indexes and file names
are excluded from domain duplicate definitions; no input rows are modified.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ie212.ingestion.schemas import normalize_csv_header, get_dataset_schema

METRICS = (
    "consumerRPC_MCR", "providerRPC_MCR", "HTTP_MCR", "providerMQ_MCR",
    "consumerMQ_MCR", "consumerRPC_RT", "providerRPC_RT", "HTTP_RT",
    "providerMQ_RT", "consumerMQ_RT",
)
TOKENS = ("", "NAN", "(?)", "NULL", "null")
KEYS = {
    "oss_node": ["nodeid", "timestamp"],
    "oss_msresource": ["msname", "msinstanceid", "timestamp"],
    "oss_msrtqps": ["msname", "msinstanceid", "timestamp", "metric"],
    "casper_mscallgraph": ["traceid", "rpcid"],
}


def literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def missing(column: str) -> str:
    return f"({ident(column)} IS NULL OR trim({ident(column)}) IN ({','.join(literal(t) for t in TOKENS)}))"


def records(conn: Any, sql: str) -> list[dict[str, Any]]:
    cur = conn.execute(sql)
    return [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]


def file_evidence(path: Path) -> dict[str, Any]:
    sha = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    return {"path": path.as_posix(), "bytes": path.stat().st_size,
            "sha256": sha.hexdigest(), "header": header}


def profile_samples(inputs: dict[str, list[Path]], output: Path,
                    workspace: Path, data_cfg: dict[str, Any], *, reuse_raw: bool = False) -> dict[str, Any]:
    previous = json.loads(output.read_text(encoding="utf-8")) if output.is_file() else {}
    output.parent.mkdir(parents=True, exist_ok=True)
    workspace.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(workspace / "raw_profile.duckdb"))
    conn.execute("SET memory_limit='1GB'")
    conn.execute("SET threads=2")
    conn.execute(f"SET temp_directory={literal(workspace / 'spill')}")
    conn.execute("SET preserve_insertion_order=false")
    report: dict[str, Any] = {
        "version": "t06_raw_v1", "status": "running", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": "t06_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "scope": "All rows of explicitly listed local development samples; not full dataset statistics",
        "resources": {"duckdb_memory_limit": "1GB", "threads": 2},
        "missing_tokens": list(TOKENS), "datasets": {},
    }
    try:
        for dataset, files in inputs.items():
            schema = get_dataset_schema(dataset, data_cfg)
            evidence = [file_evidence(p) for p in files]
            for item in evidence:
                normalize_csv_header(item["header"], schema)
            columns = list(schema.columns)
            has_index = evidence[0]["header"][0] == ""
            names = (["__source_index"] if has_index else []) + columns
            types = "{" + ",".join(literal(c) + ":'VARCHAR'" for c in names) + "}"
            not_null = "[" + ",".join(literal(c) for c in names) + "]"
            file_list = "[" + ",".join(literal(p) for p in files) + "]"
            domain = ",".join(ident(c) for c in columns)
            print(f"Profiling {dataset}: {len(files)} files", flush=True)
            can_reuse = (reuse_raw and previous.get("datasets", {}).get(dataset, {}).get("files") == evidence
                         and conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [dataset]).fetchone()[0] == 1)
            if not can_reuse:
                conn.execute(f"CREATE OR REPLACE TABLE {dataset} AS SELECT {domain}, filename AS source_file FROM read_csv({file_list}, header=true, columns={types}, auto_detect=false, delim=',', force_not_null={not_null}, filename=true)")
            total = conn.execute(f"SELECT count(*) FROM {dataset}").fetchone()[0]
            ds: dict[str, Any] = {"files": evidence, "rows": total, "columns": {}}
            expressions = []
            for c in columns:
                expressions.extend([
                    f"count(*) FILTER (WHERE {ident(c)} IS NULL) AS {ident(c + '_sql_null')}",
                    f"count(*) FILTER (WHERE trim({ident(c)})='') AS {ident(c + '_blank')}",
                    f"count(*) FILTER (WHERE {missing(c)}) AS {ident(c + '_missing')}",
                ])
            counts = records(conn, f"SELECT {','.join(expressions)} FROM {dataset}")[0]
            for c in columns:
                ds["columns"][c] = {k: counts[c + '_' + k] for k in ("sql_null", "blank", "missing")}
                if counts[c + "_missing"]:
                    ds["columns"][c]["sentinel_counts"] = records(conn, f"SELECT trim({ident(c)}) token,count(*) rows FROM {dataset} WHERE {missing(c)} GROUP BY 1 ORDER BY 1")
            ts = "try_cast(timestamp AS DOUBLE)"
            valid_ts = f"isfinite({ts}) AND {ts}>=0 AND {ts}=floor({ts}) AND {ts}<9223372036854775808"
            cadence = data_cfg["datasets"][dataset].get("cadence_seconds")
            ds["time"] = records(conn, f"SELECT min({ts}) FILTER (WHERE {valid_ts}) min_ms, max({ts}) FILTER (WHERE {valid_ts}) max_ms, count(*) FILTER (WHERE NOT coalesce(({valid_ts}),false)) invalid_rows" + (f", count(*) FILTER (WHERE ({valid_ts}) AND {ts}%{cadence * 1000}<>0) off_cadence_rows" if cadence else "") + f" FROM {dataset}")[0]
            if cadence:
                ds["time"]["cadence_ms"] = cadence * 1000
                ds["time"]["observed_residues_ms"] = records(conn, f"SELECT {ts}%60000 residue, count(*) rows FROM {dataset} WHERE {valid_ts} GROUP BY 1 ORDER BY 1")
            # Exact textual domain deduplication, excluding source index/lineage.
            conn.execute(f"CREATE OR REPLACE TABLE exact_groups AS SELECT {domain}, count(*) n FROM {dataset} GROUP BY ALL")
            unique = conn.execute("SELECT count(*) FROM exact_groups").fetchone()[0]
            keys = ",".join(ident(c) for c in KEYS[dataset])
            ds["exact_duplicates"] = {"distinct_domain_rows": unique, "excess_rows": total - unique}
            ds["candidate_key"] = {"columns": KEYS[dataset], **records(conn, f"SELECT count(*) groups, count(*) FILTER (WHERE n>1) duplicate_groups, coalesce(sum(n-1),0) excess_rows, count(*) FILTER (WHERE variants>1) conflicting_groups, max(n) maximum_multiplicity FROM (SELECT {keys}, sum(n) n, count(*) variants FROM exact_groups GROUP BY {keys})")[0]}
            if dataset == "casper_mscallgraph":
                ds["candidate_key"]["multiple_payload_groups"] = ds["candidate_key"].pop("conflicting_groups")
                ds["candidate_key"]["warning"] = "Occurrence grouping only; both observation sides and distinct occurrences may share traceid/rpcid. This is not a deduplication key."
                ds["rpc_types"] = records(conn, "SELECT rpctype,count(*) rows,count(*) FILTER (WHERE try_cast(rt AS DOUBLE)<0) negative_rt_rows,count(*) FILTER (WHERE try_cast(rt AS DOUBLE)=0) zero_rt_rows FROM casper_mscallgraph GROUP BY 1 ORDER BY 1")
                ds["topology"] = records(conn, f"SELECT count(DISTINCT traceid) FILTER (WHERE NOT {missing('traceid')}) traces, count(*) FILTER (WHERE NOT {missing('um')} AND NOT {missing('dm')} AND um=dm) known_self_loop_rows FROM casper_mscallgraph")[0]
                ds["topology"]["traces_across_sample_files"] = conn.execute(f"SELECT count(*) FROM (SELECT traceid FROM casper_mscallgraph WHERE NOT {missing('traceid')} GROUP BY traceid HAVING count(DISTINCT source_file)>1)").fetchone()[0]
                ds["topology"]["unresolved_endpoint_rows"] = conn.execute(f"SELECT count(*) FROM casper_mscallgraph WHERE {missing('um')} OR {missing('dm')}").fetchone()[0]
            elif dataset == "oss_msrtqps":
                ds["metric_counts"] = records(conn, "SELECT metric,count(*) rows FROM oss_msrtqps GROUP BY 1 ORDER BY 1")
                ds["unknown_metric_rows"] = conn.execute("SELECT count(*) FROM oss_msrtqps WHERE metric NOT IN (" + ",".join(literal(m) for m in METRICS) + ")").fetchone()[0]
                ds["http_equality"] = records(conn, "SELECT count(*) matched_keys, count(*) FILTER (WHERE mcr=rt) equal_values FROM (SELECT msname,msinstanceid,timestamp, max(try_cast(value AS DOUBLE)) FILTER (WHERE metric='HTTP_MCR') mcr,max(try_cast(value AS DOUBLE)) FILTER (WHERE metric='HTTP_RT') rt FROM oss_msrtqps WHERE metric IN ('HTTP_MCR','HTTP_RT') GROUP BY ALL) WHERE mcr IS NOT NULL AND rt IS NOT NULL")[0]
            numeric = [c for c in columns if c in ("rt", "value") or c.endswith("_usage")]
            ds["numeric"] = {}
            for c in numeric:
                val = f"try_cast({ident(c)} AS DOUBLE)"
                ds["numeric"][c] = records(conn, f"SELECT count(*) FILTER (WHERE NOT {missing(c)} AND NOT coalesce(isfinite({val}),false)) invalid_nonmissing_rows, count(*) FILTER (WHERE {val}<0) negative_rows, count(*) FILTER (WHERE {val}>1) above_one_rows, min({val}) FILTER (WHERE isfinite({val})) min_value, max({val}) FILTER (WHERE isfinite({val})) max_value FROM {dataset}")[0]
            report["datasets"][dataset] = ds
            output.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(f"Completed {dataset}: {total} rows", flush=True)
        if {"oss_msresource", "oss_msrtqps"} <= inputs.keys():
            report["service_coverage"] = records(conn, "WITH r AS (SELECT DISTINCT msname FROM oss_msresource), m AS (SELECT DISTINCT msname FROM oss_msrtqps) SELECT (SELECT count(*) FROM r) resource_services,(SELECT count(*) FROM m) metric_services,(SELECT count(*) FROM r JOIN m USING(msname)) matched_services")[0]
            report["container_minute_coverage"] = records(conn, "WITH r AS (SELECT DISTINCT msname,msinstanceid,floor(try_cast(timestamp AS DOUBLE)/60000) minute_key FROM oss_msresource), m AS (SELECT DISTINCT msname,msinstanceid,floor(try_cast(timestamp AS DOUBLE)/60000) minute_key FROM oss_msrtqps) SELECT count(*) metric_keys,count(*) FILTER (WHERE r.msname IS NOT NULL) matched_keys FROM m LEFT JOIN r USING(msname,msinstanceid,minute_key)")[0]
        if {"casper_mscallgraph", "oss_msrtqps"} <= inputs.keys():
            report["call_service_coverage"] = records(conn, f"WITH c AS (SELECT um service FROM casper_mscallgraph WHERE NOT {missing('um')} UNION SELECT dm FROM casper_mscallgraph WHERE NOT {missing('dm')}), m AS (SELECT DISTINCT msname service FROM oss_msrtqps) SELECT (SELECT count(*) FROM c) call_services,(SELECT count(*) FROM m) metric_services,(SELECT count(*) FROM c JOIN m USING(service)) matched_services")[0]
        report["status"] = "passed"
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        conn.close()
