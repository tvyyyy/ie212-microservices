"""Independent DuckDB verification of exported T10 snapshots and rerun counts."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import duckdb
from ie212.quality.sample_profile import literal, ident, records


def verify(report_path: Path, snapshots: Path, first_run: Path | None = None):
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report["status"] != "passed":
        raise ValueError("Silver must finish successfully before independent verification")
    connection = duckdb.connect()
    connection.execute("SET memory_limit='1GB'")
    connection.execute("SET threads=2")
    spill = snapshots.parent / "verify-spill"
    spill.mkdir(parents=True, exist_ok=True)
    connection.execute(f"SET temp_directory={literal(spill)}")
    checks = {}
    try:
        for dataset, stats in report["datasets"].items():
            name = {"oss_node":"node", "oss_msresource":"ms_resource", "oss_msrtqps":"ms_rt_qps"}[dataset]
            entities = ["nodeid"] if dataset == "oss_node" else ["msname", "msinstanceid"]
            keys = entities + ["timestamp"] + (["metric"] if dataset == "oss_msrtqps" else [])
            connection.execute(f"CREATE OR REPLACE VIEW native AS SELECT * FROM read_parquet({literal(snapshots / name / '**/*.parquet')},hive_partitioning=false)")
            connection.execute(f"CREATE OR REPLACE VIEW rejected AS SELECT * FROM read_parquet({literal(snapshots / (name + '_quarantine') / '**/*.parquet')},hive_partitioning=false)")
            key_sql = ",".join(ident(k) for k in keys)
            duplicates = connection.execute(f"SELECT count(*) FROM (SELECT {key_sql} FROM native GROUP BY {key_sql} HAVING count(*)>1)").fetchone()[0]
            if duplicates:
                raise AssertionError(f"Duplicate canonical keys in {name}")
            pointers = records(connection, "SELECT ptr.source_file file,ptr.source_sha256 sha256,count(*) pointers,min(ptr.source_row_offset) min_offset,max(ptr.source_row_offset) max_offset,count(DISTINCT ptr.source_row_offset) unique_offsets FROM (SELECT source_lineage FROM native UNION ALL SELECT source_lineage FROM rejected),unnest(source_lineage) t(ptr) GROUP BY 1,2")
            # Each accepted development dataset contains exactly one source file.
            # Distinct integer offsets covering 0..N-1 prove no pointer was lost or reused.
            if len(stats["sources"]) != 1 or len(pointers) != 1:
                raise ValueError("This acceptance verifier requires one input file per dataset")
            ptr, source = pointers[0], stats["sources"][0]
            expected = stats["input_rows"]
            assert ptr["file"] == source["source_file"] and ptr["sha256"] == source["source_sha256"]
            assert ptr["pointers"] == ptr["unique_offsets"] == expected
            assert ptr["min_offset"] == 0 and ptr["max_offset"] == expected - 1
            result = {"canonical_duplicate_keys": duplicates, "source_pointer_coverage": pointers,
                      "all_source_offsets_covered_once": True}
            if dataset != "oss_msrtqps":
                minute_name = name + "_minute"
                connection.execute(f"CREATE OR REPLACE VIEW minute_output AS SELECT * FROM read_parquet({literal(snapshots / minute_name / '**/*.parquet')},hive_partitioning=false)")
                entity_sql = ",".join(ident(c) for c in entities)
                duplicate_minutes = connection.execute(f"SELECT count(*) FROM (SELECT {entity_sql},minute_bucket FROM minute_output GROUP BY {entity_sql},minute_bucket HAVING count(*)>1)").fetchone()[0]
                assert duplicate_minutes == 0
                values = ["node_cpu_usage","node_memory_usage"] if dataset == "oss_node" else ["instance_cpu_usage","instance_memory_usage"]
                averages = ",".join(f"avg({ident(c)}) {ident(c)}" for c in values)
                connection.execute(f"CREATE OR REPLACE VIEW expected_minutes AS SELECT {entity_sql},floor(timestamp/60000)::BIGINT expected_bucket,{averages} FROM native GROUP BY {entity_sql},expected_bucket")
                on = " AND ".join(f"e.{ident(c)}=m.{ident(c)}" for c in entities) + " AND e.expected_bucket=m.minute_bucket"
                mismatch = " OR ".join(f"(e.{ident(c)} IS NULL)<>(m.{ident(c)} IS NULL) OR abs(e.{ident(c)}-m.{ident(c)})>1e-12" for c in values)
                sql = f"SELECT count(*) FROM expected_minutes e FULL OUTER JOIN minute_output m ON {on} WHERE e.expected_bucket IS NULL OR m.minute_bucket IS NULL OR {mismatch}"
                differences = connection.execute(sql).fetchone()[0]
                assert differences == 0
                result.update({"minute_duplicate_keys": duplicate_minutes, "independent_minute_average_mismatches": differences,
                               "average_absolute_tolerance": 1e-12, "average_check_sql": sql})
            checks[dataset] = result
        report["independent_snapshot_verification"] = {"engine": "DuckDB " + duckdb.__version__, "datasets": checks, "status": "passed"}
        if first_run:
            first = json.loads(first_run.read_text(encoding="utf-8"))
            assert first["status"] == "passed"
            def table_counts(r):
                return {t["table"]:t["rows"] for d in r["datasets"].values() for t in d["tables"]}
            assert table_counts(first) == table_counts(report)
            assert first["join_validation"] == report["join_validation"]
            report["rerun_comparison"] = {"iceberg_row_counts_unchanged": True, "join_diagnostics_unchanged": True,
                                          "compared_table_counts": table_counts(report), "no_row_append_observed": True}
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"independent_verification": "passed", "datasets": list(checks), "rerun": report.get("rerun_comparison")},indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report", default="reports/data_quality/t10_silver_acceptance.json")
    p.add_argument("--snapshots", default="artifacts/t10/silver")
    p.add_argument("--first-run", default=None)
    args = p.parse_args()
    verify(Path(args.report), Path(args.snapshots), Path(args.first_run) if args.first_run else None)
