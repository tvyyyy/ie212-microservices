"""Run T10 native-cadence and aligned Silver measurements on Iceberg."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from pyspark import StorageLevel
from pyspark.sql import functions as F
from ie212.smoke_test import (
    _create_default_spark_session, build_spark_iceberg_conf,
    resolve_lakehouse_settings, validate_sql_identifier,
)
from ie212.silver.metrics import SPECS, normalize_metric_groups, align_resource_minutes, summarize_outputs


def run_silver_metrics(config: dict[str, Any], bronze_root: Path, output: Path,
                       report_path: Path, datasets: list[str], namespace: str = "silver",
                       threads: int = 2, target: str = "container") -> dict[str, Any]:
    namespace = validate_sql_identifier(namespace)
    settings = resolve_lakehouse_settings(config, target=target, require_credentials=True)
    conf = build_spark_iceberg_conf(settings)
    conf.update({"spark.app.name": "ie212-t10-silver", "spark.master": f"local[{threads}]",
                 "spark.sql.shuffle.partitions": "8", "spark.sql.adaptive.coalescePartitions.enabled": "false"})
    spark = _create_default_spark_session(conf)
    spark.sparkContext.setLogLevel("WARN")
    report: dict[str, Any] = {"stage": "silver_metrics", "task": "T10", "status": "running",
        "run_id": "t10_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "spark_version": spark.version, "threads": threads,
        "namespace": namespace, "datasets": {}, "scope": "Explicit input Bronze partitions only",
        "timestamp_unit": "ms relative to trace window", "minute_rule": "floor(timestamp/60000); no cross-minute averaging"}
    output.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.catalog_name}.{namespace}")
        for dataset in datasets:
            path = bronze_root / f"dataset={dataset}"
            if not path.is_dir():
                raise FileNotFoundError(f"Missing Bronze input: {path}")
            raw = spark.read.parquet(str(path)).persist(StorageLevel.DISK_ONLY)
            grouped = normalize_metric_groups(raw, dataset).persist(StorageLevel.DISK_ONLY)
            canonical = grouped.filter(F.col("quarantine_reason") == "")
            quarantine = grouped.filter(F.col("quarantine_reason") != "")
            try:
                stats = summarize_outputs(raw, canonical, quarantine)
                stats["quarantine_reasons"] = [r.asDict() for r in quarantine.groupBy("quarantine_reason").agg(F.sum("observation_count").alias("source_rows")).collect()]
                stats["input"] = path.as_posix()
                stats["sources"] = [r.asDict() for r in raw.select("source_file", "source_sha256").distinct().collect()]
                stats["tables"] = []
                outputs = [(SPECS[dataset]["table"], canonical), (SPECS[dataset]["table"] + "_quarantine", quarantine)]
                if dataset != "oss_msrtqps":
                    outputs.append((SPECS[dataset]["table"] + "_minute", align_resource_minutes(canonical, dataset).persist(StorageLevel.DISK_ONLY)))
                for name, frame in outputs:
                    full_name = f"{settings.catalog_name}.{namespace}.{name}"
                    (frame.writeTo(full_name).using("iceberg")
                     .tableProperty("ie212.run_id", report["run_id"])
                     .tableProperty("ie212.task", "T10")
                     .partitionedBy("minute_bucket").createOrReplace())
                    readback = spark.table(full_name)
                    expected_rows, actual_rows = frame.count(), readback.count()
                    if expected_rows != actual_rows:
                        raise RuntimeError(f"Iceberg readback row mismatch for {full_name}")
                    readback.write.mode("overwrite").parquet(str(output / name), compression="zstd")
                    stats["tables"].append({"table": full_name, "rows": actual_rows, "iceberg_readback_passed": True})
                    if name.endswith("_minute"):
                        stats["minute_windows"] = readback.agg(
                            F.sum(F.col("is_partial_minute").cast("long")).alias("partial_windows"),
                            F.sum((~F.col("is_complete_measurement_window")).cast("long")).alias("incomplete_measurement_windows"),
                            F.count("*").alias("total_windows"),
                        ).first().asDict()
                        if dataset == "oss_msresource":
                            stats["minute_windows"]["ambiguous_host_windows"] = readback.filter("host_mapping_ambiguous").count()
                        frame.unpersist()
                report["datasets"][dataset] = stats
                report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
                print(f"Verified Silver {dataset}: {stats}", flush=True)
            finally:
                raw.unpersist()
                grouped.unpersist()
        if set(datasets) == set(SPECS):
            resource = spark.table(f"{settings.catalog_name}.{namespace}.ms_resource_minute")
            node = spark.table(f"{settings.catalog_name}.{namespace}.node_minute")
            metrics = spark.table(f"{settings.catalog_name}.{namespace}.ms_rt_qps")
            keys = ["msname", "msinstanceid", "minute_bucket"]
            left = metrics.select(*keys).distinct()
            joined = left.join(resource.select(*keys, F.lit(True).alias("resource_matched")), keys, "left")
            left_rows, joined_rows = left.count(), joined.count()
            if left_rows != joined_rows:
                raise RuntimeError("Metric-resource join multiplied container-minute keys")
            resource_rows = resource.count()
            host_join = resource.join(node.select("nodeid", "minute_bucket", F.lit(True).alias("node_matched")), ["nodeid", "minute_bucket"], "left")
            if resource_rows != host_join.count():
                raise RuntimeError("Resource-host join multiplied rows")
            report["join_validation"] = {
                "metric_container_minutes": left_rows, "metric_resource_left_join_rows": joined_rows,
                "resource_matched_keys": joined.filter("resource_matched").count(),
                "resource_minutes": resource_rows, "resource_host_left_join_rows": resource_rows,
                "host_matched_rows": host_join.filter("node_matched").count(),
                "row_multiplication_factor": 1.0,
            }
        report["status"] = "passed"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    except Exception:
        report["status"] = "failed"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise
    finally:
        spark.stop()
