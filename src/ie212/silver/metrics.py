"""T10 Spark transformations for resource and RT/QPS measurements.

Conflicting keys are quarantined, never averaged or arbitrarily selected.
Native-cadence tables remain separate from explicit minute projections.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, Window, functions as F

from ie212.quality.sample_profile import METRICS

SPECS = {
    "oss_node": {
        "table": "node", "keys": ["nodeid", "timestamp"], "ids": ["nodeid"],
        "values": ["node_cpu_usage", "node_memory_usage"], "cadence": 30000,
    },
    "oss_msresource": {
        "table": "ms_resource", "keys": ["msname", "msinstanceid", "timestamp"],
        "ids": ["msname", "msinstanceid", "nodeid"],
        "values": ["instance_cpu_usage", "instance_memory_usage"], "cadence": 30000,
    },
    "oss_msrtqps": {
        "table": "ms_rt_qps", "keys": ["msname", "msinstanceid", "timestamp", "metric"],
        "ids": ["msname", "msinstanceid"], "values": ["value"], "cadence": 60000,
    },
}


def normalize_metric_groups(frame: DataFrame, dataset: str) -> DataFrame:
    """Normalize native observations and label grouped quarantine reasons.

    Every collapsed duplicate retains the full set of source row pointers.
    Invalid values become explicit nulls with flags; invalid keys/times and
    conflicting payloads are excluded from the canonical tables.
    """
    spec = SPECS[dataset]
    fields = [*spec["ids"], "timestamp", *spec["values"]]
    if dataset == "oss_msrtqps":
        fields.append("metric")
    required = set(fields + ["source_file", "source_row_offset", "source_sha256"])
    absent = required - set(frame.columns)
    if absent:
        raise ValueError(f"Missing Bronze columns: {sorted(absent)}")
    df = frame.select(*[F.col(c) for c in fields], "source_file", "source_row_offset", "source_sha256")
    for c in fields:
        df = df.withColumn("bronze_" + c, F.col(c).cast("string"))
    for c in spec["ids"]:
        text = F.trim(F.col(c))
        df = df.withColumn(c, F.when(text.isin("", "NAN", "(?)", "NULL", "null"), F.lit(None)).otherwise(text))
    number = F.expr("try_cast(bronze_timestamp AS DOUBLE)")
    valid_ts = number.isNotNull() & ~F.isnan(number) & (F.abs(number) != float("inf")) & (number >= 0) & (number < 9223372036854775808.0) & (number == F.floor(number))
    df = df.withColumn("timestamp", F.when(valid_ts, F.expr("try_cast(bronze_timestamp AS BIGINT)")))
    df = df.withColumn("invalid_timestamp", F.col("timestamp").isNull())
    df = df.withColumn("off_cadence", (F.col("timestamp") % spec["cadence"] != 0))
    df = df.withColumn("missing_identifier", F.lit(False))
    for c in spec["ids"]:
        df = df.withColumn("missing_identifier", F.col("missing_identifier") | F.col(c).isNull())
    for c in spec["values"]:
        value = F.expr(f"try_cast(bronze_{c} AS DOUBLE)")
        valid = value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf")) & (value >= 0)
        df = df.withColumn(c + "_valid", valid)
        df = df.withColumn(c, F.when(valid, value))
        if c != "value":
            df = df.withColumn(c + "_above_one", value > 1)
    if dataset == "oss_msrtqps":
        df = df.withColumn("metric", F.trim(F.col("metric")))
        df = df.withColumn("unknown_metric", ~F.col("metric").isin(*METRICS) | F.col("metric").isNull())
        df = df.withColumn("http_metric_suspect", F.col("metric").isin("HTTP_MCR", "HTTP_RT"))
        df = df.withColumn("recording_cadence_ms", F.lit(60000).cast("long"))
        df = df.withColumn("window_boundaries_verified", F.lit(False))
    else:
        df = df.withColumn("unknown_metric", F.lit(False))
    df = df.withColumn("minute_bucket", F.floor(F.col("timestamp") / 60000).cast("long"))
    # Group all domain fields and flags, including Bronze string renderings, excluding lineage.
    domain = [c for c in df.columns if c not in ("source_file", "source_row_offset", "source_sha256")]
    df = df.groupBy(*domain).agg(
        F.count("*").alias("observation_count"),
        F.collect_list(F.struct("source_file", "source_row_offset", "source_sha256")).alias("source_lineage"),
    )
    df = df.withColumn("key_variant_count", F.count("*").over(Window.partitionBy(*spec["keys"])))
    df = df.withColumn("quarantine_reason", F.concat_ws(";",
        F.when(F.col("invalid_timestamp"), "invalid_timestamp"),
        F.when(F.col("off_cadence"), "off_cadence"),
        F.when(F.col("missing_identifier"), "missing_identifier"),
        F.when(F.col("unknown_metric"), "unknown_metric"),
        F.when(F.col("key_variant_count") > 1, "conflicting_key"),
    ))
    return df


def normalize_metrics(frame: DataFrame, dataset: str) -> tuple[DataFrame, DataFrame]:
    """Return canonical and quarantine frames; caller controls persistence."""
    grouped = normalize_metric_groups(frame, dataset)
    return grouped.filter(F.col("quarantine_reason") == ""), grouped.filter(F.col("quarantine_reason") != "")


def align_resource_minutes(frame: DataFrame, dataset: str) -> DataFrame:
    """Explicit 30s -> 60s projection, preserving incomplete/ambiguous windows."""
    if dataset not in ("oss_node", "oss_msresource"):
        raise ValueError("Minute resource alignment requires Node or MSResource")
    spec = SPECS[dataset]
    keys = ["nodeid"] if dataset == "oss_node" else ["msname", "msinstanceid"]
    grouped = frame.groupBy(*keys, "minute_bucket").agg(
        F.countDistinct("timestamp").alias("half_window_sample_count"),
        F.sum("observation_count").alias("observation_count"),
        F.flatten(F.collect_list("source_lineage")).alias("source_lineage"),
        *[expr for c in spec["values"] for expr in (F.avg(c).alias(c), F.count(c).alias(c + "_valid_samples"))],
        *([F.sort_array(F.collect_set("nodeid")).alias("nodeids_observed")] if dataset == "oss_msresource" else []),
    )
    grouped = grouped.withColumn("timestamp", F.col("minute_bucket") * 60000)
    grouped = grouped.withColumn("minute_ts", F.col("timestamp"))
    grouped = grouped.withColumn("is_partial_minute", F.col("half_window_sample_count") != 2)
    if dataset == "oss_msresource":
        grouped = grouped.withColumn("host_mapping_ambiguous", F.size("nodeids_observed") != 1)
        grouped = grouped.withColumn("nodeid", F.when(~F.col("host_mapping_ambiguous"), F.col("nodeids_observed")[0]))
    complete = ~F.col("is_partial_minute")
    for column in spec["values"]:
        complete = complete & (F.col(column + "_valid_samples") == 2)
    if dataset == "oss_msresource":
        complete = complete & ~F.col("host_mapping_ambiguous")
    grouped = grouped.withColumn("is_complete_measurement_window", complete)
    return grouped


def summarize_outputs(raw: DataFrame, canonical: DataFrame, quarantine: DataFrame) -> dict[str, Any]:
    def count(frame: DataFrame) -> dict[str, int]:
        row = frame.agg(F.count("*").alias("groups"), F.coalesce(F.sum("observation_count"), F.lit(0)).alias("source_rows")).first()
        return row.asDict()
    accepted, rejected = count(canonical), count(quarantine)
    total = raw.count()
    accounted = accepted["source_rows"] + rejected["source_rows"]
    if total != accounted:
        raise RuntimeError(f"Lineage row accounting failed: {total} != {accounted}")
    return {"input_rows": total, "canonical": accepted, "quarantine": rejected,
            "duplicate_observations_collapsed": total - accepted["groups"] - rejected["groups"],
            "row_accounting_passed": True}
