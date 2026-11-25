"""Synthetic fixtures only: key conflicts, boundaries, lineage, missing values."""
import importlib.util
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from ie212.silver.metrics import normalize_metrics, align_resource_minutes, summarize_outputs


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("t10-fixtures")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def node_frame(spark, rows):
    return spark.createDataFrame(
        [(str(ts), node, cpu, memory, "fixture.csv", i, "synthetic-sha")
         for i, (ts, node, cpu, memory) in enumerate(rows)],
        "timestamp string,nodeid string,node_cpu_usage string,node_memory_usage string,source_file string,source_row_offset long,source_sha256 string",
    )


def test_duplicates_conflicting_keys_and_full_lineage(spark):
    raw = node_frame(spark, [(0,"n","0.2","0.4"),(0,"n","0.2","0.4"),
                             (30000,"n","0.3","0.4"),(30000,"n","0.8","0.4")])
    good, bad = normalize_metrics(raw, "oss_node")
    assert good.count() == 1
    accepted = good.first()
    assert accepted.observation_count == 2
    assert sorted(x.source_row_offset for x in accepted.source_lineage) == [0, 1]
    assert bad.count() == 2
    assert all("conflicting_key" in r.quarantine_reason for r in bad.collect())
    assert summarize_outputs(raw, good, bad)["row_accounting_passed"]


def test_minute_boundary_and_partial_invalid_samples(spark):
    raw = node_frame(spark, [(0,"n","0.2","0.4"),(0,"n","0.2","0.4"),(30000,"n","0.4","0.6"),
                             (60000,"n","inf","0.8"),(90000,"n","1.2","0.8"),
                             (120000,"n","0.9","0.8"),(120001,"n","0.9","0.8")])
    good, bad = normalize_metrics(raw, "oss_node")
    assert bad.count() == 1
    minutes = {r.minute_ts: r for r in align_resource_minutes(good, "oss_node").collect()}
    assert minutes[0].node_cpu_usage == pytest.approx(0.3)
    assert minutes[60000].node_cpu_usage == 1.2  # do not clamp utilization
    assert minutes[60000].node_cpu_usage_valid_samples == 1
    assert minutes[60000].is_complete_measurement_window is False
    assert minutes[0].is_complete_measurement_window is True
    assert minutes[120000].is_partial_minute is True
    assert minutes[120000].half_window_sample_count == 1


def test_resource_host_change_does_not_multiply_minutes(spark):
    frame = spark.createDataFrame([
        ("s","i","n1",0,"0.1","0.2","f",0,"synthetic"),
        ("s","i","n2",30000,"0.3","0.4","f",1,"synthetic")],
        "msname string,msinstanceid string,nodeid string,timestamp long,instance_cpu_usage string,instance_memory_usage string,source_file string,source_row_offset long,source_sha256 string")
    good, bad = normalize_metrics(frame, "oss_msresource")
    assert bad.count() == 0
    minute = align_resource_minutes(good, "oss_msresource").collect()
    assert len(minute) == 1
    assert minute[0].host_mapping_ambiguous is True
    assert minute[0].nodeid is None
    assert minute[0].nodeids_observed == ["n1", "n2"]


def test_rt_zero_missing_negative_and_http_policy(spark):
    frame = spark.createDataFrame([
        ("s","i",0,"providerRPC_MCR","0","f",0,"synthetic"),
        ("s","i",0,"providerRPC_RT","","f",1,"synthetic"),
        ("s","i",60000,"providerRPC_RT","-1","f",2,"synthetic"),
        ("s","i",0,"HTTP_RT","5","f",3,"synthetic"),
        ("s","i",0,"unknown","5","f",4,"synthetic")],
        "msname string,msinstanceid string,timestamp long,metric string,value string,source_file string,source_row_offset long,source_sha256 string")
    good, bad = normalize_metrics(frame, "oss_msrtqps")
    rows = {(r.timestamp,r.metric):r for r in good.collect()}
    assert rows[0,"providerRPC_MCR"].value == 0
    assert rows[0,"providerRPC_RT"].value is None
    assert rows[60000,"providerRPC_RT"].value is None
    assert rows[0,"HTTP_RT"].http_metric_suspect is True
    assert bad.count() == 1


def test_silver_cli_validates_without_running_pipeline(capsys):
    p = Path(__file__).parents[1] / "scripts/run_silver.py"
    spec = importlib.util.spec_from_file_location("silver_cli", p)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main(["--check-config"]) == 0
    assert '"status": "validated"' in capsys.readouterr().out
