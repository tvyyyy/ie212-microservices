"""A failed replacement run must not leave a stale successful acceptance gate."""
import json
from types import SimpleNamespace
import pytest
from ie212.silver import pipeline


def test_run_report_is_running_before_first_catalog_mutation(tmp_path, monkeypatch):
    report = tmp_path / "report.json"
    report.write_text('{"status":"passed","run_id":"old"}')
    observed = []

    class FakeSpark:
        version = "synthetic"
        sparkContext = SimpleNamespace(setLogLevel=lambda level: None)
        def sql(self, statement):
            observed.append(json.loads(report.read_text())["status"])
            raise RuntimeError("synthetic catalog failure")
        def stop(self):
            observed.append("stopped")

    monkeypatch.setattr(pipeline, "resolve_lakehouse_settings", lambda *a, **k: SimpleNamespace(catalog_name="ie212"))
    monkeypatch.setattr(pipeline, "build_spark_iceberg_conf", lambda *a, **k: {})
    monkeypatch.setattr(pipeline, "_create_default_spark_session", lambda conf: FakeSpark())
    with pytest.raises(RuntimeError, match="synthetic catalog failure"):
        pipeline.run_silver_metrics({}, tmp_path, tmp_path / "outputs", report, [])
    assert observed == ["running", "stopped"]
    assert json.loads(report.read_text())["status"] == "failed"
