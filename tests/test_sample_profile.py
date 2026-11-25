"""Synthetic raw values must be measured before coercion and normalization."""
import gzip
from ie212.config import load_config
from ie212.quality.sample_profile import profile_samples


def test_raw_profile_counts_duplicates_conflicts_and_invalids(tmp_path):
    source = tmp_path / "node.csv.gz"
    with gzip.open(source, "wt", encoding="utf-8") as f:
        f.write(
            ",timestamp,nodeid,node_cpu_usage,node_memory_usage\n"
            "0,0,n,0.2,0.4\n1,0,n,0.2,0.4\n2,0,n,0.8,0.4\n"
            "3,30000,(?),abc,\n4,30001,n,1.2,0.4\n5,1.5,n,0.1,0.2\n"
        )
    result = profile_samples({"oss_node": [source]}, tmp_path / "report.json", tmp_path / "work", load_config("configs/data.yaml"))
    ds = result["datasets"]["oss_node"]
    assert ds["rows"] == 6
    assert ds["exact_duplicates"]["excess_rows"] == 1
    assert ds["candidate_key"]["conflicting_groups"] == 1
    assert ds["columns"]["nodeid"]["missing"] == 1
    assert ds["numeric"]["node_cpu_usage"]["invalid_nonmissing_rows"] == 1
    assert ds["time"]["invalid_rows"] == 1
    assert ds["time"]["off_cadence_rows"] == 1
    # A changed raw file must invalidate the optional disk staging cache.
    with gzip.open(source, "wt", encoding="utf-8") as f:
        f.write(",timestamp,nodeid,node_cpu_usage,node_memory_usage\n0,0,n,0.1,0.2\n")
    changed = profile_samples({"oss_node": [source]}, tmp_path / "report.json", tmp_path / "work", load_config("configs/data.yaml"), reuse_raw=True)
    assert changed["datasets"]["oss_node"]["rows"] == 1
