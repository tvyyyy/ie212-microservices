"""Hand-calculated synthetic fixtures for trace validation and Gold trace_stats/edges (T14, T15, T17, T18)."""

from ie212.gold.service_edges import build_service_edges, project_service_graph
from ie212.gold.trace_stats import build_comm_type_by_depth, build_trace_stats
from ie212.quality.reports import build_data_quality_v1_report
from ie212.silver.calls import process_silver_calls
from ie212.silver.traces import validate_traces


def test_trace_validation_missing_parent_and_endpoint_mismatch():
    raw_calls = [
        # Trace A: clean complete trace (depth 2, 2 calls, 3 distinct services)
        {
            "traceid": "trace_clean",
            "timestamp": 1000,
            "rpcid": "0",
            "um": "gateway",
            "rpctype": "http",
            "dm": "api",
            "interface": "/v1",
            "rt": 20.0,
        },
        {
            "traceid": "trace_clean",
            "timestamp": 1005,
            "rpcid": "0.1",
            "um": "api",
            "rpctype": "rpc",
            "dm": "orders",
            "interface": "create",
            "rt": 10.0,
        },
        # Trace B: missing parent (has 0 and 0.1.1 without 0.1)
        {
            "traceid": "trace_missing_parent",
            "timestamp": 2000,
            "rpcid": "0",
            "um": "gateway",
            "rpctype": "http",
            "dm": "api",
            "interface": "/v1",
            "rt": 15.0,
        },
        {
            "traceid": "trace_missing_parent",
            "timestamp": 2005,
            "rpcid": "0.1.1",
            "um": "orders",
            "rpctype": "db",
            "dm": "mysql",
            "interface": "select",
            "rt": 4.0,
        },
        # Trace C: parent-child endpoint conflict (0 dm=api, 0.1 um=other)
        {
            "traceid": "trace_conflict",
            "timestamp": 3000,
            "rpcid": "0",
            "um": "gateway",
            "rpctype": "http",
            "dm": "api",
            "interface": "/v1",
            "rt": 18.0,
        },
        {
            "traceid": "trace_conflict",
            "timestamp": 3005,
            "rpcid": "0.1",
            "um": "unexpected_caller",
            "rpctype": "rpc",
            "dm": "billing",
            "interface": "charge",
            "rt": 7.0,
        },
    ]

    silver = process_silver_calls(raw_calls)
    tv = validate_traces(silver.canonical_calls, silver.ambiguous_calls)

    summaries = {s["traceid"]: s for s in tv.trace_summaries}
    assert summaries["trace_clean"]["is_clean_trace"] is True
    assert summaries["trace_clean"]["is_structurally_complete"] is True

    assert summaries["trace_missing_parent"]["missing_parent_count"] == 1
    assert summaries["trace_missing_parent"]["is_structurally_complete"] is False
    assert "missing_parent" in summaries["trace_missing_parent"]["quality_reasons"]

    assert summaries["trace_conflict"]["endpoint_mismatch_count"] == 1
    assert summaries["trace_conflict"]["is_clean_trace"] is False
    assert "parent_child_endpoint_mismatch" in summaries["trace_conflict"]["quality_reasons"]

    # T17: Gold trace_stats separates call_count and unique_service_count
    stats_rows = build_trace_stats(tv.annotated_calls, tv.trace_summaries)
    by_trace = {r["traceid"]: r for r in stats_rows}
    assert by_trace["trace_clean"]["call_count"] == 2
    assert by_trace["trace_clean"]["unique_service_count"] == 3
    assert by_trace["trace_clean"]["max_depth"] == 2

    comm_depth = build_comm_type_by_depth(tv.annotated_calls)
    depth1 = [r for r in comm_depth if r["call_depth"] == 1]
    assert depth1[0]["rpctype"] == "http"
    assert depth1[0]["call_count"] == 3
    assert depth1[0]["total_calls_at_depth"] == 3
    assert depth1[0]["share"] == 1.0

    # T18: service_edges and undirected projection
    edges = build_service_edges(silver.canonical_calls)
    assert any(
        e["caller_service"] == "gateway"
        and e["callee_service"] == "api"
        and e["observed_call_count"] == 3
        for e in edges
    )
    undirected = project_service_graph(edges, directed=False, exclude_self_loops=True)
    assert all(e["directed"] is False for e in undirected)

    # T16: quality report structure
    dq = build_data_quality_v1_report(
        silver.stats,
        tv.stats,
        service_call_set={"gateway", "api", "orders"},
        service_metric_set={"api", "orders", "auth"},
        scope_note="synthetic_test_only",
    )
    assert dq["services"]["overlapping_services"] == 2
    assert dq["traces"]["total_trace_count"] == 3
