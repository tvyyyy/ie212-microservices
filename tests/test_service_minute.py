"""Hand-calculated synthetic fixtures for 30s->60s alignment and service-minute index (T20)."""

from ie212.gold.service_minute import align_30s_to_60s, build_service_minute_index


def test_30s_to_60s_alignment_never_mixes_next_minute_half_window():
    # Minute 0 owns 0 and 30000; Minute 1 owns 60000 and 90000.
    node_rows = [
        {"timestamp": 0, "nodeid": "n1", "node_cpu_usage": 0.2, "node_memory_usage": 0.4},
        {"timestamp": 30000, "nodeid": "n1", "node_cpu_usage": 0.4, "node_memory_usage": 0.6},
        {"timestamp": 60000, "nodeid": "n1", "node_cpu_usage": 0.8, "node_memory_usage": 0.5},
        # Intentionally omit 90000 -> minute 60000 should be partial (count=1)
    ]
    aligned_nodes, n_stats = align_30s_to_60s(
        node_rows,
        entity_keys=("nodeid",),
        value_columns=("node_cpu_usage", "node_memory_usage"),
    )
    assert n_stats["is_unique_key"] is True
    assert n_stats["partial_minute_count"] == 1
    by_min = {r["minute_ts"]: r for r in aligned_nodes}
    assert abs(by_min[0]["node_cpu_usage"] - 0.3) < 1e-9
    assert by_min[0]["is_partial_minute"] is False
    assert abs(by_min[60000]["node_cpu_usage"] - 0.8) < 1e-9
    assert by_min[60000]["is_partial_minute"] is True


def test_service_minute_complete_grid_and_no_row_multiplication():
    res_rows = [
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c1", "nodeid": "n1", "instance_cpu_usage": 0.2, "instance_memory_usage": 0.3},
        {"timestamp": 30000, "msname": "svc_a", "msinstanceid": "c1", "nodeid": "n1", "instance_cpu_usage": 0.4, "instance_memory_usage": 0.5},
        # Second container on a different host in minute 0
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c2", "nodeid": "n2", "instance_cpu_usage": 0.6, "instance_memory_usage": 0.7},
        {"timestamp": 30000, "msname": "svc_a", "msinstanceid": "c2", "nodeid": "n2", "instance_cpu_usage": 0.6, "instance_memory_usage": 0.7},
    ]
    aligned_res, _ = align_30s_to_60s(
        res_rows,
        entity_keys=("msname", "msinstanceid"),
        value_columns=("instance_cpu_usage", "instance_memory_usage"),
        passthrough_keys=("nodeid",),
    )
    node_minute = [
        {"nodeid": "n1", "minute_ts": 0, "node_cpu_usage": 0.5, "node_memory_usage": 0.5},
        {"nodeid": "n2", "minute_ts": 0, "node_cpu_usage": 0.7, "node_memory_usage": 0.6},
    ]

    grid, stats = build_service_minute_index(
        services=["svc_a", "svc_b"],
        min_minute_ts=0,
        max_minute_ts=120000,
        resource_minute_rows=aligned_res,
        node_minute_rows=node_minute,
    )
    # 2 services * 3 minutes (0, 60000, 120000) = 6 rows, zero fan-out despite 2 containers & 2 hosts
    assert stats["expected_grid_rows"] == 6
    assert stats["actual_grid_rows"] == 6
    assert stats["is_unique_service_minute"] is True
    assert stats["row_multiplication_factor"] == 1.0

    row_a0 = next(r for r in grid if r["service_id"] == "svc_a" and r["minute_ts"] == 0)
    assert row_a0["container_count"] == 2
    assert row_a0["host_count"] == 2
    assert abs(row_a0["mean_host_cpu_usage"] - 0.6) < 1e-9
