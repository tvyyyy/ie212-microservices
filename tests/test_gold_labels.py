"""Hand-calculated synthetic fixtures for Gold weighted RT labels (T19)."""

from ie212.gold.labels import compute_service_minute_labels


def test_weighted_mean_rt_and_strict_next_minute_target():
    # Minute 0 (ts=0): container c1 (MCR=10, RT=20), container c2 (MCR=30, RT=40)
    #   -> weighted mean = (10*20 + 30*40) / (10 + 30) = 1400 / 40 = 35.0
    # Minute 1 (ts=60000): container c1 (MCR=20, RT=50) -> mean = 50.0
    # Minute 2 (ts=120000): container c1 (MCR=0, RT=10) -> zero_denominator (None)
    # Minute 4 (ts=240000): container c1 (MCR=5, RT=15) -> gap after Minute 2!
    rows = [
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_MCR", "value": 10.0},
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 20.0},
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c2", "metric": "providerRPC_MCR", "value": 30.0},
        {"timestamp": 0, "msname": "svc_a", "msinstanceid": "c2", "metric": "providerRPC_RT", "value": 40.0},
        {"timestamp": 60000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_MCR", "value": 20.0},
        {"timestamp": 60000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 50.0},
        {"timestamp": 120000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_MCR", "value": 0.0},
        {"timestamp": 120000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 10.0},
        {"timestamp": 240000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_MCR", "value": 5.0},
        {"timestamp": 240000, "msname": "svc_a", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 15.0},
    ]
    labels = compute_service_minute_labels(rows)
    by_min = {r["forecast_origin_minute"]: r for r in labels}

    assert by_min[0]["mean_rt_ms"] == 35.0
    assert by_min[0]["has_valid_target"] is True
    assert by_min[0]["y_mean_rpc_rt_ms"] == 50.0

    # Target for minute 1 is minute 2 (zero_denominator) -> must NOT impute 0
    assert by_min[60000]["has_valid_target"] is False
    assert by_min[60000]["y_mean_rpc_rt_ms"] is None
    assert by_min[60000]["target_missing_reason"] == "target_zero_denominator"

    # Minute 2 has zero denominator itself, and next minute 3 (180000) is missing (gap to 240000)
    assert by_min[120000]["mean_rt_ms"] is None
    assert by_min[120000]["label_status"] == "zero_denominator"
    assert by_min[120000]["has_valid_target"] is False
    assert by_min[120000]["target_missing_reason"] == "missing_next_minute"

    # Minute 4 has no minute 5 -> missing_next_minute
    assert by_min[240000]["mean_rt_ms"] == 15.0
    assert by_min[240000]["has_valid_target"] is False


def test_incompatible_window_and_missing_pair_handling():
    rows = [
        # Incompatible 30s timestamp (30000 % 60000 != 0)
        {"timestamp": 30000, "msname": "svc_b", "msinstanceid": "c1", "metric": "providerRPC_MCR", "value": 5.0},
        {"timestamp": 30000, "msname": "svc_b", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 12.0},
        # Missing MCR for RT observation at ts=60000
        {"timestamp": 60000, "msname": "svc_b", "msinstanceid": "c1", "metric": "providerRPC_RT", "value": 12.0},
    ]
    labels = compute_service_minute_labels(rows)
    by_min = {r["forecast_origin_minute"]: r for r in labels}
    assert by_min[0]["label_status"] == "window_incompatible"
    assert by_min[0]["mean_rt_ms"] is None
    assert by_min[60000]["label_status"] == "missing_metric"
    assert by_min[60000]["mean_rt_ms"] is None
