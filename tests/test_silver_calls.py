"""Hand-calculated synthetic fixtures for Silver MSCallGraph normalization & dedup (T13, T15)."""

from ie212.silver.calls import CallProcessingConfig, process_silver_calls


def test_two_sided_rpc_pairing_and_rt_separation():
    rows = [
        {
            "": "0",
            "traceid": "t1",
            "timestamp": 60000,
            "rpcid": "0.1",
            "um": "svc_a",
            "rpctype": "rpc",
            "dm": "svc_b",
            "interface": "getUser",
            "rt": 15.0,
        },
        {
            "": "1",
            "traceid": "t1",
            "timestamp": 60050,
            "rpcid": "0.1",
            "um": "svc_a",
            "rpctype": "rpc",
            "dm": "svc_b",
            "interface": "getUser",
            "rt": -12.0,
        },
    ]
    res = process_silver_calls(rows, CallProcessingConfig(timestamp_tolerance_ms=1000))
    assert res.stats["raw_row_count"] == 2
    assert res.stats["canonical_call_count"] == 1
    assert res.stats["paired_two_sided_count"] == 1
    assert res.stats["ambiguous_row_count"] == 0

    call = res.canonical_calls[0]
    assert call["observation_side"] == "paired"
    assert call["caller_rt_ms"] == 15.0
    assert call["callee_rt_ms"] == 12.0
    assert call["exclude_from_duration_summary"] is False


def test_zero_rt_is_ambiguous_side_unless_paired_with_known_side():
    rows = [
        {
            "traceid": "t2",
            "timestamp": 60000,
            "rpcid": "0",
            "um": "gateway",
            "rpctype": "http",
            "dm": "auth",
            "interface": "/login",
            "rt": 0.0,
        },
        # Unpaired zero in another rpcid
        {
            "traceid": "t2",
            "timestamp": 60100,
            "rpcid": "0.1",
            "um": "auth",
            "rpctype": "rpc",
            "dm": "db_proxy",
            "interface": "check",
            "rt": 0.0,
        },
        # Paired caller + ambiguous_zero
        {
            "traceid": "t2",
            "timestamp": 60200,
            "rpcid": "0.2",
            "um": "auth",
            "rpctype": "rpc",
            "dm": "profile",
            "interface": "read",
            "rt": 3.0,
        },
        {
            "traceid": "t2",
            "timestamp": 60210,
            "rpcid": "0.2",
            "um": "auth",
            "rpctype": "rpc",
            "dm": "profile",
            "interface": "read",
            "rt": 0.0,
        },
    ]
    res = process_silver_calls(rows)
    by_rpc = {c["rpcid"]: c for c in res.canonical_calls}

    assert by_rpc["0"]["observation_side"] == "ambiguous_zero"
    assert by_rpc["0"]["is_zero_rt"] is True
    assert by_rpc["0"]["caller_rt_ms"] is None
    assert by_rpc["0"]["callee_rt_ms"] is None
    assert by_rpc["0"]["exclude_from_duration_summary"] is True

    assert by_rpc["0.2"]["observation_side"] == "paired"
    assert by_rpc["0.2"]["caller_rt_ms"] == 3.0
    assert by_rpc["0.2"]["callee_rt_ms"] == 0.0


def test_mq_negative_rt_flagged_invalid_not_callee_rpc():
    rows = [
        {
            "traceid": "t3",
            "timestamp": 120000,
            "rpcid": "0.1",
            "um": "producer",
            "rpctype": "mq",
            "dm": "broker",
            "interface": "topicA",
            "rt": -45.0,
        },
        {
            "traceid": "t3",
            "timestamp": 120100,
            "rpcid": "0.2",
            "um": "svc_x",
            "rpctype": "mc",
            "dm": "redis",
            "interface": "get",
            "rt": 0.0,
        },
    ]
    res = process_silver_calls(rows)
    by_rpc = {c["rpcid"]: c for c in res.canonical_calls}

    mq_call = by_rpc["0.1"]
    assert mq_call["observation_side"] == "single"
    assert mq_call["callee_rt_ms"] is None
    assert mq_call["is_invalid_duration"] is True
    assert mq_call["exclude_from_duration_summary"] is True

    mc_call = by_rpc["0.2"]
    assert mc_call["is_invalid_duration"] is False
    assert mc_call["other_rt_ms"] == 0.0
    assert mc_call["exclude_from_duration_summary"] is False


def test_multiplicity_gt_2_and_endpoint_conflict_routed_to_ambiguous():
    rows = [
        # Exact duplicate collapses cleanly
        {
            "traceid": "t4",
            "timestamp": 1000,
            "rpcid": "0",
            "um": "a",
            "rpctype": "mc",
            "dm": "b",
            "interface": "k",
            "rt": 2.0,
        },
        {
            "traceid": "t4",
            "timestamp": 1000,
            "rpcid": "0",
            "um": "a",
            "rpctype": "mc",
            "dm": "b",
            "interface": "k",
            "rt": 2.0,
        },
        # Multiplicity 3 after exact dedup -> ambiguous
        {
            "traceid": "t4",
            "timestamp": 2000,
            "rpcid": "0.1",
            "um": "b",
            "rpctype": "rpc",
            "dm": "c",
            "interface": "m",
            "rt": 5.0,
        },
        {
            "traceid": "t4",
            "timestamp": 2005,
            "rpcid": "0.1",
            "um": "b",
            "rpctype": "rpc",
            "dm": "c",
            "interface": "m",
            "rt": -4.0,
        },
        {
            "traceid": "t4",
            "timestamp": 2010,
            "rpcid": "0.1",
            "um": "b",
            "rpctype": "rpc",
            "dm": "c",
            "interface": "m",
            "rt": 7.0,
        },
        # Conflicting endpoint in two-sided pair -> ambiguous
        {
            "traceid": "t4",
            "timestamp": 3000,
            "rpcid": "0.2",
            "um": "b",
            "rpctype": "rpc",
            "dm": "d1",
            "interface": "fn",
            "rt": 6.0,
        },
        {
            "traceid": "t4",
            "timestamp": 3005,
            "rpcid": "0.2",
            "um": "b",
            "rpctype": "rpc",
            "dm": "d2",
            "interface": "fn",
            "rt": -5.0,
        },
    ]
    res = process_silver_calls(rows)
    assert res.stats["exact_duplicates_collapsed"] == 1
    assert res.stats["canonical_call_count"] == 1
    assert res.canonical_calls[0]["exact_duplicate_count"] == 2
    assert res.stats["ambiguous_group_count"] == 2
    assert res.stats["ambiguous_row_count"] == 5
    reasons = {r["ambiguity_reason"] for r in res.ambiguous_calls}
    assert "multiplicity_gt_2:3" in reasons
    assert "endpoint_conflict" in reasons


def test_sentinel_normalization_and_unambiguous_pair_repair():
    rows = [
        # Unpaired sentinel remains None with flag
        {
            "traceid": "t5",
            "timestamp": 1000,
            "rpcid": "0",
            "um": "(?)",
            "rpctype": "http",
            "dm": "svc_a",
            "interface": "",
            "rt": 10.0,
        },
        # Paired sentinel repaired unambiguously from matching side
        {
            "traceid": "t5",
            "timestamp": 2000,
            "rpcid": "0.1",
            "um": "svc_a",
            "rpctype": "rpc",
            "dm": "NAN",
            "interface": "op",
            "rt": 8.0,
        },
        {
            "traceid": "t5",
            "timestamp": 2010,
            "rpcid": "0.1",
            "um": "svc_a",
            "rpctype": "rpc",
            "dm": "svc_b",
            "interface": "op",
            "rt": -6.0,
        },
        # Self-loop
        {
            "traceid": "t5",
            "timestamp": 3000,
            "rpcid": "0.2",
            "um": "svc_a",
            "rpctype": "userDefined",
            "dm": "svc_a",
            "interface": "local",
            "rt": 1.0,
        },
    ]
    res = process_silver_calls(rows)
    by_rpc = {c["rpcid"]: c for c in res.canonical_calls}

    assert by_rpc["0"]["um"] is None
    assert by_rpc["0"]["um_is_sentinel"] is True
    assert by_rpc["0"]["has_sentinel_endpoint"] is True

    assert by_rpc["0.1"]["dm"] == "svc_b"
    assert by_rpc["0.1"]["repaired_from_pair"] is True
    assert by_rpc["0.1"]["repaired_fields"] == ["dm"]

    assert by_rpc["0.2"]["is_self_loop"] is True
