"""Gold trace statistics and communication breakdown by call depth (T17).

Keeps trace `call_count` (number of call occurrences) and `unique_service_count`
(distinct resolved services) strictly separate per agents_ie212.md §6.1, and reports
explicit denominators for communication-type shares at every depth tier.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


def build_trace_stats(
    annotated_calls: Iterable[Mapping[str, Any]],
    trace_summaries: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate per-trace size, unique service count, depth, and quality flags."""
    calls_by_trace: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for call in annotated_calls:
        calls_by_trace[str(call["traceid"])].append(call)

    summary_map: dict[str, Mapping[str, Any]] = {
        str(s["traceid"]): s for s in (trace_summaries or ())
    }
    all_traceids = sorted(set(calls_by_trace.keys()) | set(summary_map.keys()))

    rows: list[dict[str, Any]] = []
    for traceid in all_traceids:
        t_calls = calls_by_trace.get(traceid, [])
        services: set[str] = set()
        depths: list[int] = []
        self_loop_count = 0

        for c in t_calls:
            if c.get("um") is not None:
                services.add(str(c["um"]))
            if c.get("dm") is not None:
                services.add(str(c["dm"]))
            depth = int(c.get("call_depth", 0) or 0)
            if depth > 0:
                depths.append(depth)
            if c.get("is_self_loop"):
                self_loop_count += 1

        summary = summary_map.get(traceid, {})
        rows.append(
            {
                "traceid": traceid,
                "call_count": len(t_calls),
                "unique_service_count": len(services),
                "max_depth": max(depths) if depths else 0,
                "self_loop_count": self_loop_count,
                "has_self_loop": self_loop_count > 0,
                "is_structurally_complete": bool(
                    summary.get("is_structurally_complete", bool(t_calls))
                ),
                "is_clean_trace": bool(summary.get("is_clean_trace", bool(t_calls))),
            }
        )
    return rows


def build_comm_type_by_depth(
    annotated_calls: Iterable[Mapping[str, Any]],
    only_complete_traces: bool = False,
    complete_traceids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Compute communication type counts and shares per depth with explicit denominators."""
    counts_by_depth_type: dict[tuple[int, str], int] = defaultdict(int)
    totals_by_depth: dict[int, int] = defaultdict(int)

    for call in annotated_calls:
        if only_complete_traces and complete_traceids is not None:
            if str(call["traceid"]) not in complete_traceids:
                continue
        depth = int(call.get("call_depth", 0) or 0)
        if depth <= 0:
            continue
        rpctype = str(call["rpctype"])
        counts_by_depth_type[(depth, rpctype)] += 1
        totals_by_depth[depth] += 1

    results: list[dict[str, Any]] = []
    for (depth, rpctype), count in sorted(counts_by_depth_type.items()):
        denom = totals_by_depth[depth]
        results.append(
            {
                "call_depth": depth,
                "rpctype": rpctype,
                "call_count": count,
                "total_calls_at_depth": denom,
                "share": count / denom if denom > 0 else 0.0,
            }
        )
    return results
