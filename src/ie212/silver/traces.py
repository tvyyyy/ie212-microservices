"""Trace structure validation and call-tree reconstruction for Silver calls (T14).

Detects missing root, missing parent `rpcid`, parent-child endpoint inconsistencies
(`parent.dm != child.um`), unresolved ambiguity from `(traceid, rpcid)`, and sentinel endpoints.
Methodologically inspired by Casper (Huye et al., ICPE 2024) without claiming execution of
the standalone single-machine Casper tool.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


@dataclass
class TraceValidationResult:
    """Outputs of trace-level validation and structural annotation."""

    annotated_calls: list[dict[str, Any]] = field(default_factory=list)
    trace_summaries: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


def parse_rpcid_parent_and_depth(rpcid: str) -> tuple[str | None, int, bool]:
    """Parse hierarchical Alibaba `rpcid` into `(parent_rpcid, depth, is_valid_rpcid)`.

    Convention:
    - Root `rpcid` has no dot (e.g., `"0"`, `"9"`): `parent_rpcid = None`, `depth = 1`.
    - Child `rpcid` `"0.1"` has `parent_rpcid = "0"`, `depth = 2`.
    - Child `rpcid` `"0.1.2"` has `parent_rpcid = "0.1"`, `depth = 3`.
    - Empty or malformed segments (e.g., `"0..1"` or `""`) return `(None, 0, False)`.
    """
    token = rpcid.strip()
    if not token:
        return None, 0, False
    parts = token.split(".")
    if any(part == "" for part in parts):
        return None, 0, False
    depth = len(parts)
    parent = ".".join(parts[:-1]) if depth > 1 else None
    return parent, depth, True


def validate_traces(
    canonical_calls: Iterable[Mapping[str, Any]],
    ambiguous_calls: Iterable[Mapping[str, Any]] = (),
) -> TraceValidationResult:
    """Validate call-graph topology per `traceid` and annotate each call with depth/flags."""
    calls_by_trace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for call in canonical_calls:
        calls_by_trace[str(call["traceid"])].append(dict(call))

    ambiguous_by_trace: dict[str, set[str]] = defaultdict(set)
    for amb in ambiguous_calls:
        ambiguous_by_trace[str(amb["traceid"])].add(str(amb["rpcid"]))

    all_traceids = sorted(set(calls_by_trace.keys()) | set(ambiguous_by_trace.keys()))
    annotated_calls: list[dict[str, Any]] = []
    trace_summaries: list[dict[str, Any]] = []

    exclusion_reason_counts: dict[str, int] = defaultdict(int)
    valid_trace_count = 0
    excluded_trace_count = 0

    for traceid in all_traceids:
        trace_calls = calls_by_trace.get(traceid, [])
        amb_rpcids = ambiguous_by_trace.get(traceid, set())

        by_rpcid: dict[str, dict[str, Any]] = {}
        duplicate_rpcids: set[str] = set()
        for c in trace_calls:
            rid = str(c["rpcid"])
            if rid in by_rpcid:
                duplicate_rpcids.add(rid)
            else:
                by_rpcid[rid] = c

        has_root = any(
            parse_rpcid_parent_and_depth(rid)[0] is None
            and parse_rpcid_parent_and_depth(rid)[2]
            for rid in by_rpcid
        )

        missing_parent_count = 0
        malformed_rpcid_count = 0
        endpoint_mismatch_count = 0
        sentinel_call_count = 0

        for c in trace_calls:
            rid = str(c["rpcid"])
            parent_rid, depth, is_valid_rid = parse_rpcid_parent_and_depth(rid)
            is_missing_parent = False
            is_endpoint_mismatch = False

            if not is_valid_rid:
                malformed_rpcid_count += 1
            elif parent_rid is not None:
                parent_call = by_rpcid.get(parent_rid)
                if parent_call is None:
                    is_missing_parent = True
                    missing_parent_count += 1
                else:
                    p_dm = parent_call.get("dm")
                    c_um = c.get("um")
                    if p_dm is not None and c_um is not None and p_dm != c_um:
                        is_endpoint_mismatch = True
                        endpoint_mismatch_count += 1

            if c.get("has_sentinel_endpoint"):
                sentinel_call_count += 1

            annotated = dict(c)
            annotated["parent_rpcid"] = parent_rid
            annotated["call_depth"] = depth
            annotated["is_valid_rpcid"] = is_valid_rid
            annotated["is_missing_parent"] = is_missing_parent
            annotated["is_parent_endpoint_mismatch"] = is_endpoint_mismatch
            annotated["is_duplicate_rpcid"] = rid in duplicate_rpcids
            annotated_calls.append(annotated)

        reasons: list[str] = []
        if not trace_calls:
            reasons.append("no_canonical_calls")
        if not has_root:
            reasons.append("missing_root")
        if missing_parent_count > 0:
            reasons.append("missing_parent")
        if malformed_rpcid_count > 0:
            reasons.append("malformed_rpcid")
        if endpoint_mismatch_count > 0:
            reasons.append("parent_child_endpoint_mismatch")
        if duplicate_rpcids:
            reasons.append("duplicate_rpcid")
        if amb_rpcids:
            reasons.append("unresolved_rpcid_ambiguity")

        is_structurally_complete = (
            bool(trace_calls)
            and has_root
            and missing_parent_count == 0
            and malformed_rpcid_count == 0
            and not duplicate_rpcids
            and not amb_rpcids
        )
        is_clean_trace = is_structurally_complete and endpoint_mismatch_count == 0

        if is_structurally_complete:
            valid_trace_count += 1
        else:
            excluded_trace_count += 1
            for r in reasons:
                exclusion_reason_counts[r] += 1

        trace_summaries.append(
            {
                "traceid": traceid,
                "canonical_call_count": len(trace_calls),
                "ambiguous_rpcid_count": len(amb_rpcids),
                "has_root": has_root,
                "missing_parent_count": missing_parent_count,
                "malformed_rpcid_count": malformed_rpcid_count,
                "endpoint_mismatch_count": endpoint_mismatch_count,
                "duplicate_rpcid_count": len(duplicate_rpcids),
                "sentinel_call_count": sentinel_call_count,
                "is_structurally_complete": is_structurally_complete,
                "is_clean_trace": is_clean_trace,
                "quality_reasons": reasons,
            }
        )

    stats = {
        "total_trace_count": len(all_traceids),
        "structurally_complete_trace_count": valid_trace_count,
        "incomplete_or_ambiguous_trace_count": excluded_trace_count,
        "clean_trace_count": sum(1 for t in trace_summaries if t["is_clean_trace"]),
        "traces_with_endpoint_mismatch": sum(
            1 for t in trace_summaries if t["endpoint_mismatch_count"] > 0
        ),
        "traces_with_sentinels": sum(
            1 for t in trace_summaries if t["sentinel_call_count"] > 0
        ),
        "exclusion_reason_counts": dict(exclusion_reason_counts),
    }
    return TraceValidationResult(
        annotated_calls=annotated_calls,
        trace_summaries=trace_summaries,
        stats=stats,
    )
