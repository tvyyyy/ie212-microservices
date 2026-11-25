"""Data quality and join-coverage diagnostics (T16).

Produces structured `data_quality_v1` reports from real Silver/Gold outputs without
fabricating dataset metrics prior to data acquisition.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def compute_join_coverage(
    left_keys: Iterable[Any],
    right_keys: Iterable[Any],
    joined_row_count: int,
    left_row_count: int | None = None,
) -> dict[str, Any]:
    """Measure key overlap coverage and row-multiplication factor for a join."""
    left_set = set(left_keys)
    right_set = set(right_keys)
    matched = left_set & right_set
    base_rows = left_row_count if left_row_count is not None else len(left_set)
    return {
        "left_unique_keys": len(left_set),
        "right_unique_keys": len(right_set),
        "matched_unique_keys": len(matched),
        "left_key_coverage": len(matched) / len(left_set) if left_set else 0.0,
        "right_key_coverage": len(matched) / len(right_set) if right_set else 0.0,
        "left_row_count": base_rows,
        "joined_row_count": joined_row_count,
        "row_multiplication_factor": (
            joined_row_count / base_rows if base_rows > 0 else 0.0
        ),
    }


def build_data_quality_v1_report(
    call_stats: Mapping[str, Any],
    trace_stats: Mapping[str, Any],
    service_call_set: Iterable[str] = (),
    service_metric_set: Iterable[str] = (),
    join_coverages: Mapping[str, Mapping[str, Any]] | None = None,
    scope_note: str = "unverified_scope",
) -> dict[str, Any]:
    """Assemble `reports/data_quality_v1` payload with explicit provenance and exclusions."""
    call_services = {s for s in service_call_set if s is not None}
    metric_services = {s for s in service_metric_set if s is not None}
    overlap = call_services & metric_services

    return {
        "report_version": "v1",
        "scope_note": scope_note,
        "calls": dict(call_stats),
        "traces": dict(trace_stats),
        "services": {
            "unique_call_services": len(call_services),
            "unique_metric_services": len(metric_services),
            "overlapping_services": len(overlap),
            "overlap_over_metric_services": (
                len(overlap) / len(metric_services) if metric_services else 0.0
            ),
            "overlap_over_call_services": (
                len(overlap) / len(call_services) if call_services else 0.0
            ),
        },
        "join_coverages": {k: dict(v) for k, v in (join_coverages or {}).items()},
    }
