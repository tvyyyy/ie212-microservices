"""Gold service-minute complete time index and 30s->60s alignment (T20).

Enforces live-verified cadence rules from docs/decisions.md and agents_ie212.md §5.2:
- Both `Node` and `MSResource` sample every 30,000 ms (`ts % 30000 == 0`).
- Aligning 30s -> 60s maps `:00` and `:30` of minute `t` strictly to
  `minute_ts = (ts // 60000) * 60000` (never mixing `:30` from `t + 1`).
- Builds a complete `(service_id, minute_ts)` index grid, maps containers -> services -> hosts,
  verifies key uniqueness, and audits row multiplication factor (`≈ 1.0`).
"""

from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Iterable, Mapping, Sequence


def align_30s_to_60s(
    rows: Iterable[Mapping[str, Any]],
    entity_keys: Sequence[str],
    value_columns: Sequence[str],
    cadence_ms: int = 30_000,
    minute_ms: int = 60_000,
    passthrough_keys: Sequence[str] = (),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Aggregate 30s measurements strictly within each 60s minute `[t, t + 60s)`."""
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    input_count = 0
    misaligned_count = 0

    for row in rows:
        input_count += 1
        ts = int(row["timestamp"])
        if ts % cadence_ms != 0:
            misaligned_count += 1
            continue
        minute_ts = (ts // minute_ms) * minute_ms
        key = tuple(row[k] for k in entity_keys) + (minute_ts,)
        grouped[key].append(row)

    aligned_rows: list[dict[str, Any]] = []
    partial_minute_count = 0
    duplicate_half_window_count = 0

    for key, bucket in sorted(grouped.items()):
        entity_dict = dict(zip(entity_keys, key[:-1]))
        minute_ts = key[-1]
        unique_ts = {int(r["timestamp"]) for r in bucket}
        if len(bucket) > len(unique_ts):
            duplicate_half_window_count += 1
        if len(unique_ts) < 2:
            partial_minute_count += 1

        out: dict[str, Any] = {
            **entity_dict,
            "minute_ts": minute_ts,
            "half_window_sample_count": len(unique_ts),
            "raw_rows_in_minute": len(bucket),
            "is_partial_minute": len(unique_ts) < 2,
        }
        for pk in passthrough_keys:
            vals = sorted({str(r[pk]) for r in bucket if r.get(pk) is not None})
            out[pk] = vals[0] if len(vals) == 1 else (",".join(vals) if vals else None)
            out[f"{pk}_ambiguous_in_minute"] = len(vals) > 1

        for col in value_columns:
            nums = []
            for r in bucket:
                v = r.get(col)
                if v is not None and math.isfinite(float(v)):
                    nums.append(float(v))
            out[col] = sum(nums) / len(nums) if nums else None

        aligned_rows.append(out)

    stats = {
        "input_row_count": input_count,
        "misaligned_row_count": misaligned_count,
        "output_minute_row_count": len(aligned_rows),
        "partial_minute_count": partial_minute_count,
        "duplicate_half_window_groups": duplicate_half_window_count,
        "is_unique_key": len(aligned_rows) == len(grouped),
    }
    return aligned_rows, stats


def build_service_minute_index(
    services: Iterable[str],
    min_minute_ts: int,
    max_minute_ts: int,
    resource_minute_rows: Iterable[Mapping[str, Any]] = (),
    node_minute_rows: Iterable[Mapping[str, Any]] = (),
    label_rows: Iterable[Mapping[str, Any]] = (),
    minute_step_ms: int = 60_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build complete `(service_id, minute_ts)` index with pre-aggregated host mappings.

    Pre-aggregates container -> service -> host at `(service_id, minute_ts)` grain before
    joining onto the complete grid so `row_multiplication_factor` stays strictly `1.0`.
    """
    service_list = sorted({str(s) for s in services if s is not None})
    if max_minute_ts < min_minute_ts:
        raise ValueError("max_minute_ts must be >= min_minute_ts")

    minutes = list(range(min_minute_ts, max_minute_ts + 1, minute_step_ms))

    # Index Node utilization by (nodeid, minute_ts)
    node_lookup: dict[tuple[str, int], Mapping[str, Any]] = {}
    node_dup_keys = 0
    for nrow in node_minute_rows:
        nkey = (str(nrow["nodeid"]), int(nrow["minute_ts"]))
        if nkey in node_lookup:
            node_dup_keys += 1
        else:
            node_lookup[nkey] = nrow

    # Pre-aggregate MSResource at (msname, minute_ts) to prevent join fan-out
    res_by_svc_min: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    seen_container_min: set[tuple[str, str, int]] = set()
    dup_container_min = 0
    for rrow in resource_minute_rows:
        ckey = (str(rrow["msname"]), str(rrow["msinstanceid"]), int(rrow["minute_ts"]))
        if ckey in seen_container_min:
            dup_container_min += 1
            continue
        seen_container_min.add(ckey)
        res_by_svc_min[(ckey[0], ckey[2])].append(rrow)

    label_lookup: dict[tuple[str, int], Mapping[str, Any]] = {}
    for lrow in label_rows:
        lkey = (str(lrow["service_id"]), int(lrow["forecast_origin_minute"]))
        label_lookup[lkey] = lrow

    grid_rows: list[dict[str, Any]] = []
    seen_grid_keys: set[tuple[str, int]] = set()
    matched_resource_minutes = 0
    matched_host_minutes = 0
    matched_label_minutes = 0

    for svc in service_list:
        for mts in minutes:
            gkey = (svc, mts)
            seen_grid_keys.add(gkey)
            containers = res_by_svc_min.get(gkey, [])
            node_ids = sorted(
                {str(c["nodeid"]) for c in containers if c.get("nodeid") is not None}
            )
            cpus = [
                float(c["instance_cpu_usage"])
                for c in containers
                if c.get("instance_cpu_usage") is not None
            ]
            mems = [
                float(c["instance_memory_usage"])
                for c in containers
                if c.get("instance_memory_usage") is not None
            ]

            # Aggregate host utilization across distinct hosts hosting this service in minute t
            host_cpus = []
            host_mems = []
            for nid in node_ids:
                ninfo = node_lookup.get((nid, mts))
                if ninfo is not None:
                    if ninfo.get("node_cpu_usage") is not None:
                        host_cpus.append(float(ninfo["node_cpu_usage"]))
                    if ninfo.get("node_memory_usage") is not None:
                        host_mems.append(float(ninfo["node_memory_usage"]))

            if containers:
                matched_resource_minutes += 1
            if host_cpus or host_mems:
                matched_host_minutes += 1

            lbl = label_lookup.get(gkey)
            if lbl is not None:
                matched_label_minutes += 1

            grid_rows.append(
                {
                    "service_id": svc,
                    "minute_ts": mts,
                    "container_count": len(containers),
                    "host_count": len(node_ids),
                    "host_ids": node_ids,
                    "mean_instance_cpu_usage": sum(cpus) / len(cpus) if cpus else None,
                    "mean_instance_memory_usage": (
                        sum(mems) / len(mems) if mems else None
                    ),
                    "mean_host_cpu_usage": (
                        sum(host_cpus) / len(host_cpus) if host_cpus else None
                    ),
                    "mean_host_memory_usage": (
                        sum(host_mems) / len(host_mems) if host_mems else None
                    ),
                    "has_resource_mapping": bool(containers),
                    "has_host_mapping": bool(host_cpus or host_mems),
                    "has_label_record": lbl is not None,
                    "mean_rt_ms": lbl.get("mean_rt_ms") if lbl else None,
                    "y_mean_rpc_rt_ms": lbl.get("y_mean_rpc_rt_ms") if lbl else None,
                    "has_valid_target": bool(lbl.get("has_valid_target")) if lbl else False,
                }
            )

    expected_rows = len(service_list) * len(minutes)
    stats = {
        "service_count": len(service_list),
        "minute_count": len(minutes),
        "expected_grid_rows": expected_rows,
        "actual_grid_rows": len(grid_rows),
        "unique_key_count": len(seen_grid_keys),
        "is_unique_service_minute": len(grid_rows) == len(seen_grid_keys),
        "row_multiplication_factor": (
            len(grid_rows) / expected_rows if expected_rows > 0 else 1.0
        ),
        "duplicate_node_keys_ignored": node_dup_keys,
        "duplicate_container_keys_ignored": dup_container_min,
        "matched_resource_minutes": matched_resource_minutes,
        "matched_host_minutes": matched_host_minutes,
        "matched_label_minutes": matched_label_minutes,
    }
    return grid_rows, stats
