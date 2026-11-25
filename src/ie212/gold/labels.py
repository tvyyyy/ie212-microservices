"""Gold next-minute traffic-weighted RPC latency labels (T19).

Implements agents_ie212.md §5.1:
- `mean_rt(s, t) = sum_i(q(i, t) * r(i, t)) / sum_i(q(i, t))` using container-level
  `providerRPC_RT` (`r`) and `providerRPC_MCR` (`q`) on compatible 60s windows.
- `y(s, t) = mean_rt(s, t + 1)` ONLY when minute `t + 1` is strictly contiguous
  (`minute_ts + expected_window_ms`). Never uses `lead(1)` across gaps.
- Missing metrics or zero traffic denominators remain explicitly unlabeled (`None`),
  never imputed as `0.0` or unweighted container averages.
"""

from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Iterable, Mapping

TARGET_RT_METRIC = "providerRPC_RT"
TARGET_WEIGHT_METRIC = "providerRPC_MCR"


def _is_finite_number(val: Any) -> bool:
    if val is None:
        return False
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return False
    return math.isfinite(fval)


def compute_service_minute_labels(
    metric_rows: Iterable[Mapping[str, Any]],
    expected_window_ms: int = 60_000,
    horizon_minutes: int = 1,
) -> list[dict[str, Any]]:
    """Compute service-minute weighted mean RT and strict `t + horizon` targets."""
    # Group container-minute observations from long MSRTQps rows
    container_obs: dict[tuple[str, str, int], dict[str, Any]] = {}
    incompatible_windows: dict[tuple[str, int], int] = defaultdict(int)

    for row in metric_rows:
        msname = str(row["msname"])
        msinstanceid = str(row["msinstanceid"])
        ts = int(row["timestamp"])
        metric = str(row["metric"])
        if metric not in (TARGET_RT_METRIC, TARGET_WEIGHT_METRIC):
            continue

        if ts % expected_window_ms != 0:
            minute_bucket = (ts // expected_window_ms) * expected_window_ms
            incompatible_windows[(msname, minute_bucket)] += 1
            continue

        key = (msname, msinstanceid, ts)
        entry = container_obs.setdefault(
            key, {"rt": None, "mcr": None, "msname": msname, "minute_ts": ts}
        )
        entry["rt" if metric == TARGET_RT_METRIC else "mcr"] = row.get("value")

    by_service_minute: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for (_, _, _), obs in container_obs.items():
        by_service_minute[(obs["msname"], obs["minute_ts"])].append(obs)

    # Also include service-minutes that only had incompatible windows so they are explicitly flagged
    all_keys = sorted(set(by_service_minute.keys()) | set(incompatible_windows.keys()))

    current_rt_rows: dict[tuple[str, int], dict[str, Any]] = {}
    for service_id, minute_ts in all_keys:
        obs_list = by_service_minute.get((service_id, minute_ts), [])
        incompat_count = incompatible_windows.get((service_id, minute_ts), 0)

        weighted_num = 0.0
        weighted_den = 0.0
        valid_containers = 0
        zero_mcr_containers = 0
        dropped_containers = 0

        for obs in obs_list:
            rt_val = obs["rt"]
            mcr_val = obs["mcr"]
            if not (_is_finite_number(rt_val) and _is_finite_number(mcr_val)):
                dropped_containers += 1
                continue
            r = float(rt_val)
            q = float(mcr_val)
            if r < 0 or q < 0:
                dropped_containers += 1
                continue
            if q == 0.0:
                zero_mcr_containers += 1
                continue
            weighted_num += q * r
            weighted_den += q
            valid_containers += 1

        if weighted_den > 0.0 and valid_containers > 0:
            mean_rt_ms: float | None = weighted_num / weighted_den
            label_status = "valid"
        elif zero_mcr_containers > 0 and valid_containers == 0 and dropped_containers == 0:
            mean_rt_ms = None
            label_status = "zero_denominator"
        elif incompat_count > 0 and not obs_list:
            mean_rt_ms = None
            label_status = "window_incompatible"
        else:
            mean_rt_ms = None
            label_status = "missing_metric"

        current_rt_rows[(service_id, minute_ts)] = {
            "service_id": service_id,
            "forecast_origin_minute": minute_ts,
            "mean_rt_ms": mean_rt_ms,
            "total_provider_rpc_mcr": weighted_den,
            "valid_container_count": valid_containers,
            "zero_mcr_container_count": zero_mcr_containers,
            "dropped_container_count": dropped_containers,
            "incompatible_window_count": incompat_count,
            "label_status": label_status,
        }

    step_ms = horizon_minutes * expected_window_ms
    labeled_rows: list[dict[str, Any]] = []
    for (service_id, origin_ts), row in sorted(current_rt_rows.items()):
        target_ts = origin_ts + step_ms
        target_row = current_rt_rows.get((service_id, target_ts))

        if target_row is None:
            y_val: float | None = None
            has_valid_target = False
            target_missing_reason: str | None = "missing_next_minute"
        elif target_row["label_status"] != "valid":
            y_val = None
            has_valid_target = False
            target_missing_reason = f"target_{target_row['label_status']}"
        else:
            y_val = target_row["mean_rt_ms"]
            has_valid_target = True
            target_missing_reason = None

        out = dict(row)
        out["target_minute"] = target_ts
        out["y_mean_rpc_rt_ms"] = y_val
        out["has_valid_target"] = has_valid_target
        out["target_missing_reason"] = target_missing_reason
        labeled_rows.append(out)

    return labeled_rows
