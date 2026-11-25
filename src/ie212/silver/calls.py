"""Silver processing for MSCallGraph records (T13).

Implements live-verified Alibaba v2021 call normalization rules locked in docs/decisions.md:
- Preserve raw signed `rt` in milliseconds and source lineage.
- Normalize documented sentinel endpoints (`""`, whitespace, `(?)`, `NAN`) to `None` with flags.
- Separate caller (`+rt`) and callee (`-rt`) sides for `rpc`/`http`; treat `rt == 0` as ambiguous side.
- Flag negative `mq` durations (`is_invalid_duration=True`) and exclude from duration summaries without
  misclassifying them as callee RPCs; retain topology/counts subject to deduplication.
- Retain `0` as valid for `mc`/`db`/`userDefined`, flagging negative durations.
- Collapse exact duplicate observations and perform two-sided pairing within `(traceid, rpcid)` only
  when unambiguous; route multiplicity > 2 or conflicting groups to `ambiguous_calls`.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import math
from typing import Any, Iterable, Mapping, Sequence

VALID_RPCTYPES: frozenset[str] = frozenset(
    {"rpc", "mc", "http", "userDefined", "db", "mq"}
)
TWO_SIDED_RPCTYPES: frozenset[str] = frozenset({"rpc", "http"})
DEFAULT_SENTINEL_TOKENS: frozenset[str] = frozenset({"", "(?)", "NAN"})


@dataclass(frozen=True)
class CallProcessingConfig:
    """Configuration for Silver MSCallGraph normalization and deduplication."""

    timestamp_tolerance_ms: int = 1000
    allow_unambiguous_endpoint_repair: bool = True
    sentinel_tokens: frozenset[str] = DEFAULT_SENTINEL_TOKENS
    valid_rpctypes: frozenset[str] = VALID_RPCTYPES


@dataclass
class CallDeduplicationResult:
    """Outputs and audit counts from Silver call processing."""

    canonical_calls: list[dict[str, Any]] = field(default_factory=list)
    ambiguous_calls: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


def normalize_endpoint(
    value: Any,
    sentinel_tokens: frozenset[str] = DEFAULT_SENTINEL_TOKENS,
) -> tuple[str | None, str | None, bool]:
    """Return `(raw_text, normalized_service, is_sentinel)` without guessing unknown names."""
    if value is None:
        return None, None, True
    if isinstance(value, float) and math.isnan(value):
        return "NAN", None, True
    raw = str(value)
    stripped = raw.strip()
    if stripped == "" or stripped in sentinel_tokens or stripped.upper() == "NAN":
        return raw, None, True
    return raw, stripped, False


def classify_observation_rt(
    rpctype: str,
    rt_value: Any,
) -> dict[str, Any]:
    """Classify observation side and duration validity by `rpctype` without global `abs(rt)`."""
    if rt_value is None or (isinstance(rt_value, float) and math.isnan(rt_value)):
        return {
            "raw_rt_ms": None,
            "observation_side": "missing_rt",
            "caller_rt_ms": None,
            "callee_rt_ms": None,
            "other_rt_ms": None,
            "is_zero_rt": False,
            "is_invalid_duration": True,
            "exclude_from_duration_summary": True,
        }

    raw_rt = float(rt_value)
    if not math.isfinite(raw_rt):
        return {
            "raw_rt_ms": raw_rt,
            "observation_side": "invalid_rt",
            "caller_rt_ms": None,
            "callee_rt_ms": None,
            "other_rt_ms": None,
            "is_zero_rt": False,
            "is_invalid_duration": True,
            "exclude_from_duration_summary": True,
        }

    if rpctype in TWO_SIDED_RPCTYPES:
        if raw_rt > 0:
            return {
                "raw_rt_ms": raw_rt,
                "observation_side": "caller",
                "caller_rt_ms": raw_rt,
                "callee_rt_ms": None,
                "other_rt_ms": None,
                "is_zero_rt": False,
                "is_invalid_duration": False,
                "exclude_from_duration_summary": False,
            }
        if raw_rt < 0:
            return {
                "raw_rt_ms": raw_rt,
                "observation_side": "callee",
                "caller_rt_ms": None,
                "callee_rt_ms": abs(raw_rt),
                "other_rt_ms": None,
                "is_zero_rt": False,
                "is_invalid_duration": False,
                "exclude_from_duration_summary": False,
            }
        return {
            "raw_rt_ms": raw_rt,
            "observation_side": "ambiguous_zero",
            "caller_rt_ms": None,
            "callee_rt_ms": None,
            "other_rt_ms": None,
            "is_zero_rt": True,
            "is_invalid_duration": False,
            "exclude_from_duration_summary": True,
        }

    if rpctype == "mq":
        is_invalid = raw_rt < 0
        return {
            "raw_rt_ms": raw_rt,
            "observation_side": "single",
            "caller_rt_ms": None,
            "callee_rt_ms": None,
            "other_rt_ms": None if is_invalid else raw_rt,
            "is_zero_rt": raw_rt == 0.0,
            "is_invalid_duration": is_invalid,
            "exclude_from_duration_summary": is_invalid,
        }

    # mc, db, userDefined (or flagged unknown rpctype): retain zero, flag negative duration
    is_invalid = raw_rt < 0
    return {
        "raw_rt_ms": raw_rt,
        "observation_side": "single",
        "caller_rt_ms": None,
        "callee_rt_ms": None,
        "other_rt_ms": None if is_invalid else raw_rt,
        "is_zero_rt": raw_rt == 0.0,
        "is_invalid_duration": is_invalid,
        "exclude_from_duration_summary": is_invalid,
    }


def normalize_call_record(
    row: Mapping[str, Any],
    source_file: str | None = None,
    source_row_offset: int | None = None,
    config: CallProcessingConfig | None = None,
) -> dict[str, Any]:
    """Normalize a single raw MSCallGraph record while retaining raw values and lineage."""
    cfg = config or CallProcessingConfig()

    traceid = str(row["traceid"]).strip()
    rpcid = str(row["rpcid"]).strip()
    timestamp_ms = int(row["timestamp"])
    rpctype = str(row["rpctype"]).strip()
    interface_raw, interface_norm, interface_sentinel = normalize_endpoint(
        row.get("interface"), cfg.sentinel_tokens
    )
    um_raw, um_norm, um_sentinel = normalize_endpoint(row.get("um"), cfg.sentinel_tokens)
    dm_raw, dm_norm, dm_sentinel = normalize_endpoint(row.get("dm"), cfg.sentinel_tokens)

    rt_info = classify_observation_rt(rpctype, row.get("rt"))
    is_known_rpctype = rpctype in cfg.valid_rpctypes
    is_self_loop = um_norm is not None and dm_norm is not None and um_norm == dm_norm

    lineage_file = row.get("source_file", source_file)
    lineage_offset = row.get("source_row_offset", source_row_offset)
    if lineage_offset is None and "" in row:
        lineage_offset = row[""]

    return {
        "traceid": traceid,
        "rpcid": rpcid,
        "timestamp_ms": timestamp_ms,
        "rpctype": rpctype,
        "is_known_rpctype": is_known_rpctype,
        "interface": interface_norm,
        "interface_raw": interface_raw,
        "interface_is_sentinel": interface_sentinel,
        "um": um_norm,
        "um_raw": um_raw,
        "um_is_sentinel": um_sentinel,
        "dm": dm_norm,
        "dm_raw": dm_raw,
        "dm_is_sentinel": dm_sentinel,
        "has_sentinel_endpoint": um_sentinel or dm_sentinel,
        "is_self_loop": is_self_loop,
        "source_refs": [(lineage_file, lineage_offset)],
        **rt_info,
    }


def _exact_dedup_key(rec: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        rec["traceid"],
        rec["rpcid"],
        rec["timestamp_ms"],
        rec["rpctype"],
        rec["interface"],
        rec["um"],
        rec["dm"],
        rec["observation_side"],
        rec["raw_rt_ms"],
    )


def _merge_exact_duplicates(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for rec in records:
        key = _exact_dedup_key(rec)
        if key not in grouped:
            copied = dict(rec)
            copied["source_refs"] = list(rec["source_refs"])
            copied["exact_duplicate_count"] = 1
            grouped[key] = copied
        else:
            grouped[key]["source_refs"].extend(rec["source_refs"])
            grouped[key]["exact_duplicate_count"] += 1
    return list(grouped.values())


def _compatible_endpoint(a: str | None, b: str | None) -> tuple[bool, str | None, bool]:
    """Check endpoint compatibility across two observations of the same occurrence.

    Returns `(is_compatible, resolved_value, was_repaired)`.
    """
    if a == b:
        return True, a, False
    if a is None and b is not None:
        return True, b, True
    if b is None and a is not None:
        return True, a, True
    return False, None, False


def _attempt_two_sided_pair(
    obs_a: dict[str, Any],
    obs_b: dict[str, Any],
    cfg: CallProcessingConfig,
) -> tuple[dict[str, Any] | None, str | None]:
    """Pair two distinct observations in a `(traceid, rpcid)` group if unambiguous."""
    if obs_a["rpctype"] != obs_b["rpctype"]:
        return None, "rpctype_conflict"
    if obs_a["rpctype"] not in TWO_SIDED_RPCTYPES:
        return None, "non_two_sided_multiplicity"

    if abs(obs_a["timestamp_ms"] - obs_b["timestamp_ms"]) > cfg.timestamp_tolerance_ms:
        return None, "timestamp_tolerance_exceeded"

    iface_ok, iface_val, _ = _compatible_endpoint(obs_a["interface"], obs_b["interface"])
    if not iface_ok:
        return None, "interface_conflict"

    um_ok, um_val, um_repaired = _compatible_endpoint(obs_a["um"], obs_b["um"])
    dm_ok, dm_val, dm_repaired = _compatible_endpoint(obs_a["dm"], obs_b["dm"])
    if not um_ok or not dm_ok:
        return None, "endpoint_conflict"

    if (um_repaired or dm_repaired) and not cfg.allow_unambiguous_endpoint_repair:
        return None, "endpoint_mismatch_repair_disabled"

    sides = {obs_a["observation_side"], obs_b["observation_side"]}
    valid_pair_sides = (
        sides == {"caller", "callee"}
        or sides == {"caller", "ambiguous_zero"}
        or sides == {"callee", "ambiguous_zero"}
        or sides == {"ambiguous_zero"}
    )
    if not valid_pair_sides:
        return None, f"incompatible_sides:{sorted(sides)}"

    caller_rt = (
        obs_a["caller_rt_ms"]
        if obs_a["caller_rt_ms"] is not None
        else obs_b["caller_rt_ms"]
    )
    callee_rt = (
        obs_a["callee_rt_ms"]
        if obs_a["callee_rt_ms"] is not None
        else obs_b["callee_rt_ms"]
    )
    # For unambiguous caller + ambiguous_zero (or callee + ambiguous_zero), retain 0.0 on the missing side
    if sides == {"caller", "ambiguous_zero"} and callee_rt is None:
        callee_rt = 0.0
    if sides == {"callee", "ambiguous_zero"} and caller_rt is None:
        caller_rt = 0.0

    repaired_fields = []
    if um_repaired:
        repaired_fields.append("um")
    if dm_repaired:
        repaired_fields.append("dm")

    is_zero_only = sides == {"ambiguous_zero"}
    is_self_loop = um_val is not None and dm_val is not None and um_val == dm_val
    has_sentinel = um_val is None or dm_val is None

    paired = {
        "traceid": obs_a["traceid"],
        "rpcid": obs_a["rpcid"],
        "timestamp_ms": min(obs_a["timestamp_ms"], obs_b["timestamp_ms"]),
        "rpctype": obs_a["rpctype"],
        "is_known_rpctype": obs_a["is_known_rpctype"],
        "interface": iface_val,
        "um": um_val,
        "dm": dm_val,
        "um_is_sentinel": um_val is None,
        "dm_is_sentinel": dm_val is None,
        "has_sentinel_endpoint": has_sentinel,
        "is_self_loop": is_self_loop,
        "observation_side": "paired" if not is_zero_only else "paired_zero",
        "observation_count": 2,
        "exact_duplicate_count": (
            obs_a["exact_duplicate_count"] + obs_b["exact_duplicate_count"]
        ),
        "raw_rt_ms": caller_rt if caller_rt is not None else obs_a["raw_rt_ms"],
        "caller_rt_ms": caller_rt,
        "callee_rt_ms": callee_rt,
        "other_rt_ms": None,
        "is_zero_rt": is_zero_only or (caller_rt == 0.0) or (callee_rt == 0.0),
        "is_invalid_duration": False,
        "exclude_from_duration_summary": is_zero_only,
        "repaired_from_pair": bool(repaired_fields),
        "repaired_fields": repaired_fields,
        "source_refs": list(obs_a["source_refs"]) + list(obs_b["source_refs"]),
    }
    return paired, None


def process_silver_calls(
    raw_rows: Iterable[Mapping[str, Any]],
    config: CallProcessingConfig | None = None,
) -> CallDeduplicationResult:
    """Normalize and deduplicate raw MSCallGraph rows into canonical and ambiguous tables."""
    cfg = config or CallProcessingConfig()
    normalized = [
        normalize_call_record(row, source_row_offset=idx, config=cfg)
        for idx, row in enumerate(raw_rows)
    ]

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for rec in normalized:
        groups[(rec["traceid"], rec["rpcid"])].append(rec)

    canonical: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    ambiguity_reasons: dict[str, int] = defaultdict(int)
    exact_dups_collapsed = 0
    paired_count = 0
    repaired_count = 0

    for (traceid, rpcid), recs in groups.items():
        deduped = _merge_exact_duplicates(recs)
        exact_dups_collapsed += len(recs) - len(deduped)

        if len(deduped) == 1:
            single = dict(deduped[0])
            single["observation_count"] = 1
            single["repaired_from_pair"] = False
            single["repaired_fields"] = []
            canonical.append(single)
        elif len(deduped) == 2:
            paired, reason = _attempt_two_sided_pair(deduped[0], deduped[1], cfg)
            if paired is not None:
                canonical.append(paired)
                paired_count += 1
                if paired["repaired_from_pair"]:
                    repaired_count += 1
            else:
                ambiguity_reasons[reason or "unpaired_two_rows"] += 1
                for item in deduped:
                    amb = dict(item)
                    amb["ambiguity_reason"] = reason
                    amb["group_size_after_exact_dedup"] = len(deduped)
                    ambiguous.append(amb)
        else:
            reason = f"multiplicity_gt_2:{len(deduped)}"
            ambiguity_reasons["multiplicity_gt_2"] += 1
            for item in deduped:
                amb = dict(item)
                amb["ambiguity_reason"] = reason
                amb["group_size_after_exact_dedup"] = len(deduped)
                ambiguous.append(amb)

    raw_count = len(normalized)
    canonical_count = len(canonical)
    ambiguous_group_count = sum(ambiguity_reasons.values())
    stats = {
        "raw_row_count": raw_count,
        "group_count_traceid_rpcid": len(groups),
        "exact_duplicates_collapsed": exact_dups_collapsed,
        "canonical_call_count": canonical_count,
        "paired_two_sided_count": paired_count,
        "repaired_endpoint_count": repaired_count,
        "ambiguous_row_count": len(ambiguous),
        "ambiguous_group_count": ambiguous_group_count,
        "ambiguity_reasons": dict(ambiguity_reasons),
        "sentinel_call_count": sum(1 for r in canonical if r["has_sentinel_endpoint"]),
        "self_loop_call_count": sum(1 for r in canonical if r["is_self_loop"]),
        "invalid_duration_call_count": sum(
            1 for r in canonical if r["is_invalid_duration"]
        ),
        "canonical_coverage_over_groups": (
            (len(groups) - ambiguous_group_count) / len(groups) if groups else 1.0
        ),
    }
    return CallDeduplicationResult(
        canonical_calls=canonical,
        ambiguous_calls=ambiguous,
        stats=stats,
    )
