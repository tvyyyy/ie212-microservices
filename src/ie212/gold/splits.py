"""Chronological 70/15/15 splits, train_only warm-up graph boundary, and common cohort (T24).

Implements anti-leakage rules from docs/split_cohort.md and agents_ie212.md §6.3 & §8.1:
- Global chronological 70/15/15 split by target timestamp `t + 1` (never random row split).
- Dedicated warm-up window inside Train (`0..warmup_end`) for constructing `train_only`
  service dependency graphs; model training only uses origins strictly after warm-up.
- Services unseen in `train_only` graph are flagged `is_unseen_in_train_graph=True`
  without peeking at future edges.
- Common cohort intersection across feature sets A, B, and C with exact sample-loss logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class TemporalSplitBoundaries:
    """Chronological boundaries shared across all services."""

    min_target_minute: int
    max_target_minute: int
    train_end_minute: int
    val_end_minute: int
    graph_warmup_end_minute: int
    total_unique_minutes: int
    train_minute_count: int
    val_minute_count: int
    test_minute_count: int
    warmup_minute_count: int


def compute_temporal_split_boundaries(
    target_minutes: Iterable[int],
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    warmup_minutes: int | None = 180,
    warmup_fraction_fallback: float = 0.35,
) -> TemporalSplitBoundaries:
    """Compute global chronological boundaries on sorted unique `target_minute` values."""
    if not abs((train_ratio + val_ratio + test_ratio) - 1.0) < 1e-6:
        raise ValueError("Split ratios must sum to 1.0")

    unique_mins = sorted({int(m) for m in target_minutes})
    n = len(unique_mins)
    if n < 5:
        raise ValueError("Need at least 5 unique target minutes to form train/val/test splits.")

    train_count = max(2, int(round(n * train_ratio)))
    val_count = max(1, int(round(n * val_ratio)))
    if train_count + val_count >= n:
        train_count = max(2, n - 2)
        val_count = 1
    test_count = n - train_count - val_count

    train_mins = unique_mins[:train_count]
    val_mins = unique_mins[train_count : train_count + val_count]

    if warmup_minutes is not None and warmup_minutes < train_count:
        wu_count = max(1, warmup_minutes)
    else:
        wu_count = max(1, min(train_count - 1, int(round(train_count * warmup_fraction_fallback))))

    return TemporalSplitBoundaries(
        min_target_minute=unique_mins[0],
        max_target_minute=unique_mins[-1],
        train_end_minute=train_mins[-1],
        val_end_minute=val_mins[-1],
        graph_warmup_end_minute=train_mins[wu_count - 1],
        total_unique_minutes=n,
        train_minute_count=train_count,
        val_minute_count=val_count,
        test_minute_count=test_count,
        warmup_minute_count=wu_count,
    )


def assign_split_and_warmup_flags(
    rows: Iterable[Mapping[str, Any]],
    boundaries: TemporalSplitBoundaries,
    train_graph_services: Iterable[str] = (),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Assign `split` (`train_warmup`, `train`, `validation`, `test`) and `unseen` flags."""
    known_train_services = {str(s) for s in train_graph_services}
    out_rows: list[dict[str, Any]] = []
    counts = {"train_warmup": 0, "train": 0, "validation": 0, "test": 0, "unseen_rows": 0}

    for row in rows:
        target_min = int(row["target_minute"])
        svc = str(row["service_id"])
        if target_min <= boundaries.graph_warmup_end_minute:
            split = "train_warmup"
        elif target_min <= boundaries.train_end_minute:
            split = "train"
        elif target_min <= boundaries.val_end_minute:
            split = "validation"
        else:
            split = "test"

        is_unseen = svc not in known_train_services
        counts[split] += 1
        if is_unseen:
            counts["unseen_rows"] += 1

        copied = dict(row)
        copied["split"] = split
        copied["is_train_warmup"] = split == "train_warmup"
        copied["is_unseen_in_train_graph"] = is_unseen
        out_rows.append(copied)

    return out_rows, counts


def build_common_cohort(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Filter to post-warmup rows valid across common A/B/C cohort and log loss per step."""
    materialized = [dict(r) for r in rows]
    n_total = len(materialized)

    step_label = [r for r in materialized if r.get("has_valid_target")]
    step_post_warmup = [r for r in step_label if r.get("split") != "train_warmup"]
    step_a = [r for r in step_post_warmup if r.get("has_features_a", True)]
    step_b = [r for r in step_a if r.get("has_features_b", True)]
    step_c = [r for r in step_b if r.get("has_features_c", True)]

    loss_log = {
        "total_rows": n_total,
        "after_valid_target": len(step_label),
        "dropped_invalid_target": n_total - len(step_label),
        "after_exclude_warmup": len(step_post_warmup),
        "dropped_warmup": len(step_label) - len(step_post_warmup),
        "after_feature_set_A": len(step_a),
        "dropped_missing_A": len(step_post_warmup) - len(step_a),
        "after_feature_set_B": len(step_b),
        "dropped_missing_B": len(step_a) - len(step_b),
        "after_feature_set_C_common_cohort": len(step_c),
        "dropped_missing_C": len(step_b) - len(step_c),
        "common_cohort_service_count": len({str(r["service_id"]) for r in step_c}),
    }
    return step_c, loss_log
