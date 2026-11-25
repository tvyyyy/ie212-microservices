"""Hand-calculated synthetic fixtures for temporal split, warm-up graph, and common cohort (T24)."""

from ie212.gold.splits import (
    assign_split_and_warmup_flags,
    build_common_cohort,
    compute_temporal_split_boundaries,
)


def test_chronological_split_warmup_and_unseen_flags():
    minutes = [i * 60000 for i in range(1, 21)]  # 20 target minutes -> 14 train, 3 val, 3 test
    bounds = compute_temporal_split_boundaries(
        minutes,
        train_ratio=0.70,
        val_ratio=0.15,
        test_ratio=0.15,
        warmup_minutes=5,
    )
    assert bounds.train_minute_count == 14
    assert bounds.val_minute_count == 3
    assert bounds.test_minute_count == 3
    assert bounds.warmup_minute_count == 5
    assert bounds.graph_warmup_end_minute == 5 * 60000
    assert bounds.train_end_minute == 14 * 60000
    assert bounds.val_end_minute == 17 * 60000

    rows = []
    for m in minutes:
        rows.append(
            {
                "service_id": "svc_known",
                "target_minute": m,
                "has_valid_target": True,
                "has_features_a": True,
                "has_features_b": True,
                "has_features_c": m != 18 * 60000,  # missing C at minute 18
            }
        )
        rows.append(
            {
                "service_id": "svc_new",
                "target_minute": m,
                "has_valid_target": True,
                "has_features_a": True,
                "has_features_b": True,
                "has_features_c": True,
            }
        )

    flagged, split_counts = assign_split_and_warmup_flags(
        rows, bounds, train_graph_services={"svc_known"}
    )
    assert split_counts["train_warmup"] == 10
    assert split_counts["train"] == 18
    assert split_counts["validation"] == 6
    assert split_counts["test"] == 6
    assert split_counts["unseen_rows"] == 20

    cohort, loss = build_common_cohort(flagged)
    assert loss["dropped_warmup"] == 10
    assert loss["dropped_missing_C"] == 1
    assert len(cohort) == 29
    assert all(r["split"] != "train_warmup" for r in cohort)
