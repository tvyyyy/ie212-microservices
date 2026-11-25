# Temporal Splits and Evaluation Cohort

The analytical dataset has not yet been constructed (`T24`). Locked experimental design rules as of **25 November 2026**:

- **Chronological Split:** Split observations globally by target timestamp (`t + 1`) using a proposed **70/15/15** Train/Validation/Test ratio (~504 / 108 / 108 minutes across the 12-hour window). Exact boundary timestamps and sample counts will be recorded in `T24` once real data are ingested.
- **Anti-Intra-Train-Leakage Graph Snapshot (`train_only`):** Construct the predictive service dependency graph exclusively on a dedicated **warm-up window** at the start of Train (e.g., minutes `0..179` for graph construction, minutes `180..503` for model fitting — configured in `configs/features.yaml` with `warmup_minutes: 180` and `warmup_fraction_fallback: 0.35`, implemented in `src/ie212/gold/splits.py`) so structural descriptors are strictly available at every forecast origin. Unseen services receive explicit missing/unseen flags (`is_unseen_in_train_graph`).
- **Common Cohort:** Evaluate feature sets **A**, **B**, and **C** on the exact same `(service_id, target_minute)` keys; `build_common_cohort` logs dropped rows/services at every join and filter stage (`valid_target`, `post_warmup`, `A`, `B`, `C`).

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).
