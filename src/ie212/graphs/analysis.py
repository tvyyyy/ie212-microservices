"""RQ1 structural graph analysis: degree distributions, load concentration, and Burr fitting (T21, T23).

Implements:
- Empirical degree distributions (in-degree, out-degree, total degree) from `service_edges`.
- Call concentration and load inequality metrics (Gini coefficient, Lorenz curve, top-k% share)
  for qualitative comparison against Luo et al. (2021) §3.1 and Winchester et al. (2025).
- Discrete / tail-truncated distribution fitting comparing candidate Burr Type XII (Luo Fig. 3a,
  99th-percentile tail truncation) against LogNormal, Exponential, and discrete Power-Law
  using binned discrete log-likelihood, AIC, and discrete Kolmogorov-Smirnov statistic.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import math
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
from scipy import optimize, stats


def compute_degree_distribution(
    edges: Iterable[Mapping[str, Any]],
    exclude_self_loops: bool = True,
) -> list[dict[str, Any]]:
    """Compute per-service in/out/total unweighted degree and weighted call counts."""
    in_deg: dict[str, set[str]] = defaultdict(set)
    out_deg: dict[str, set[str]] = defaultdict(set)
    in_calls: dict[str, int] = defaultdict(int)
    out_calls: dict[str, int] = defaultdict(int)
    nodes: set[str] = set()

    for edge in edges:
        u = str(edge["caller_service"])
        v = str(edge["callee_service"])
        w = int(edge["observed_call_count"])
        if exclude_self_loops and u == v:
            continue
        nodes.add(u)
        nodes.add(v)
        out_deg[u].add(v)
        in_deg[v].add(u)
        out_calls[u] += w
        in_calls[v] += w

    rows: list[dict[str, Any]] = []
    for svc in sorted(nodes):
        ind = len(in_deg[svc])
        outd = len(out_deg[svc])
        rows.append(
            {
                "service_id": svc,
                "in_degree": ind,
                "out_degree": outd,
                "total_degree": ind + outd,
                "in_call_count": in_calls[svc],
                "out_call_count": out_calls[svc],
                "total_incident_calls": in_calls[svc] + out_calls[svc],
            }
        )
    return rows


def compute_load_concentration(
    load_by_service: Mapping[str, float] | Sequence[float],
    top_fractions: Sequence[float] = (0.01, 0.05, 0.10, 0.20),
) -> dict[str, Any]:
    """Compute Gini coefficient, top-k% service load shares, and Lorenz points."""
    if isinstance(load_by_service, Mapping):
        values = np.asarray([float(v) for v in load_by_service.values()], dtype=float)
    else:
        values = np.asarray([float(v) for v in load_by_service], dtype=float)

    values = values[np.isfinite(values) & (values >= 0.0)]
    n = len(values)
    total = float(values.sum()) if n > 0 else 0.0
    if n == 0 or total == 0.0:
        return {
            "service_count": n,
            "total_load": total,
            "gini": 0.0,
            "top_shares": {f"top_{int(round(f * 100))}pct": 0.0 for f in top_fractions},
            "lorenz_x": [0.0, 1.0],
            "lorenz_y": [0.0, 1.0],
        }

    asc = np.sort(values)
    desc = asc[::-1]
    cum_asc = np.cumsum(asc) / total
    lorenz_x = np.concatenate(([0.0], np.arange(1, n + 1) / n)).tolist()
    lorenz_y = np.concatenate(([0.0], cum_asc)).tolist()

    # Standard Gini via sorted values
    idx = np.arange(1, n + 1, dtype=float)
    gini = float((2.0 * np.sum(idx * asc)) / (n * total) - (n + 1.0) / n)

    top_shares: dict[str, float] = {}
    for frac in top_fractions:
        k = max(1, int(math.ceil(frac * n)))
        top_shares[f"top_{int(round(frac * 100))}pct"] = float(desc[:k].sum() / total)

    return {
        "service_count": n,
        "total_load": total,
        "gini": gini,
        "top_shares": top_shares,
        "lorenz_x": lorenz_x,
        "lorenz_y": lorenz_y,
    }


def _discrete_pmf_from_cdf(cdf_fn, support_k: np.ndarray, upper_bound: int) -> np.ndarray:
    """Discretize a continuous positive distribution onto integer support {1, ..., upper_bound}.

    Follows discrete probability mass P(X = k) = (F(k + 0.5) - F(k - 0.5)) / F(upper + 0.5)
    with lower edge clipped at 0 so total truncated mass equals 1.0.
    """
    low_edges = np.maximum(0.0, support_k - 0.5)
    high_edges = support_k + 0.5
    norm = float(cdf_fn(upper_bound + 0.5))
    if not math.isfinite(norm) or norm <= 0.0:
        return np.full_like(support_k, 1e-15, dtype=float)
    probs = (cdf_fn(high_edges) - cdf_fn(low_edges)) / norm
    return np.clip(probs, 1e-15, 1.0)


def fit_discrete_size_distributions(
    sizes: Sequence[int],
    truncate_quantile: float | None = 0.99,
) -> dict[str, Any]:
    """Fit Burr XII and comparator discrete models to integer trace/graph sizes.

    Accounts for integer discreteness via interval PMF `F(k+0.5)-F(k-0.5)` and optionally
    truncates at `truncate_quantile` (default 0.99, matching Luo et al. 2021 Fig. 3a).
    """
    arr = np.asarray([int(x) for x in sizes if int(x) >= 1], dtype=int)
    if len(arr) < 10:
        raise ValueError("Need at least 10 positive integer samples for distribution fitting.")

    raw_max = int(arr.max())
    if truncate_quantile is not None:
        cutoff = int(np.quantile(arr, truncate_quantile))
        sample = arr[arr <= cutoff]
    else:
        cutoff = raw_max
        sample = arr

    counts = Counter(sample.tolist())
    support = np.array(sorted(counts.keys()), dtype=float)
    obs_freq = np.array([counts[int(k)] for k in support], dtype=float)
    n = float(obs_freq.sum())
    obs_ecdf = np.cumsum(obs_freq) / n

    candidates: dict[str, dict[str, Any]] = {}

    def _evaluate_model(
        name: str,
        n_params: int,
        cdf_fn,
        params_dict: dict[str, float],
    ) -> dict[str, Any]:
        pmf = _discrete_pmf_from_cdf(cdf_fn, support, cutoff)
        pmf = pmf / pmf.sum()
        loglik = float(np.sum(obs_freq * np.log(pmf)))
        aic = float(2 * n_params - 2.0 * loglik)
        # Model CDF at support points
        all_k = np.arange(1, cutoff + 1, dtype=float)
        full_pmf = _discrete_pmf_from_cdf(cdf_fn, all_k, cutoff)
        full_pmf = full_pmf / full_pmf.sum()
        model_cdf_all = np.cumsum(full_pmf)
        model_cdf_at_support = model_cdf_all[support.astype(int) - 1]
        ks_stat = float(np.max(np.abs(obs_ecdf - model_cdf_at_support)))
        return {
            "model": name,
            "n_params": n_params,
            "params": params_dict,
            "log_likelihood": loglik,
            "aic": aic,
            "discrete_ks": ks_stat,
        }

    # 1. Burr Type XII (scipy.stats.burr12: c, d, scale)
    def _burr_nll(theta: np.ndarray) -> float:
        c, d, scale = np.exp(theta)
        cdf_fn = lambda x: stats.burr12.cdf(x, c=c, d=d, scale=scale)
        pmf = _discrete_pmf_from_cdf(cdf_fn, support, cutoff)
        pmf = pmf / pmf.sum()
        return -float(np.sum(obs_freq * np.log(pmf)))

    res_burr = optimize.minimize(
        _burr_nll,
        x0=np.log([2.0, 1.0, max(float(np.median(sample)), 1.0)]),
        method="Nelder-Mead",
        options={"maxiter": 600},
    )
    b_c, b_d, b_scale = [float(v) for v in np.exp(res_burr.x)]
    candidates["burr12"] = _evaluate_model(
        "burr12",
        3,
        lambda x: stats.burr12.cdf(x, c=b_c, d=b_d, scale=b_scale),
        {"c": b_c, "k": b_d, "lambda": b_scale},
    )

    # 2. LogNormal (s, scale)
    def _lognorm_nll(theta: np.ndarray) -> float:
        s, scale = np.exp(theta)
        cdf_fn = lambda x: stats.lognorm.cdf(x, s=s, scale=scale)
        pmf = _discrete_pmf_from_cdf(cdf_fn, support, cutoff)
        pmf = pmf / pmf.sum()
        return -float(np.sum(obs_freq * np.log(pmf)))

    res_ln = optimize.minimize(
        _lognorm_nll,
        x0=np.log([1.0, max(float(np.median(sample)), 1.0)]),
        method="Nelder-Mead",
        options={"maxiter": 500},
    )
    ln_s, ln_scale = [float(v) for v in np.exp(res_ln.x)]
    candidates["lognormal"] = _evaluate_model(
        "lognormal",
        2,
        lambda x: stats.lognorm.cdf(x, s=ln_s, scale=ln_scale),
        {"sigma": ln_s, "scale": ln_scale},
    )

    # 3. Exponential (scale)
    def _exp_nll(theta: np.ndarray) -> float:
        scale = float(np.exp(theta[0]))
        cdf_fn = lambda x: stats.expon.cdf(x, scale=scale)
        pmf = _discrete_pmf_from_cdf(cdf_fn, support, cutoff)
        pmf = pmf / pmf.sum()
        return -float(np.sum(obs_freq * np.log(pmf)))

    res_exp = optimize.minimize(
        _exp_nll,
        x0=np.log([max(float(np.mean(sample)), 1.0)]),
        method="Nelder-Mead",
        options={"maxiter": 300},
    )
    ex_scale = float(np.exp(res_exp.x[0]))
    candidates["exponential"] = _evaluate_model(
        "exponential",
        1,
        lambda x: stats.expon.cdf(x, scale=ex_scale),
        {"scale": ex_scale},
    )

    # 4. Discrete Power-Law / Zeta over {1..cutoff}: P(X=k) proportional to k^(-alpha)
    def _pl_nll(theta: np.ndarray) -> float:
        alpha = 1.0 + float(np.exp(theta[0]))
        all_k = np.arange(1, cutoff + 1, dtype=float)
        norm = float(np.sum(all_k ** (-alpha)))
        pmf = np.clip((support ** (-alpha)) / norm, 1e-15, 1.0)
        return -float(np.sum(obs_freq * np.log(pmf)))

    res_pl = optimize.minimize(
        _pl_nll,
        x0=np.array([0.0]),
        method="Nelder-Mead",
        options={"maxiter": 300},
    )
    pl_alpha = 1.0 + float(np.exp(res_pl.x[0]))
    all_k_pl = np.arange(1, cutoff + 1, dtype=float)
    pl_weights = all_k_pl ** (-pl_alpha)
    pl_pmf_all = pl_weights / pl_weights.sum()
    pl_cdf_all = np.cumsum(pl_pmf_all)
    pl_pmf_supp = pl_pmf_all[support.astype(int) - 1]
    pl_ll = float(np.sum(obs_freq * np.log(np.clip(pl_pmf_supp, 1e-15, 1.0))))
    candidates["discrete_powerlaw"] = {
        "model": "discrete_powerlaw",
        "n_params": 1,
        "params": {"alpha": pl_alpha},
        "log_likelihood": pl_ll,
        "aic": float(2.0 - 2.0 * pl_ll),
        "discrete_ks": float(np.max(np.abs(obs_ecdf - pl_cdf_all[support.astype(int) - 1]))),
    }

    ranked = sorted(candidates.values(), key=lambda m: m["aic"])
    return {
        "sample_count": int(n),
        "raw_count": int(len(arr)),
        "truncate_quantile": truncate_quantile,
        "cutoff_value": cutoff,
        "best_by_aic": ranked[0]["model"],
        "models": ranked,
    }
