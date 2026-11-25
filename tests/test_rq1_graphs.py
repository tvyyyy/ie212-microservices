"""Hand-calculated synthetic fixtures for RQ1 graph analysis & NetworKit centrality (T21, T22, T23)."""

import numpy as np

from ie212.graphs.analysis import (
    compute_degree_distribution,
    compute_load_concentration,
    fit_discrete_size_distributions,
)
from ie212.graphs.centrality import CentralityConfig, compute_service_centrality


def test_degree_distribution_and_load_concentration():
    edges = [
        {"caller_service": "gw", "callee_service": "a", "rpctype": "http", "observed_call_count": 90},
        {"caller_service": "gw", "callee_service": "b", "rpctype": "rpc", "observed_call_count": 8},
        {"caller_service": "a", "callee_service": "c", "rpctype": "rpc", "observed_call_count": 2},
        # Self-loop should be excluded by default in degree distribution
        {"caller_service": "a", "callee_service": "a", "rpctype": "userDefined", "observed_call_count": 50},
    ]
    deg_rows = compute_degree_distribution(edges, exclude_self_loops=True)
    by_svc = {r["service_id"]: r for r in deg_rows}
    assert by_svc["gw"]["out_degree"] == 2
    assert by_svc["a"]["in_degree"] == 1
    assert by_svc["a"]["out_degree"] == 1

    conc = compute_load_concentration(
        {"s1": 95.0, "s2": 2.0, "s3": 1.0, "s4": 1.0, "s5": 1.0},
        top_fractions=(0.20,),
    )
    assert conc["service_count"] == 5
    assert conc["top_shares"]["top_20pct"] == 0.95
    assert conc["gini"] > 0.7


def test_networkit_centrality_reproducible_and_excludes_self_loops():
    edges = [
        {"caller_service": "gw", "callee_service": "a", "rpctype": "http", "observed_call_count": 10},
        {"caller_service": "a", "callee_service": "b", "rpctype": "rpc", "observed_call_count": 5},
        {"caller_service": "b", "callee_service": "c", "rpctype": "rpc", "observed_call_count": 5},
        {"caller_service": "c", "callee_service": "a", "rpctype": "rpc", "observed_call_count": 5},
        # Self-loop must not artificially inflate degree or K-core
        {"caller_service": "gw", "callee_service": "gw", "rpctype": "userDefined", "observed_call_count": 100},
    ]
    cfg = CentralityConfig(snapshot_name="full_window", betweenness_n_samples=4, seed=1217)
    rows1, meta1 = compute_service_centrality(edges, cfg)
    rows2, _ = compute_service_centrality(edges, cfg)

    assert rows1 == rows2  # deterministic seed
    assert meta1["node_count"] == 4
    by_svc = {r["service_id"]: r for r in rows1}
    # Triangle a-b-c has K-core 2; leaf gw attached to a has K-core 1
    assert by_svc["a"]["kcore"] == 2
    assert by_svc["b"]["kcore"] == 2
    assert by_svc["c"]["kcore"] == 2
    assert by_svc["gw"]["kcore"] == 1
    assert by_svc["gw"]["out_degree"] == 1


def test_discrete_size_distribution_fitting_runs_and_ranks_models():
    rng = np.random.default_rng(1217)
    # Synthetic heavy-tailed integer trace sizes >= 1
    sizes = np.clip(np.round(rng.lognormal(mean=1.5, sigma=0.8, size=300)), 1, None).astype(int)
    fit_res = fit_discrete_size_distributions(sizes.tolist(), truncate_quantile=0.99)
    assert fit_res["sample_count"] > 200
    model_names = {m["model"] for m in fit_res["models"]}
    assert model_names == {"burr12", "lognormal", "exponential", "discrete_powerlaw"}
    assert fit_res["best_by_aic"] in model_names
