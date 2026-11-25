"""Service dependency centrality computation using NetworKit (T22).

Computes all four required structural measures on a service graph snapshot
(`full_window` for RQ1 or `train_only` for RQ2):
- Directed `in_degree`, `out_degree`, `total_degree` (self-loops excluded by default)
- `pagerank` (directed graph, configurable damping/tolerance/weighted)
- `approx_betweenness` (`nk.centrality.EstimateBetweenness` with fixed `seed` and `n_samples`)
- `kcore` (`nk.centrality.CoreDecomposition` on simple undirected projection without self-loops)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import networkit as nk

from ie212.gold.service_edges import project_service_graph


@dataclass(frozen=True)
class CentralityConfig:
    """Parameters and seed recorded alongside every centrality table."""

    snapshot_name: str = "full_window"
    exclude_self_loops: bool = True
    pagerank_damping: float = 0.85
    pagerank_tol: float = 1e-6
    pagerank_weighted: bool = True
    betweenness_n_samples: int = 256
    betweenness_normalized: bool = True
    seed: int = 1217


def compute_service_centrality(
    service_edges: Iterable[Mapping[str, Any]],
    config: CentralityConfig | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Compute Degree, PageRank, Approximate Betweenness, and K-core via NetworKit."""
    cfg = config or CentralityConfig()
    edge_list = list(service_edges)

    directed_edges = project_service_graph(
        edge_list,
        directed=True,
        exclude_self_loops=cfg.exclude_self_loops,
    )
    undirected_edges = project_service_graph(
        edge_list,
        directed=False,
        exclude_self_loops=cfg.exclude_self_loops,
    )

    nodes = sorted({e["u"] for e in directed_edges} | {e["v"] for e in directed_edges})
    node_to_idx = {svc: i for i, svc in enumerate(nodes)}
    n = len(nodes)

    if n == 0:
        return [], {
            "snapshot_name": cfg.snapshot_name,
            "node_count": 0,
            "directed_edge_count": 0,
            "undirected_edge_count": 0,
            "exclude_self_loops": cfg.exclude_self_loops,
            "seed": cfg.seed,
        }

    nk.setSeed(cfg.seed, False)

    g_dir = nk.Graph(n, weighted=cfg.pagerank_weighted, directed=True)
    g_dir_unweighted = nk.Graph(n, weighted=False, directed=True)
    for e in directed_edges:
        u_i = node_to_idx[e["u"]]
        v_i = node_to_idx[e["v"]]
        if cfg.pagerank_weighted:
            g_dir.addEdge(u_i, v_i, float(e["observed_call_count"]))
        else:
            g_dir.addEdge(u_i, v_i)
        g_dir_unweighted.addEdge(u_i, v_i)

    g_undir = nk.Graph(n, weighted=False, directed=False)
    for e in undirected_edges:
        u_i = node_to_idx[e["u"]]
        v_i = node_to_idx[e["v"]]
        g_undir.addEdge(u_i, v_i)

    pr_scores = nk.centrality.PageRank(
        g_dir, damp=cfg.pagerank_damping, tol=cfg.pagerank_tol
    ).run().scores()

    eb_samples = min(cfg.betweenness_n_samples, n)
    eb_scores = nk.centrality.EstimateBetweenness(
        g_dir_unweighted,
        nSamples=eb_samples,
        normalized=cfg.betweenness_normalized,
    ).run().scores()

    kcore_scores = nk.centrality.CoreDecomposition(g_undir).run().scores()

    rows: list[dict[str, Any]] = []
    for svc, idx in node_to_idx.items():
        in_deg = int(g_dir_unweighted.degreeIn(idx))
        out_deg = int(g_dir_unweighted.degreeOut(idx))
        rows.append(
            {
                "snapshot_name": cfg.snapshot_name,
                "service_id": svc,
                "in_degree": in_deg,
                "out_degree": out_deg,
                "total_degree": in_deg + out_deg,
                "undirected_degree": int(g_undir.degree(idx)),
                "pagerank": float(pr_scores[idx]),
                "approx_betweenness": float(eb_scores[idx]),
                "kcore": int(round(kcore_scores[idx])),
            }
        )

    metadata = {
        "snapshot_name": cfg.snapshot_name,
        "node_count": n,
        "directed_edge_count": len(directed_edges),
        "undirected_edge_count": len(undirected_edges),
        "exclude_self_loops": cfg.exclude_self_loops,
        "pagerank_damping": cfg.pagerank_damping,
        "pagerank_tol": cfg.pagerank_tol,
        "pagerank_weighted": cfg.pagerank_weighted,
        "betweenness_n_samples_requested": cfg.betweenness_n_samples,
        "betweenness_n_samples_used": eb_samples,
        "betweenness_normalized": cfg.betweenness_normalized,
        "betweenness_weighted": False,
        "kcore_projection": "simple_undirected",
        "seed": cfg.seed,
    }
    return rows, metadata
