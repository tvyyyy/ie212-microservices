"""Gold service dependency edge builder and graph projections (T18).

Locked conventions:
- Directed edge `caller_service -> callee_service` weighted by `observed_call_count`
  and broken down by `rpctype`.
- Sentinel/null endpoints are excluded from canonical edges (never collapsed into one vertex).
- Self-loops (`caller_service == callee_service`) are tagged `is_self_loop=True` and
  explicitly filtered out before default centrality / K-core projections.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


def build_service_edges(
    canonical_calls: Iterable[Mapping[str, Any]],
    exclude_sentinel_endpoints: bool = True,
    include_self_loops: bool = True,
) -> list[dict[str, Any]]:
    """Aggregate canonical calls into directed `(caller_service, callee_service, rpctype)` edges."""
    edge_counts: dict[tuple[str, str, str], int] = defaultdict(int)

    for call in canonical_calls:
        um = call.get("um")
        dm = call.get("dm")
        if exclude_sentinel_endpoints and (um is None or dm is None):
            continue
        caller = str(um)
        callee = str(dm)
        if not include_self_loops and caller == callee:
            continue
        rpctype = str(call["rpctype"])
        edge_counts[(caller, callee, rpctype)] += 1

    edges: list[dict[str, Any]] = []
    for (caller, callee, rpctype), count in sorted(edge_counts.items()):
        edges.append(
            {
                "caller_service": caller,
                "callee_service": callee,
                "rpctype": rpctype,
                "observed_call_count": count,
                "is_self_loop": caller == callee,
            }
        )
    return edges


def project_service_graph(
    edges: Iterable[Mapping[str, Any]],
    directed: bool = True,
    exclude_self_loops: bool = True,
    allowed_rpctypes: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Project `service_edges` into a simple directed or undirected graph for centrality."""
    allowed = set(allowed_rpctypes) if allowed_rpctypes is not None else None
    projected_weights: dict[tuple[str, str], int] = defaultdict(int)

    for edge in edges:
        if allowed is not None and str(edge["rpctype"]) not in allowed:
            continue
        u = str(edge["caller_service"])
        v = str(edge["callee_service"])
        if exclude_self_loops and u == v:
            continue
        key = (u, v) if directed else (min(u, v), max(u, v))
        projected_weights[key] += int(edge["observed_call_count"])

    return [
        {
            "u": u,
            "v": v,
            "directed": directed,
            "observed_call_count": weight,
        }
        for (u, v), weight in sorted(projected_weights.items())
    ]
