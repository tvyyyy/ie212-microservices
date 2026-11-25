# Data Dictionary

Initial live header and cadence verification was performed via HTTP Range sampling on Alibaba OSS on **25 November 2026**; full-file null rates, sentinel percentages, and entity join coverage will be finalized during `P0` (`T02`, `T04`, `T06`).

## Live-Verified Raw Schemas (Alibaba `cluster-trace-microservices-v2021`)

All four raw CSV tables contain a leading unnamed index column (`,...`) that must be dropped or normalized during Bronze ingestion.

| Table | Verified CSV Header | Verified Cadence & Sharding | Key Anomalies to Handle in Silver |
| --- | --- | --- | --- |
| `Node` (`Node_0.tar.gz`) | `,timestamp,nodeid,node_cpu_usage,node_memory_usage` | **30s** (`ts % 30000 == 0`), unsorted | Bare-metal capacity undisclosed (Issue #119); aggregate 30s→60s. |
| `MSResource` (`MSResource_{0..11}.tar.gz`) | `,msname,msinstanceid,nodeid,instance_cpu_usage,instance_memory_usage,timestamp` | **30s** (`ts % 30000 == 0`, differs from README), time-sharded | `timestamp` is the **last column**; container quota denominator incompatible with `Node` (do not subtract); aggregate 30s→60s before joining `MSRTQps`. |
| `MSRTQps` (`MSRTQps_{0..24}.tar.gz`) | `,timestamp,msname,msinstanceid,metric,value` | **60s** (`ts % 60000 == 0`), 30-min time slices | Singular `metric`; rows grouped in metric blocks (no `head(N)` sampling); `HTTP_MCR == HTTP_RT` bug (Issue #120); units: `*_MCR` = calls/s, `*_RT` = ms. |
| `MSCallGraph` (OSS `_0..144` or CASPER Shuffled `doi:10.7910/DVN/RXIC9Z`) | `,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt` | Millisecond event timestamps; 5-min slices (OSS) or trace-hashed partitions (CASPER) | 6 `rpctype` (`rpc`, `mc`, `http`, `userDefined`, `db`, `mq`); `mq` has majority negative RT; ~12% self-loops; ~17% sentinels (`(?)`, `''`); `(traceid, rpcid)` multiplicity >2; ~3% MS overlap with `MSResource`/`MSRTQps`. |

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).

## Column Contract — Proposed Until Full-File Validation

Headers above were inspected in limited OSS samples. Types and candidate keys below are ingestion contracts, not evidence of full-file uniqueness. Raw source file and row offsets must be retained; the unnamed index is not a global key. Nullability, ranges, conflicts, and coverage remain P0 checks.

| Table | Row grain | Candidate key to validate |
| --- | --- | --- |
| Node | One host measurement at a timestamp | `(nodeid, timestamp)` |
| MSResource | One container resource measurement at a timestamp | `(msname, msinstanceid, timestamp)`; validate host mapping separately |
| MSRTQps | One metric for one container and measurement window | `(msname, msinstanceid, timestamp, metric)` |
| MSCallGraph | One observation of a call occurrence, potentially recorded on both sides | No unique key established; `(traceid, rpcid)` can have multiplicity >2 |

| Table | Column(s) | Proposed type | Unit / meaning |
| --- | --- | --- | --- |
| All | unnamed leading index | string in raw staging | Source-local row index; retain as lineage or drop after recording source row offset |
| All | timestamp | int64 | Milliseconds relative to the observation window, not a Unix timestamp; window start/end semantics require validation |
| Node, MSResource | nodeid | string | Opaque host identifier; join only after key/time alignment |
| Node | node_cpu_usage, node_memory_usage | float64 | Reported host utilization ratios; bare-metal capacity/normalization is not established |
| MSResource, MSRTQps | msname | string | Opaque service identifier |
| MSResource, MSRTQps | msinstanceid | string | Opaque container instance identifier |
| MSResource | instance_cpu_usage, instance_memory_usage | float64 | Reported container utilization; quota/capacity denominator is not established and is not interchangeable with Node utilization |
| MSRTQps | metric | string enum | One of the ten source metrics below |
| MSRTQps | value | float64 | Calls/s for MCR; mean milliseconds for RT; missing and zero traffic must not become valid zero-latency labels |
| MSCallGraph | traceid | string | Trace identifier; keep all sampled trace records |
| MSCallGraph | rpcid | string | Hierarchical call identifier within a trace; preserve dotted structure and repeated observations |
| MSCallGraph | um, dm | string, nullable after normalization | Upstream/caller and downstream/callee service identifiers; retain sentinel flags |
| MSCallGraph | rpctype | string enum | `rpc`, `mc`, `http`, `userDefined`, `db`, `mq` |
| MSCallGraph | interface | string, nullable | Downstream interface identifier where available |
| MSCallGraph | rt | signed numeric | Milliseconds; retain sign and zero; apply communication-specific policy in decisions.md |

### MSRTQps Metric Vocabulary

This vocabulary follows the upstream Alibaba documentation; complete-file presence and validity remain to be checked.

| Metric | Unit | Meaning for the measured service |
| --- | --- | --- |
| consumerRPC_MCR | calls/s | Outgoing RPC call rate |
| providerRPC_MCR | calls/s | Incoming RPC call rate |
| HTTP_MCR | calls/s | HTTP call rate |
| providerMQ_MCR | calls/s | Message sending rate |
| consumerMQ_MCR | calls/s | Message fetching rate |
| consumerRPC_RT | ms | Mean outgoing RPC response time |
| providerRPC_RT | ms | Mean incoming RPC response time; primary target input |
| HTTP_RT | ms | Mean HTTP response time; reported equality issue with HTTP_MCR requires validation |
| providerMQ_RT | ms | Mean response time for the message-provider operation; exact window semantics require validation |
| consumerMQ_RT | ms | Mean response time for the message-consumer operation; exact window semantics require validation |

Node and MSResource must first be aggregated from 30s to 60s at their own entity grain before joining MSRTQps. Record window boundaries, aggregation functions, missing half-windows, and join multiplicity during implementation. Never subtract summed container CPU from host CPU.

Upstream vocabulary and units: [Alibaba v2021 README](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-microservices-v2021/README.md). Raw header corrections above come from limited OSS inspection, rather than assuming the upstream README exactly matches raw files.
