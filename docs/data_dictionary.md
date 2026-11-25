# Data Dictionary

Initial live header and cadence verification was performed via HTTP Range sampling on Alibaba OSS on **25 November 2026**; sample-level null/sentinel, duplicate, cadence, and join diagnostics are now measured in `reports/data_quality/t06_sample_profile.json`. Whole-dataset validation remains separate.

## Live-Verified Raw Schemas (Alibaba `cluster-trace-microservices-v2021`)

The four Alibaba OSS raw CSV tables (`Node`, `MSResource`, `MSRTQps`, and OSS `MSCallGraph`) contain a leading unnamed index column (`,...`) that must be dropped or normalized during Bronze ingestion. CASPER Shuffled v2021 has exactly **8 columns**, beginning directly with `traceid`, and **no leading unnamed index column**.

| Table | Verified CSV Header | Verified Cadence & Sharding | Key Anomalies to Handle in Silver |
| --- | --- | --- | --- |
| `Node` (`Node_0.tar.gz`) | `,timestamp,nodeid,node_cpu_usage,node_memory_usage` | **30s** (`ts % 30000 == 0`), unsorted | Bare-metal capacity undisclosed (Issue #119); aggregate 30s→60s. |
| `MSResource` (`MSResource_{0..11}.tar.gz`) | `,msname,msinstanceid,nodeid,instance_cpu_usage,instance_memory_usage,timestamp` | **30s** (`ts % 30000 == 0`, differs from README), time-sharded | `timestamp` is the **last column**; container quota denominator incompatible with `Node` (do not subtract); aggregate 30s→60s before joining `MSRTQps`. |
| `MSRTQps` (`MSRTQps_{0..24}.tar.gz`) | `,timestamp,msname,msinstanceid,metric,value` | **60s** (`ts % 60000 == 0`), 30-min time slices | Singular `metric`; rows grouped in metric blocks (no `head(N)` sampling); `HTTP_MCR == HTTP_RT` bug (Issue #120); units: `*_MCR` = calls/s, `*_RT` = ms. |
| `MSCallGraph` (Alibaba OSS `_0..144`) | `,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt` | Millisecond event timestamps; 5-min slices | Signed/zero RT, repeated `(traceid, rpcid)`, sentinel endpoints, and self-loops require source-scoped handling. Current T06 anomaly counts are measured on CASPER, not this OSS archive. |
| `MSCallGraph` (CASPER Shuffled v2021 `doi:10.7910/DVN/RXIC9Z`) | `traceid,timestamp,rpcid,um,rpctype,dm,interface,rt` | Millisecond event timestamps; trace-hashed partitions | 8 columns; no unnamed index. See the T06 CASPER profile for measured signs, duplicates, sentinels, and join coverage. |

For raw `MSCallGraph`, the zero-based `timestamp` column index is **2 for Alibaba OSS** and **1 for CASPER**. Select it by header name where possible; never drop CASPER's first column as an index. The CASPER header was checked in sample `partition_id=-10.csv.gz`, as recorded in the PR #2 review follow-up.

Sources: External workspace context documents (`metadata.txt` and `agents_ie212.md`, maintained outside the repository).

## Column Contract — Proposed Until Full-File Validation

Headers above were inspected in limited OSS samples and the CASPER sample identified above. Types and candidate keys below are ingestion contracts, not evidence of full-file uniqueness. Raw source file and row offsets must be retained; the unnamed index is not a global key. Nullability, ranges, conflicts, and coverage remain P0 checks.

| Table | Row grain | Candidate key to validate |
| --- | --- | --- |
| Node | One host measurement at a timestamp | `(nodeid, timestamp)` |
| MSResource | One container resource measurement at a timestamp | `(msname, msinstanceid, timestamp)`; validate host mapping separately |
| MSRTQps | One metric for one container and measurement window | `(msname, msinstanceid, timestamp, metric)` |
| MSCallGraph | One observation of a call occurrence, potentially recorded on both sides | No unique key established; `(traceid, rpcid)` can have multiplicity >2 |

| Table | Column(s) | Proposed type | Unit / meaning |
| --- | --- | --- | --- |
| Alibaba OSS tables only (not CASPER) | unnamed leading index | string in raw staging | Source-local row index; retain as lineage or drop after recording source row offset |
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

Upstream vocabulary and units: [Alibaba v2021 README](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-microservices-v2021/README.md). Raw header corrections above come from limited OSS inspection and the CASPER sample follow-up, rather than assuming the upstream README exactly matches raw files.


## T02 — Official README Crosscheck (26 November 2026)

Crosschecked the official Alibaba v2021 README at commit [`2c9879e`](https://github.com/alibaba/clusterdata/blob/2c9879e690f478d761a76e4777fcbb451bfd10cc/cluster-trace-microservices-v2021/README.md) against every header and all timestamps in the declared local samples. The source prose and examples are a semantic reference; actual CSV layout is the ingestion contract.

| Topic | README vs observed sample | Adopted contract |
|---|---|---|
| Leading column | README omits the index; OSS samples have one; CASPER has none | Drop only a verified unnamed OSS index |
| Node names | Generic utilization names vs `node_cpu_usage`, `node_memory_usage` | Retain CSV names |
| Resource layout | Timestamp shown first vs last; actual utilization names have `instance_` prefix | Resolve by header |
| Resource cadence | README says 60s; all 2,023,518 sample rows lie on 30s grid | Native 30s, explicit flagged minute projection |
| RT/QPS field | `metrics` vs actual `metric` | Singular field and ten-name vocabulary |
| Call column order | Timestamp/trace and interface/dm ordering differ | Source-specific header validation |
| Communication types | Uppercase DB/MC in prose vs six observed categories | Preserve observed `db, http, mc, mq, rpc, userDefined` |
| RT sign / units | Signed RPC/HTTP observations; RT milliseconds, MCR calls/s | Preserve side/sign and zero; no global absolute value |

The machine-readable comparison is [t02_schema_crosscheck.json](../reports/data_quality/t02_schema_crosscheck.json), with observed-file hashes in [T06 evidence](../reports/data_quality/t06_sample_profile.json). Header and cadence agreement does not prove full-file key uniqueness or precise RT/MCR measurement-window boundaries.

Maintainer clarifications: [Issue 119](https://github.com/alibaba/clusterdata/issues/119#issuecomment-1020778808) states that node/container capacities are not released, so utilization denominators cannot be interchanged. [Issue 120](https://github.com/alibaba/clusterdata/issues/120#issuecomment-1027853173) confirms the HTTP metric equality bug. The local sample reproduces equality on all 246,620 paired keys.

CASPER anomaly percentages must use the current sample denominators. In particular, negative MQ RT is 37,288/81,659 (45.66%), not a majority here. Earlier OSS inspection estimates are not retained as measurements of the current CASPER scope; use the declared T06 denominators instead. See [measured T06 findings](../reports/data_quality/t06_sample_profile.md) before applying cleaning rules.

A fresh header-only OSS check on 26 November 2026 used HTTP Range (206) to read at most 131,072 compressed bytes from `MSCallGraph_0.tar.gz`. It confirmed the leading unnamed index and `,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt`, separately from CASPER. The URL, timestamp, header, and explicit prefix-only scope are recorded in the T02 JSON; no whole-file anomalies were inferred from this check.
