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
