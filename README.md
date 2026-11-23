# Microservice Dependency Analysis and Latency Prediction with a Data Lakehouse

**IE212 — Big Data Technologies**

**Authors:** 
Nguyen Thuy Vy - 24522071
Tran Ngoc Gia Huy - 24520703

## Overview

This project investigates microservice dependencies, structural service importance, and short-horizon latency prediction using the Alibaba `cluster-trace-microservices-v2021` dataset. A batch-oriented data lakehouse is proposed to integrate call records, service metrics, container resources, and host measurements into reproducible analytical datasets.

The study combines descriptive graph analysis, next-minute mean RPC response-time prediction, and comparative processing benchmarks. Structural centrality is interpreted as an indicator of architectural importance; associations with resource utilization or latency do not establish causal bottlenecks.

## Implementation Status

As of **23 November 2026**, the repository contains the initial directory structure and configuration placeholders. Data acquisition, executable pipelines, infrastructure deployment, model training, and empirical evaluation have not yet been completed. Docker Compose, the dependency list, and YAML files are scaffolding rather than a validated runtime environment.

The following diagram presents the **proposed architecture**. Execution commands and verified dependency versions will be documented after the corresponding stages have been implemented and tested.

## Research Questions

| Question | Objective | Planned evidence |
| --- | --- | --- |
| **RQ1 — Dependency structure and service importance** | Characterize trace-level call graphs and service dependency networks; examine relationships between structural importance, load, resources, and latency. | Trace size and depth; communication types by depth; degree distributions; load concentration; centrality rankings; distribution fitting and comparison with prior studies. |
| **RQ2 — Predictive value of graph and co-location features** | Determine whether graph structure and co-located resource activity improve next-minute mean RPC latency prediction beyond service history. | Persistence and Moving Average baselines; Ridge Regression and Global LightGBM; A/B/C ablation on a common cohort; pooled and per-service MAE and RMSE. |
| **RQ3 — Data processing performance** | Compare Spark, DuckDB, and Pandas on equivalent workloads as input size increases under controlled resource limits. | Runtime, peak memory, measurable spill, result-equivalence checks, repeated measurements, and explicit failure records. |

## Proposed Pipeline

```mermaid
flowchart TB
    subgraph sources["Alibaba microservices v2021"]
        node["Node<br/>Host resources"]
        resource["MSResource<br/>Container resources and mappings"]
        metrics["MSRTQps<br/>Response time and traffic"]
        calls["MSCallGraph<br/>Sampled call records"]
    end

    ingest["Acquisition and provenance<br/>Source version, manifest, checksums<br/>Configured sampling and resource limits"]
    node --> ingest
    resource --> ingest
    metrics --> ingest
    calls --> ingest

    bronze["Bronze — MinIO<br/>Immutable raw inputs<br/>Partitioned Parquet and file lineage"]
    silver["Silver — Iceberg tables on MinIO<br/>Schema and unit normalization; quality flags<br/>Caller/callee separation and deduplication<br/>Trace validation, time alignment, and entity joins"]
    gold["Gold — Iceberg tables on MinIO<br/>Trace statistics and service edges<br/>Service-minute metrics and next-minute labels"]
    ingest --> bronze
    bronze -->|"PySpark ETL"| silver
    silver -->|"PySpark aggregation"| gold

    fullgraph["Descriptive graph<br/>Full observation window"]
    traingraph["Predictive graph<br/>Training data only"]
    features["Feature and label dataset<br/>A: service history<br/>B: A + graph structure<br/>C: B + co-location activity"]
    gold --> fullgraph
    gold --> traingraph
    gold --> features
    traingraph -->|"Available at forecast origin"| features

    rq1["RQ1 — Structural analysis<br/>Trace statistics and centrality<br/>Load concentration and distribution fitting"]
    rq2["RQ2 — Latency prediction<br/>Temporal split and common A/B/C cohort<br/>Baselines, Ridge, LightGBM, and ablation"]
    benchinput["Fixed benchmark inputs<br/>Metrics aggregation; resource joins<br/>Service-edge aggregation"]
    rq3["RQ3 — Engine comparison<br/>Spark / DuckDB / Pandas<br/>Equivalent outputs and repeated measurements"]
    fullgraph --> rq1
    gold --> rq1
    features --> rq2
    bronze --> benchinput
    silver --> benchinput
    benchinput --> rq3

    outputs["Research outputs<br/>Quality reports, tables, figures, models, and predictions<br/>Dashboard, final report, and reproducibility documentation"]
    rq1 --> outputs
    rq2 --> outputs
    rq3 --> outputs
```

MinIO provides object storage, Parquet provides the columnar file representation, and Apache Iceberg manages Silver and Gold tables through a catalog. PySpark is the primary ETL engine. GraphFrames or NetworKit will be selected according to algorithm support and the available memory budget. An initial Spark deployment will operate locally; distributed scalability claims require separate empirical evidence.

RQ3 evaluates transformations on fixed, documented inputs. Any conversion required for engine interoperability will be documented, together with whether its cost is included in the measured workload.

## Data and Experimental Methodology

### Data preparation

The four input groups differ in entity coverage and measurement characteristics. Acquisition will record source versions, file sizes, checksums, row counts, and observation intervals. Validation will address missing and sentinel values, duplicates, conflicting keys, measurement units, and sampling intervals.

When sampling is required, a deterministic hash of `traceid` will be used and related records will be collected across files or partitions. Call processing will distinguish caller-side and callee-side observations before deduplication or response-time normalization. Quality reports will disclose exclusions, incomplete traces, join coverage, and unexpected row multiplication.

### Latency prediction

A prediction sample represents a service at forecast-origin minute `t`; its target is mean incoming RPC response time in minute `t + 1`. Container-level `providerRPC_RT` values will be aggregated using `providerRPC_MCR` weights only after measurement-window compatibility has been verified. Missing targets and zero-traffic denominators will remain explicitly unavailable.

| Configuration | Information available at the forecast origin |
| --- | --- |
| **A — Service history** | Historical latency, traffic, service resources, lagged values, and rolling statistics. |
| **B — Graph structure** | All features in A, supplemented by structural descriptors from a graph constructed exclusively from training data. |
| **C — Co-location activity** | All features in B, supplemented by host load and observed activity of other containers sharing the host. |

Train, Validation, and Test will be separated chronologically by target timestamp, with an initial proposed ratio of **70/15/15**. Preprocessing will be fitted on Train, model selection will use Validation, and Test will be reserved for final evaluation. A/B/C comparisons will use the same services and timestamps. Graph snapshots and all other features must be available at the forecast origin.

The full-window graph used for descriptive RQ1 analysis will be maintained separately from the training-only graph used for RQ2. Persistence and Moving Average will provide reference forecasts before Ridge Regression and Global LightGBM are evaluated. RQ1 centrality measures comprise Degree, PageRank, Approximate Betweenness, and K-core, with algorithm settings and graph conventions documented.

### Processing benchmarks

The workloads comprise service-minute metric aggregation, joins between metrics and container/host resources, and service-edge aggregation. Each engine will process **10%, 25%, 50%, and 100% of the declared benchmark input**, subject to feasible resource limits. These percentages do not imply coverage of the entire Alibaba dataset.

Output equivalence will be checked before performance comparisons. Each configuration will be executed at least three times, with resource limits, cache policy, materialization, and measurement boundaries documented. Runtime, peak memory, and measurable spill will be retained alongside out-of-memory, timeout, and unsupported-operation outcomes. A separate experiment will consider 1/2/4/8 cores where hardware and engine capabilities permit meaningful parallel execution.

### Scope and limitations

The study concerns retrospective batch analysis within the observed dataset. Streaming systems, deployment optimization, intervention-based causal analysis, and deep learning models are outside the current scope. Generalization beyond the observed period or system requires additional evidence.

P95 prediction and latency-threshold alerts are conditional extensions. P95 would be derived from individual observed calls, subject to sample-count, coverage, and stability criteria; it will not be inferred from container-level mean response times. Alert thresholds will be described as experimental thresholds or hypothetical SLOs.

## Repository Structure

```text
ie212-microservices/
├── README.md
├── .gitignore
├── .env.example
├── docker-compose.yml
├── requirements.txt
├── configs/
│   ├── data.yaml
│   ├── lakehouse.yaml
│   ├── features.yaml
│   ├── experiments.yaml
│   └── benchmarks.yaml
├── src/ie212/
│   ├── ingestion/
│   ├── quality/
│   ├── silver/
│   ├── gold/
│   ├── graphs/
│   ├── features/
│   ├── models/
│   ├── evaluation/
│   └── benchmarks/
├── scripts/
├── tests/
├── notebooks/
├── dashboard/
├── artifacts/
│   ├── manifest/
│   ├── models/
│   ├── predictions/
│   └── benchmarks/
├── reports/
│   ├── data_quality/
│   ├── figures/
│   └── tables/
├── docs/
│   ├── data_dictionary.md
│   ├── decisions.md
│   ├── resources.md
│   ├── sampling_plan.md
│   ├── split_cohort.md
│   └── benchmark_design.md
└── data/
    ├── README.md
    ├── raw/
    └── sample/
```

Reusable implementation belongs in `src/ie212/`, stage entrypoints in `scripts/`, and exploratory work in `notebooks/`. Configuration files define paths, sampling, resource limits, feature definitions, and experimental parameters. Generated artifacts will be associated with run identifiers. Empty directories are retained with `.gitkeep` files, and Python packages contain `__init__.py` files.

## Documentation and Reproducibility

The authoritative project specification is maintained outside the repository at `D:\IE212\Project\context\metadata.txt`. Implementation guidance is currently located at `D:\IE212\Project\context\agents_ie212.md`. Contributors must consult both documents before implementation. The external context directory is not synchronized by GitHub; contributors on other machines must obtain the corresponding materials. Copies of the authoritative metadata and the planning/structure workbooks must not be added to the repository.

Technical documentation is maintained in a flat `docs/` directory. The team will supply its existing LaTeX report as `docs/main.tex`; this file is not yet present. Analytical figures and tables belong in `reports/figures/` and `reports/tables/`, referenced through `../reports/figures/` and `../reports/tables/`. Compilation instructions will be added after the report template and required engine have been verified.

Reproducibility records will include data manifests, seeds, sampling scope, environment versions, resource budgets, split boundaries, model parameters, graph snapshots, and benchmark conditions. Results will be reported as measured, including cases where additional features do not improve prediction or a single-machine engine outperforms Spark.

## Collaboration

Development takes place in a shared repository using `feature/<task>` branches. Each change is submitted through a pull request and reviewed by the other team member before merging. Raw data, credentials, and large generated artifacts must remain outside version control. Weekly planning and repository-structure workbooks are maintained in the external workspace context directory.
