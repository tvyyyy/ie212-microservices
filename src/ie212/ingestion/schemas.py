"""Dataset schemas, header normalization, and CASPER partition selection for Bronze ingestion."""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa


class BronzeError(Exception):
    """Base exception for Bronze ingestion errors."""


class BronzeSchemaError(BronzeError, ValueError):
    """Raised when a CSV header or dataset schema violates expected structure."""


PROVENANCE_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("source_file", pa.string()),
    ("source_row_offset", pa.int64()),
    ("source_uri", pa.string()),
    ("source_sha256", pa.string()),
    ("source_md5", pa.string()),
    ("ingested_at_utc", pa.string()),
)

DEFAULT_CASPER_PARTITION_MIN = -999
DEFAULT_CASPER_PARTITION_MAX = 999
DEFAULT_CASPER_TOTAL_PARTITIONS = 1999

COLUMN_TYPE_MAP: dict[str, pa.DataType] = {
    "traceid": pa.string(),
    "timestamp": pa.int64(),
    "rpcid": pa.string(),
    "um": pa.string(),
    "rpctype": pa.string(),
    "dm": pa.string(),
    "interface": pa.string(),
    "rt": pa.float64(),
    "nodeid": pa.string(),
    "node_cpu_usage": pa.float64(),
    "node_memory_usage": pa.float64(),
    "msname": pa.string(),
    "msinstanceid": pa.string(),
    "instance_cpu_usage": pa.float64(),
    "instance_memory_usage": pa.float64(),
    "metric": pa.string(),
    "value": pa.float64(),
}

DATASET_TO_LAKEHOUSE_TABLE: dict[str, str] = {
    "casper_mscallgraph": "ms_call_graph",
    "oss_mscallgraph": "ms_call_graph",
    "oss_node": "node",
    "oss_msresource": "ms_resource",
    "oss_msrtqps": "ms_rt_qps",
}

DEFAULT_SOURCES_CONFIG: dict[str, dict[str, str]] = {
    "casper_dataverse": {
        "doi": "10.7910/DVN/RXIC9Z",
        "dataset_url": "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/RXIC9Z",
    },
    "alibaba_oss": {
        "base_url": "http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2021MicroservicesTraces",
    },
}

DEFAULT_DATASETS_CONFIG: dict[str, dict[str, Any]] = {
    "casper_mscallgraph": {
        "source": "casper_dataverse",
        "file_pattern": "partition_id=*.csv.gz",
        "total_partitions": DEFAULT_CASPER_TOTAL_PARTITIONS,
        "partition_min": DEFAULT_CASPER_PARTITION_MIN,
        "partition_max": DEFAULT_CASPER_PARTITION_MAX,
        "columns": [
            "traceid",
            "timestamp",
            "rpcid",
            "um",
            "rpctype",
            "dm",
            "interface",
            "rt",
        ],
        "has_leading_index": False,
        "timestamp_column": "timestamp",
    },
    "oss_node": {
        "source": "alibaba_oss",
        "archive_pattern": "node/Node_{index}.tar.gz",
        "indices": [0],
        "columns": [
            "timestamp",
            "nodeid",
            "node_cpu_usage",
            "node_memory_usage",
        ],
        "has_leading_index": True,
        "timestamp_column": "timestamp",
    },
    "oss_msresource": {
        "source": "alibaba_oss",
        "archive_pattern": "MSResource/MSResource_{index}.tar.gz",
        "indices": list(range(12)),
        "columns": [
            "msname",
            "msinstanceid",
            "nodeid",
            "instance_cpu_usage",
            "instance_memory_usage",
            "timestamp",
        ],
        "has_leading_index": True,
        "timestamp_column": "timestamp",
    },
    "oss_msrtqps": {
        "source": "alibaba_oss",
        "archive_pattern": "MSRTQps/MSRTQps_{index}.tar.gz",
        "indices": list(range(25)),
        "columns": [
            "timestamp",
            "msname",
            "msinstanceid",
            "metric",
            "value",
        ],
        "has_leading_index": True,
        "timestamp_column": "timestamp",
    },
    "oss_mscallgraph": {
        "source": "alibaba_oss",
        "archive_pattern": "MSCallGraph/MSCallGraph_{index}.tar.gz",
        "index_start": 0,
        "index_end": 144,
        "columns": [
            "traceid",
            "timestamp",
            "rpcid",
            "um",
            "rpctype",
            "dm",
            "interface",
            "rt",
        ],
        "has_leading_index": True,
        "timestamp_column": "timestamp",
    },
}


@dataclass(frozen=True)
class DatasetSchema:
    """Authoritative schema metadata for an IE212 Bronze dataset."""

    name: str
    source: str
    columns: tuple[str, ...]
    has_leading_index: bool
    timestamp_column: str
    column_types: dict[str, pa.DataType]
    arrow_schema: pa.Schema
    doi_or_url: str
    lakehouse_table: str
    s3_table_uri: str | None = None
    archive_pattern: str | None = None
    indices: tuple[int, ...] = ()
    partition_min: int | None = None
    partition_max: int | None = None


@dataclass(frozen=True)
class HeaderNormalization:
    """Result of validating and normalizing a raw CSV header."""

    columns: tuple[str, ...]
    dropped_leading_index: bool
    selected_indices: tuple[int, ...]


def _resolve_arrow_type(column_name: str) -> pa.DataType:
    if column_name in COLUMN_TYPE_MAP:
        return COLUMN_TYPE_MAP[column_name]
    raise BronzeSchemaError(f"Unknown dataset column '{column_name}' with no Arrow type mapping.")


def _extract_indices(ds_cfg: Mapping[str, Any]) -> tuple[int, ...]:
    raw_indices = ds_cfg.get("indices")
    if isinstance(raw_indices, Sequence) and not isinstance(raw_indices, (str, bytes)):
        return tuple(int(item) for item in raw_indices)
    if "index_start" in ds_cfg and "index_end" in ds_cfg:
        start = int(ds_cfg["index_start"])
        end = int(ds_cfg["index_end"])
        return tuple(range(start, end + 1))
    return ()


def _resolve_source_uri(
    source_name: str,
    sources_cfg: Mapping[str, Any],
) -> str:
    source_entry = sources_cfg.get(source_name, {})
    if isinstance(source_entry, Mapping):
        if "doi" in source_entry and source_entry["doi"]:
            return f"doi:{source_entry['doi']}"
        if "base_url" in source_entry and source_entry["base_url"]:
            return str(source_entry["base_url"])
        if "dataset_url" in source_entry and source_entry["dataset_url"]:
            return str(source_entry["dataset_url"])
    return source_name


def build_dataset_schemas(
    data_cfg: Mapping[str, Any] | None = None,
    lakehouse_cfg: Mapping[str, Any] | None = None,
) -> dict[str, DatasetSchema]:
    """Build DatasetSchema definitions from configs/data.yaml and configs/lakehouse.yaml."""
    datasets_cfg = (
        data_cfg.get("datasets", {})
        if isinstance(data_cfg, Mapping) and isinstance(data_cfg.get("datasets"), Mapping)
        else DEFAULT_DATASETS_CONFIG
    )
    sources_cfg = (
        data_cfg.get("sources", {})
        if isinstance(data_cfg, Mapping) and isinstance(data_cfg.get("sources"), Mapping)
        else DEFAULT_SOURCES_CONFIG
    )
    bronze_tables_cfg = (
        lakehouse_cfg.get("tables", {}).get("bronze", {})
        if isinstance(lakehouse_cfg, Mapping)
        and isinstance(lakehouse_cfg.get("tables"), Mapping)
        and isinstance(lakehouse_cfg["tables"].get("bronze"), Mapping)
        else {}
    )

    schemas: dict[str, DatasetSchema] = {}
    for name, raw_ds in datasets_cfg.items():
        if not isinstance(raw_ds, Mapping):
            raise BronzeSchemaError(f"Dataset entry '{name}' must be a mapping.")
        raw_cols = raw_ds.get("columns")
        if (
            isinstance(raw_cols, (str, bytes))
            or not isinstance(raw_cols, Sequence)
            or not raw_cols
        ):
            raise BronzeSchemaError(f"Dataset '{name}' must define a non-empty 'columns' sequence.")
        columns = tuple(str(col) for col in raw_cols)
        has_leading_index = bool(raw_ds.get("has_leading_index", False))
        timestamp_column = str(raw_ds.get("timestamp_column", "timestamp"))
        if timestamp_column not in columns:
            raise BronzeSchemaError(
                f"Dataset '{name}' timestamp_column '{timestamp_column}' not in columns {columns}."
            )

        col_types = {col: _resolve_arrow_type(col) for col in columns}
        # Hive partition keys ('dataset', 'source_file_stem') belong ONLY in directory paths.
        arrow_fields = [pa.field(col, col_types[col]) for col in columns]
        arrow_fields.extend(pa.field(prov_col, prov_type) for prov_col, prov_type in PROVENANCE_COLUMNS)
        arrow_schema = pa.schema(arrow_fields)

        lakehouse_table = DATASET_TO_LAKEHOUSE_TABLE.get(name, name)
        s3_table_uri = (
            str(bronze_tables_cfg[lakehouse_table])
            if lakehouse_table in bronze_tables_cfg and bronze_tables_cfg[lakehouse_table]
            else None
        )
        source_name = str(raw_ds.get("source", "unknown"))
        doi_or_url = _resolve_source_uri(source_name, sources_cfg)

        raw_pmin = raw_ds.get("partition_min")
        raw_pmax = raw_ds.get("partition_max")
        partition_min: int | None = None
        partition_max: int | None = None
        if raw_pmin is not None:
            if isinstance(raw_pmin, bool) or not isinstance(raw_pmin, int):
                raise BronzeSchemaError(
                    f"Dataset '{name}' partition_min must be an integer, got {raw_pmin!r}."
                )
            partition_min = raw_pmin
        if raw_pmax is not None:
            if isinstance(raw_pmax, bool) or not isinstance(raw_pmax, int):
                raise BronzeSchemaError(
                    f"Dataset '{name}' partition_max must be an integer, got {raw_pmax!r}."
                )
            partition_max = raw_pmax

        schemas[name] = DatasetSchema(
            name=name,
            source=source_name,
            columns=columns,
            has_leading_index=has_leading_index,
            timestamp_column=timestamp_column,
            column_types=col_types,
            arrow_schema=arrow_schema,
            doi_or_url=doi_or_url,
            lakehouse_table=lakehouse_table,
            s3_table_uri=s3_table_uri,
            archive_pattern=(
                str(raw_ds["archive_pattern"]) if raw_ds.get("archive_pattern") else None
            ),
            indices=_extract_indices(raw_ds),
            partition_min=partition_min,
            partition_max=partition_max,
        )

    return schemas


def get_dataset_schema(
    dataset_name: str,
    data_cfg: Mapping[str, Any] | None = None,
    lakehouse_cfg: Mapping[str, Any] | None = None,
) -> DatasetSchema:
    """Return the DatasetSchema for a given dataset key."""
    schemas = build_dataset_schemas(data_cfg=data_cfg, lakehouse_cfg=lakehouse_cfg)
    if dataset_name not in schemas:
        available = ", ".join(sorted(schemas.keys()))
        raise BronzeSchemaError(f"Unknown dataset '{dataset_name}'. Available datasets: {available}")
    return schemas[dataset_name]


def _is_unnamed_index_column(name: str) -> bool:
    stripped = name.strip()
    return stripped == "" or stripped.lower().startswith("unnamed:")


def normalize_csv_header(raw_header: Sequence[str], schema: DatasetSchema) -> HeaderNormalization:
    """Validate and normalize a CSV header against a DatasetSchema.

    - Drops the leading unnamed index column ('' / whitespace / 'Unnamed:*') ONLY when
      present AND schema.has_leading_index is True.
    - Rejects CASPER (has_leading_index=False) files that contain a leading unnamed column.
    - Preserves all columns when raw_header matches schema.columns (e.g. CASPER 8-col
      starting with 'traceid', or pre-stripped OSS CSVs).
    """
    if not raw_header:
        raise BronzeSchemaError(f"Empty CSV header for dataset '{schema.name}'.")

    cleaned = [str(col).lstrip("\ufeff").strip() for col in raw_header]
    has_unnamed_leading = _is_unnamed_index_column(cleaned[0])

    if has_unnamed_leading:
        if not schema.has_leading_index:
            raise BronzeSchemaError(
                f"Dataset '{schema.name}' must not have a leading unnamed index column; "
                f"got header {cleaned!r}."
            )
        remaining = tuple(cleaned[1:])
        selected_indices = tuple(range(1, len(cleaned)))
        dropped_leading_index = True
    else:
        remaining = tuple(cleaned)
        selected_indices = tuple(range(len(cleaned)))
        dropped_leading_index = False

    if remaining != schema.columns:
        raise BronzeSchemaError(
            f"Header mismatch for dataset '{schema.name}': "
            f"expected {list(schema.columns)}, got {list(remaining)} "
            f"(raw={cleaned!r})."
        )

    return HeaderNormalization(
        columns=remaining,
        dropped_leading_index=dropped_leading_index,
        selected_indices=selected_indices,
    )


def select_casper_partitions(
    partitions: int | Sequence[str] | None = None,
    *,
    partition_min: int | None = None,
    partition_max: int | None = None,
    ratio: float = 0.10,
    seed: int = 42,
    min_ratio: float = 0.10,
    max_ratio: float = 0.25,
) -> list[str]:
    """Deterministically select a seeded subset of CASPER Shuffled v2021 partitions.

    CASPER Shuffled v2021 partitions span signed IDs ``-999..999`` (1,999 partitions).
    """
    if (
        isinstance(ratio, bool)
        or isinstance(min_ratio, bool)
        or isinstance(max_ratio, bool)
        or not (0.0 < min_ratio <= max_ratio <= 1.0)
        or not (min_ratio <= ratio <= max_ratio)
    ):
        raise ValueError(
            f"CASPER sampling ratio {ratio} must be within [{min_ratio}, {max_ratio}]."
        )

    if isinstance(partitions, bool):
        raise ValueError(f"partitions must be an integer or sequence of names, got {partitions!r}.")
    if isinstance(partitions, (str, bytes)):
        raise ValueError(
            "partitions must be an integer or a sequence of partition filenames, not str/bytes."
        )

    if (partition_min is None) != (partition_max is None):
        raise ValueError("partition_min and partition_max must both be provided together.")

    if partition_min is not None and partition_max is not None:
        if (
            isinstance(partition_min, bool)
            or not isinstance(partition_min, int)
            or isinstance(partition_max, bool)
            or not isinstance(partition_max, int)
        ):
            raise ValueError(
                f"partition_min and partition_max must be integers, got ({partition_min!r}, {partition_max!r})."
            )
        if partition_min > partition_max:
            raise ValueError(
                f"partition_min ({partition_min}) must be <= partition_max ({partition_max})."
            )
        expected_count = partition_max - partition_min + 1
        if partitions is not None:
            if not isinstance(partitions, int):
                raise ValueError(
                    "Cannot specify both a partitions sequence and partition_min/partition_max."
                )
            if partitions != expected_count:
                raise ValueError(
                    f"partitions count {partitions} does not match bounds "
                    f"[{partition_min}..{partition_max}] ({expected_count} partitions)."
                )
        candidates = [
            f"partition_id={idx}.csv.gz" for idx in range(partition_min, partition_max + 1)
        ]
    elif partitions is None:
        candidates = [
            f"partition_id={idx}.csv.gz"
            for idx in range(DEFAULT_CASPER_PARTITION_MIN, DEFAULT_CASPER_PARTITION_MAX + 1)
        ]
    elif isinstance(partitions, int):
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}.")
        if partitions == DEFAULT_CASPER_TOTAL_PARTITIONS:
            start = DEFAULT_CASPER_PARTITION_MIN
            end = DEFAULT_CASPER_PARTITION_MAX
        else:
            start = -(partitions // 2)
            end = start + partitions - 1
        candidates = [f"partition_id={idx}.csv.gz" for idx in range(start, end + 1)]
    else:
        candidates = sorted(str(item) for item in partitions)
        if not candidates:
            raise ValueError("partitions sequence must not be empty.")

    sample_count = max(1, round(len(candidates) * ratio))
    sample_count = min(sample_count, len(candidates))
    rng = random.Random(seed)
    selected = rng.sample(sorted(candidates), sample_count)
    return sorted(selected)


def resolve_oss_archive_urls(
    dataset_name: str,
    data_cfg: Mapping[str, Any],
    *,
    indices: Sequence[int] | None = None,
) -> list[str]:
    """Resolve Alibaba OSS archive URLs from configs/data.yaml for a given OSS dataset."""
    schema = get_dataset_schema(dataset_name, data_cfg=data_cfg)
    if schema.source != "alibaba_oss" or not schema.archive_pattern:
        raise BronzeSchemaError(f"Dataset '{dataset_name}' does not define an Alibaba OSS archive_pattern.")
    sources_cfg = data_cfg.get("sources", {})
    oss_cfg = sources_cfg.get("alibaba_oss", {}) if isinstance(sources_cfg, Mapping) else {}
    base_url = str(oss_cfg.get("base_url", "")).rstrip("/")
    if not base_url:
        raise BronzeSchemaError("data.sources.alibaba_oss.base_url must be non-empty.")

    target_indices = tuple(indices) if indices is not None else schema.indices
    if not target_indices:
        raise BronzeSchemaError(f"No archive indices resolved for dataset '{dataset_name}'.")

    return [f"{base_url}/{schema.archive_pattern.format(index=idx)}" for idx in target_indices]


__all__ = [
    "BronzeError",
    "BronzeSchemaError",
    "COLUMN_TYPE_MAP",
    "DATASET_TO_LAKEHOUSE_TABLE",
    "DEFAULT_CASPER_PARTITION_MAX",
    "DEFAULT_CASPER_PARTITION_MIN",
    "DEFAULT_CASPER_TOTAL_PARTITIONS",
    "DEFAULT_DATASETS_CONFIG",
    "DEFAULT_SOURCES_CONFIG",
    "DatasetSchema",
    "HeaderNormalization",
    "PROVENANCE_COLUMNS",
    "build_dataset_schemas",
    "get_dataset_schema",
    "normalize_csv_header",
    "resolve_oss_archive_urls",
    "select_casper_partitions",
]
