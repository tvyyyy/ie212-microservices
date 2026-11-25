"""CLI for T10 Node/Resource/RTQps Silver; calls/trace T13/T14 are separate."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from ie212.config import load_config, load_project_configs, validate_lakehouse_config
from ie212.smoke_test import collect_runtime_secrets, sanitize_secret_text


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="configs/lakehouse.yaml", help="Lakehouse configuration (configs/lakehouse.yaml)")
    p.add_argument("--config-dir", default="configs")
    p.add_argument("--dotenv", default=".env")
    p.add_argument("--check-config", action="store_true")
    p.add_argument("--bronze-root", default="data/raw/bronze_t10")
    p.add_argument("--output-dir", default="artifacts/t10/silver")
    p.add_argument("--report", default="reports/data_quality/t10_silver_acceptance.json")
    p.add_argument("--dataset", choices=["all", "oss_node", "oss_msresource", "oss_msrtqps"], default="all")
    p.add_argument("--namespace", default="silver")
    p.add_argument("--threads", type=int, choices=[1, 2, 4], default=2)
    p.add_argument("--target", choices=["local", "container"], default="container")
    a = p.parse_args(argv)
    try:
        cfg = load_config(a.config, dotenv_path=a.dotenv)
        validate_lakehouse_config(cfg)
        project = load_project_configs(a.config_dir, dotenv_path=a.dotenv)
        if a.check_config:
            print(json.dumps({"stage": "silver", "status": "validated", "check_config": True,
                              "tasks": ["T10"], "config_loaded": a.config,
                              "project_configs": sorted(project)}))
            return 0
        from ie212.silver.pipeline import run_silver_metrics
        from ie212.silver.metrics import SPECS
        result = run_silver_metrics(cfg, Path(a.bronze_root), Path(a.output_dir), Path(a.report),
                                   list(SPECS) if a.dataset == "all" else [a.dataset],
                                   a.namespace, a.threads, a.target)
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        print(f"Silver failed: {type(exc).__name__}: {sanitize_secret_text(str(exc), collect_runtime_secrets(dotenv_path=a.dotenv))}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
