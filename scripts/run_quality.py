"""T06 exact profile of all configured local development-sample rows."""
import argparse
from pathlib import Path
from ie212.config import load_config
from ie212.quality.sample_profile import profile_samples


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="configs/data.yaml")
    p.add_argument("--sample-dir", default="data/sample")
    p.add_argument("--output", default="reports/data_quality/t06_sample_profile.json")
    p.add_argument("--workspace", default="artifacts/t06")
    p.add_argument("--reuse-raw", action="store_true", help="Reuse staging only when input SHA-256 evidence matches the previous report")
    a = p.parse_args()
    root = Path(a.sample_dir)
    specs = {"oss_node": "node", "oss_msresource": "msresource", "oss_msrtqps": "msrtqps", "casper_mscallgraph": "mscallgraph"}
    inputs = {ds: sorted((root / folder).glob("*.csv.gz")) for ds, folder in specs.items()}
    if any(not paths for paths in inputs.values()):
        p.error("All four sample groups must have input files")
    profile_samples(inputs, Path(a.output), Path(a.workspace), load_config(a.config), reuse_raw=a.reuse_raw)


if __name__ == "__main__":
    main()
