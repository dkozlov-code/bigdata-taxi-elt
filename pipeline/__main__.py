import argparse
import logging

from pipeline.config import Settings
from pipeline.flow import demonstrate, run_pipeline
from pipeline.storage import read_version, restore

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["run", "demo", "versions", "read", "restore"], default="run", nargs="?")
parser.add_argument("--version")
parser.add_argument("--fault", choices=["extract", "validate", "load", "transform", "quality", "publish"])
args = parser.parse_args()
if args.command in {"read", "restore"} and not args.version:
    parser.error("Для read/restore требуется --version")
logging.basicConfig(level=logging.INFO)
settings = Settings.from_env()
if args.command == "run":
    print(run_pipeline(settings, args.fault))
elif args.command == "demo":
    demonstrate(settings)
elif args.command == "versions":
    print("\n".join(p.name for p in sorted((settings.work / "versions").glob("*.duckdb"))))
elif args.command == "read":
    print(read_version(settings.work, args.version).to_string(index=False))
else:
    restore(settings.work, args.version)
