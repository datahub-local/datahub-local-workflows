import importlib
from pathlib import Path

ROOT = Path(__file__).parent
SPARK_VERSION = ROOT.joinpath(".spark-version").read_text(encoding="utf-8").strip()
setup = importlib.import_module("setuptools").setup


setup(
    install_requires=[
        f"pyspark[pipelines]=={SPARK_VERSION}",
        "jinja2",
    ]
)