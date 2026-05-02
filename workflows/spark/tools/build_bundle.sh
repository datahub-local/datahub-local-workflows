#!/usr/bin/env sh
set -eu

PYTHON_VERSION="${1:-3.11}"

cd "$(dirname "$0")/.."

rm -rf build/runtime

mkdir -p build/runtime/site-packages build/runtime/src
uv export \
    --python "${PYTHON_VERSION}" \
    --frozen \
    --format requirements.txt \
    --no-dev \
    --no-editable \
    --no-emit-project \
    --no-hashes \
    --output-file build/runtime/requirements.txt

uv pip install \
    --python "${PYTHON_VERSION}" \
    --target build/runtime/site-packages \
    -r build/runtime/requirements.txt

PYTHON_BIN="$(uv python find "${PYTHON_VERSION}")"

"${PYTHON_BIN}" <<'PY'
from pathlib import Path
import zipfile

site_packages_dir = Path("build/runtime/site-packages")
archive_path = Path("build/runtime/python-deps.zip")

with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(site_packages_dir.rglob("*")):
        if path.is_file():
            archive.write(path, path.relative_to(site_packages_dir))
PY

cp -R src/launchers build/runtime/src/core
cp -R src/pipelines build/runtime/src/pipelines
