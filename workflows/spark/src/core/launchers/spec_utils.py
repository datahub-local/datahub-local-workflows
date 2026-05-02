from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from typing import Any, Iterator

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

DEFAULT_PIPELINE_STORAGE = "/tmp/spark-pipelines"
PIPELINE_SPEC_FILE_NAME = Path("pi") / "spark-pipeline.yml"
TEMPLATE_SPEC_SUFFIXES = (".yaml.j2", ".yml.j2")


def default_spec_path() -> Path:
    return Path(__file__).resolve().parents[1] / "pipelines" / PIPELINE_SPEC_FILE_NAME


def is_template_spec(spec_path: Path) -> bool:
    return spec_path.name.endswith(TEMPLATE_SPEC_SUFFIXES)


def is_template_sql(path: Path) -> bool:
    return path.name.endswith(".sql.j2")


def normalize_pipeline_storage(value: str) -> str:
    if "://" in value:
        return value
    return Path(value).expanduser().resolve().as_uri()


def render_template_file(path: Path, root: Path | None = None, **context: Any) -> str:
    if root is None:
        search_paths = [str(parent) for parent in path.parents]
    else:
        search_paths = [str(root)] + [str(parent) for parent in path.parents]
    
    environment = Environment(
        loader=FileSystemLoader(search_paths),
        autoescape=False,
        undefined=StrictUndefined,
    )
    context["env"] = dict(os.environ)
    return environment.get_template(path.name).render(**context)


def iter_matching_library_files(root: Path, include: str) -> Iterator[Path]:
    patterns = [include]
    if include.endswith("**"):
        patterns.append(f"{include}/*")

    seen: set[Path] = set()
    for pattern in patterns:
        for path in root.glob(pattern):
            if path.is_file() and path not in seen:
                seen.add(path)
                yield path


def iter_template_library_includes(include: str) -> Iterator[str]:
    yield include
    if include.endswith(".j2") or include.endswith("**"):
        return
    if any("**" in component and component != "**" for component in include.split("/")):
        return
    yield f"{include}.j2"


def iter_library_source_files(root: Path, include: str) -> Iterator[Path]:
    seen: set[Path] = set()
    for template_include in iter_template_library_includes(include):
        for path in iter_matching_library_files(root, template_include):
            if path not in seen:
                seen.add(path)
                yield path


def materialize_runtime_libraries(
    spec_path: Path, spec_data: dict[str, Any], pipeline_storage: str
) -> list[Path]:
    root = spec_path.parent.resolve()
    materialized_libraries: list[dict[str, Any]] = []
    materialized_roots: list[Path] = []

    temp_root = Path(
        mkdtemp(prefix=f".{spec_path.stem}-libraries-", dir=spec_path.parent)
    )

    for index, library in enumerate(spec_data.get("libraries", [])):
        include = library.get("glob", {}).get("include")
        if not include:
            materialized_libraries.append(library)
            continue

        matched_paths = list(iter_library_source_files(root, include))
        if not matched_paths:
            materialized_libraries.append(library)
            continue

        materialized_roots.append(temp_root)
        library_root = temp_root / f"library_{index}"

        for path in matched_paths:
            target_path = library_root / path.relative_to(root)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if is_template_sql(path):
                target_path = target_path.with_suffix("")
                if target_path.exists():
                    raise ValueError(
                        f"Cannot materialize library file {path}: {target_path} already exists in the temporary runtime library."
                    )
                target_path.write_text(
                    render_template_file(path, root=root, pipeline_storage=pipeline_storage),
                    encoding="utf-8",
                )
                continue

            if target_path.exists():
                raise ValueError(
                    f"Cannot materialize library file {path}: {target_path} already exists in the temporary runtime library."
                )

            shutil.copy2(path, target_path)

        materialized_library = dict(library)
        glob_config = dict(library.get("glob", {}))
        glob_config["include"] = f"{temp_root.name}/library_{index}/**"
        materialized_library["glob"] = glob_config
        materialized_libraries.append(materialized_library)

    if materialized_roots:
        spec_data["libraries"] = materialized_libraries
        return [temp_root]

    temp_root.rmdir()
    return []


def load_spec_data(spec_path: Path, pipeline_storage: str) -> dict[str, Any]:
    pipeline_storage_uri = normalize_pipeline_storage(pipeline_storage)

    if is_template_spec(spec_path):
        raw_spec = render_template_file(spec_path, pipeline_storage=pipeline_storage)
    else:
        raw_spec = spec_path.read_text()

    spec_data = yaml.safe_load(raw_spec)
    if not isinstance(spec_data, dict):
        raise ValueError(
            f"Pipeline spec {spec_path} must contain a top-level YAML mapping."
        )

    spec_data["storage"] = pipeline_storage_uri
    return spec_data


@contextmanager
def materialize_spec(spec_path: Path, pipeline_storage: str) -> Iterator[Path]:
    spec_data = load_spec_data(spec_path, pipeline_storage)
    materialized_library_roots = materialize_runtime_libraries(
        spec_path, spec_data, pipeline_storage
    )

    # Keep the generated spec next to the source spec so relative glob paths still resolve.
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yml",
        prefix=f".{spec_path.stem}-",
        dir=spec_path.parent,
        delete=False,
    ) as spec_file:
        yaml.safe_dump(spec_data, spec_file, sort_keys=False)
        effective_spec_path = Path(spec_file.name)

    try:
        yield effective_spec_path
    finally:
        effective_spec_path.unlink(missing_ok=True)
        for path in materialized_library_roots:
            shutil.rmtree(path, ignore_errors=True)
