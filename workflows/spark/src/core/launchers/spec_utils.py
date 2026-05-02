from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Iterator

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

TEMPLATE_SUFFIX = ".j2"
DEFAULT_PIPELINE_STORAGE = "/tmp/spark-pipelines"
DEFAULT_SPEC_FILE = "spark-pipeline.yml"
SPEC_FILE_NAMES = (DEFAULT_SPEC_FILE, "spark-pipeline.yaml")
TEMPLATE_SPEC_FILE_NAMES = tuple(
    f"{spec_file_name}{TEMPLATE_SUFFIX}" for spec_file_name in SPEC_FILE_NAMES
)
SPEC_FILE_VARIANTS = [*SPEC_FILE_NAMES, *TEMPLATE_SPEC_FILE_NAMES]


def default_spec_file() -> str:
    return DEFAULT_SPEC_FILE


def resolve_spec_path(pipeline: str, spec_file: str) -> Path:
    """Resolve the full path to a spec file given pipeline name and spec filename.
    
    If spec_file is a basename without extension or a partial name, this function
    will try to find a matching file among the supported variants.
    """
    pipeline_dir = Path(__file__).resolve().parents[2] / "pipelines" / pipeline
    
    # If the exact file exists, return it
    exact_path = pipeline_dir / spec_file
    if exact_path.exists():
        return exact_path
    
    # Try to find a matching variant if spec_file looks like a base name
    spec_basename = spec_file.split(".")[0] if "." in spec_file else spec_file
    if spec_basename == Path(DEFAULT_SPEC_FILE).stem:
        for variant in SPEC_FILE_VARIANTS:
            variant_path = pipeline_dir / variant
            if variant_path.exists():
                return variant_path
    
    # Return the requested path even if it doesn't exist (will fail later with better error)
    return exact_path


def is_template_path(path: Path, names: tuple[str, ...] | None = None) -> bool:
    if path.suffix != TEMPLATE_SUFFIX:
        return False

    if names is None:
        return True

    return path.name in names


def rendered_template_path(path: Path) -> Path:
    return path.with_suffix("")


def normalize_pipeline_storage(value: str) -> str:
    if "://" in value:
        return value
    return Path(value).expanduser().resolve().as_uri()


def resolve_materialization_root(spec_path: Path) -> tuple[Path, list[Path]]:
    workspace_root = Path(mkdtemp(prefix=f".{spec_path.stem}-workspace-"))
    return workspace_root, [workspace_root]


def render_template_file(path: Path, root: Path | None = None, **context: Any) -> str:
    if root is None:
        search_paths = [str(parent) for parent in path.parents]
        template_name = path.name
    else:
        search_paths = [str(root)] + [str(parent) for parent in path.parents]
        template_name = path.relative_to(root).as_posix()
    
    environment = Environment(
        loader=FileSystemLoader(search_paths),
        autoescape=False,
        undefined=StrictUndefined,
    )
    context["env"] = dict(os.environ)
    return environment.get_template(template_name).render(**context)


def iter_template_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob(f"*{TEMPLATE_SUFFIX}")
        if path.is_file() and is_template_path(path)
    )


def stage_pipeline_workspace(spec_path: Path, materialization_root: Path) -> Path:
    shutil.copytree(spec_path.parent, materialization_root, dirs_exist_ok=True)
    return materialization_root / spec_path.name


def materialize_template_workspace(
    root: Path,
    pipeline_storage: str,
) -> dict[Path, Path]:
    rendered_paths: dict[Path, Path] = {}

    for template_path in iter_template_files(root):
        output_path = rendered_template_path(template_path)
        if output_path.exists():
            raise ValueError(
                f"Cannot materialize template file {template_path}: {output_path} already exists in the staged pipeline workspace."
            )

        output_path.write_text(
            render_template_file(
                template_path,
                root=root,
                pipeline_storage=pipeline_storage,
            ),
            encoding="utf-8",
        )
        rendered_paths[template_path] = output_path

    return rendered_paths


def remove_template_sources(template_paths: list[Path]) -> None:
    for template_path in template_paths:
        template_path.unlink(missing_ok=True)


def load_spec_data(spec_path: Path, pipeline_storage: str) -> dict[str, Any]:
    pipeline_storage_uri = normalize_pipeline_storage(pipeline_storage)

    if is_template_path(spec_path, TEMPLATE_SPEC_FILE_NAMES):
        raw_spec = render_template_file(
            spec_path,
            root=spec_path.parent,
            pipeline_storage=pipeline_storage,
        )
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
    materialization_root, owned_roots = resolve_materialization_root(spec_path)
    staged_spec_path = stage_pipeline_workspace(spec_path, materialization_root)
    template_paths = iter_template_files(materialization_root)
    rendered_paths = materialize_template_workspace(
        materialization_root,
        pipeline_storage,
    )
    effective_spec_path = rendered_paths.get(staged_spec_path, staged_spec_path)
    spec_data = load_spec_data(effective_spec_path, pipeline_storage)
    with effective_spec_path.open("w", encoding="utf-8") as spec_file:
        yaml.safe_dump(spec_data, spec_file, sort_keys=False)
    remove_template_sources(template_paths)

    try:
        yield effective_spec_path
    finally:
        for path in owned_roots:
            shutil.rmtree(path, ignore_errors=True)
