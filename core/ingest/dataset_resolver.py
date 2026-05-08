from functools import lru_cache
from pathlib import Path
import re

from core.ingest.normalization import stringify


SUPPORTED_DATASET_SUFFIXES = {".shp", ".gpkg"}


def is_zip_path(path_value):
    return stringify(path_value).lower().endswith(".zip")


def resolve_numbered_sibling_datasets(path, supported_suffixes=SUPPORTED_DATASET_SUFFIXES):
    if path.suffix.lower() not in supported_suffixes or not path.exists():
        return []

    match = re.match(r"^(?P<prefix>.+?)_(?P<index>\d+)$", path.stem, flags=re.IGNORECASE)
    if not match:
        return []

    prefix = match.group("prefix")
    sibling_pattern = re.compile(
        rf"^{re.escape(prefix)}_(\d+){re.escape(path.suffix)}$",
        flags=re.IGNORECASE,
    )
    sibling_files = sorted(
        candidate for candidate in path.parent.iterdir()
        if candidate.is_file()
        and candidate.suffix.lower() in supported_suffixes
        and sibling_pattern.match(candidate.name)
    )

    if len(sibling_files) <= 1:
        return []

    return [str(candidate) for candidate in sibling_files]


def resolve_input_dataset_paths(path_value):
    raw_path = stringify(path_value)
    if not raw_path:
        raise FileNotFoundError("Campo path_shapefile_temp vazio.")

    if is_zip_path(raw_path):
        raise ValueError("Caminho aponta para arquivo ZIP; leitura desabilitada.")

    path = Path(raw_path)

    if path.suffix.lower() in SUPPORTED_DATASET_SUFFIXES:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {path}")
        sibling_dataset_paths = resolve_numbered_sibling_datasets(path)
        if sibling_dataset_paths:
            return sibling_dataset_paths
        return [str(path)]

    if not path.exists():
        raise FileNotFoundError(f"Caminho nao encontrado: {path}")

    if not path.is_dir():
        raise ValueError(f"Caminho nao suportado para leitura automatica: {path}")

    direct_dataset_files = sorted(
        candidate for candidate in path.iterdir()
        if candidate.is_file() and candidate.suffix.lower() in SUPPORTED_DATASET_SUFFIXES
    )

    if direct_dataset_files:
        return [str(candidate) for candidate in direct_dataset_files]

    dataset_files = sorted(
        candidate for candidate in path.rglob("*")
        if candidate.is_file()
        and candidate.suffix.lower() in SUPPORTED_DATASET_SUFFIXES
        and ".zip" not in {part.lower() for part in candidate.parts}
    )

    if not dataset_files:
        raise FileNotFoundError(f"Nenhum shapefile ou gpkg encontrado dentro de: {path}")

    return [str(candidate) for candidate in dataset_files]


@lru_cache(maxsize=None)
def resolve_input_dataset_paths_cached(path_value):
    return tuple(resolve_input_dataset_paths(path_value))


__all__ = [
    "SUPPORTED_DATASET_SUFFIXES",
    "is_zip_path",
    "resolve_input_dataset_paths",
    "resolve_input_dataset_paths_cached",
    "resolve_numbered_sibling_datasets",
]
