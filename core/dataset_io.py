from pathlib import Path
import warnings
from time import perf_counter
import os

import geopandas as gpd
import pyogrio

from core.utils import log
from settings import USE_ARROW_IO


SUPPORTED_INPUT_SUFFIXES = {".shp", ".gpkg"}


def _read_dataframe_with_fallback(path, layer=None):
    read_kwargs = {"layer": layer}

    if USE_ARROW_IO:
        try:
            return pyogrio.read_dataframe(path, use_arrow=True, **read_kwargs)
        except ImportError as exc:
            log(
                "Leitura Arrow indisponivel no ambiente atual; "
                f"voltando para a leitura padrao do pyogrio. Detalhe: {exc}"
            )
        except RuntimeError as exc:
            log(
                "Leitura Arrow nao pode ser utilizada neste ambiente; "
                f"voltando para a leitura padrao do pyogrio. Detalhe: {exc}"
            )
        except TypeError as exc:
            log(
                "A versao atual do pyogrio nao aceita use_arrow=True; "
                f"voltando para a leitura padrao. Detalhe: {exc}"
            )

    return pyogrio.read_dataframe(path, **read_kwargs)


def _select_input_layer(path):
    path_obj = Path(path)
    if path_obj.suffix.lower() != ".gpkg":
        return None

    layers = pyogrio.list_layers(path)
    if layers is None or len(layers) == 0:
        return None

    first = layers[0]
    if isinstance(first, (list, tuple)):
        return first[0]
    return str(first)


def _log_captured_warnings(captured_warnings, path):
    seen_messages = set()

    for warning in captured_warnings:
        message = str(warning.message)
        if message in seen_messages:
            continue
        seen_messages.add(message)

        if "invalid winding order" in message.lower():
            log(
                f"Aviso de geometria na leitura de {path}: aneis de poligono com orientacao invalida foram autocorrigidos."
            )
        else:
            log(f"Aviso na leitura de {path}: {message}")


def inspect_input_attributes(path):
    layer = _select_input_layer(path)
    started = perf_counter()
    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        info = pyogrio.read_info(path, layer=layer)
    _log_captured_warnings(captured_warnings, path)
    log(f"Leitura de metadados concluida em {perf_counter() - started:.2f}s: {path}")

    fields = info.get("fields")
    if fields is None:
        return []
    return list(fields)


def read_input_dataset(path):
    layer = _select_input_layer(path)
    log(f"Iniciando leitura do arquivo de entrada: {path}")
    started = perf_counter()
    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        gdf = _read_dataframe_with_fallback(path, layer=layer)
    _log_captured_warnings(captured_warnings, path)
    log(f"Leitura do arquivo concluida em {perf_counter() - started:.2f}s: {path}")
    return gdf


def _remove_existing_gpkg_artifacts(output):
    for candidate in [
        output,
        output.with_name(f"{output.name}-wal"),
        output.with_name(f"{output.name}-shm"),
    ]:
        if candidate.exists():
            os.remove(candidate)


def write_output_gpkg(
    gdf,
    output_path,
    layer=None,
    append=False,
    overwrite_existing=False,
):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    layer_name = layer or output.stem
    if overwrite_existing and not append:
        _remove_existing_gpkg_artifacts(output)
    started = perf_counter()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*Only 0 or 1 should be passed for a OFSTBoolean subtype.*",
            category=RuntimeWarning,
        )
        pyogrio.write_dataframe(
            gdf,
            output,
            layer=layer_name,
            driver="GPKG",
            append=append,
        )
    log(f"Escrita do arquivo concluida em {perf_counter() - started:.2f}s: {output}")
    return str(output)
