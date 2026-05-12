from argparse import ArgumentParser
from pathlib import Path
import warnings

import pandas as pd
import pyogrio


SUPPORTED_SUFFIXES = {".shp", ".gpkg"}
DEFAULT_OUTPUT_SUFFIX = "_valores_unicos.xlsx"


def _select_layer(path):
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


def _sanitize_sheet_name(name):
    invalid_chars = '[]:*?/\\'
    sanitized = "".join("_" if char in invalid_chars else char for char in str(name))
    sanitized = sanitized.strip() or "coluna"
    return sanitized[:31]


def _build_output_path(input_path, output_path=None):
    if output_path:
        return Path(output_path)
    source = Path(input_path)
    return source.with_name(f"{source.stem}{DEFAULT_OUTPUT_SUFFIX}")


def _read_dataset(input_path):
    layer = _select_layer(input_path)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pyogrio.read_dataframe(input_path, layer=layer, use_arrow=True)


def _series_unique_values(series):
    normalized = series.copy()
    if pd.api.types.is_object_dtype(normalized) or pd.api.types.is_string_dtype(normalized):
        normalized = normalized.astype("string").str.strip()
        normalized = normalized.where(normalized != "", pd.NA)

    counts = normalized.value_counts(dropna=False)
    rows = []

    for value, count in counts.items():
        if pd.isna(value):
            display_value = "<NULL>"
        else:
            display_value = value
        rows.append({
            "valor": display_value,
            "ocorrencias": int(count),
        })

    return pd.DataFrame(rows)


def _resolve_columns(df, columns=None):
    if columns is None:
        return [column for column in df.columns if column != "geometry"]

    resolved = []
    lookup = {str(column).casefold(): column for column in df.columns}
    for column in columns:
        if column in df.columns:
            resolved.append(column)
            continue

        raw = str(column)
        normalized = raw.casefold()
        if normalized in lookup:
            resolved.append(lookup[normalized])
            continue

        if raw.startswith("sdb_") and raw[4:] in df.columns:
            resolved.append(raw[4:])
            continue

        if raw.startswith("acm_") and raw[4:] in df.columns:
            resolved.append(raw[4:])
            continue

    seen = set()
    unique_resolved = []
    for column in resolved:
        if column == "geometry" or column in seen:
            continue
        seen.add(column)
        unique_resolved.append(column)
    return unique_resolved


def export_unique_values_from_dataframe(df, output_path, columns=None):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    selected_columns = _resolve_columns(df, columns)
    if not selected_columns:
        raise ValueError("Nenhum atributo elegivel encontrado para exportar valores unicos.")

    summary_rows = []
    used_sheet_names = set()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for column in selected_columns:
            unique_df = _series_unique_values(df[column])
            sheet_name = _sanitize_sheet_name(column)
            suffix = 1
            base_name = sheet_name
            while sheet_name in used_sheet_names:
                suffix += 1
                sheet_name = f"{base_name[:28]}_{suffix}"[:31]
            used_sheet_names.add(sheet_name)

            unique_df.to_excel(writer, sheet_name=sheet_name, index=False)
            summary_rows.append({
                "atributo": column,
                "tipo": str(df[column].dtype),
                "valores_unicos": int(len(unique_df)),
            })

        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="resumo", index=False)

    return output


def export_unique_values(input_path, output_path=None, columns=None):
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {source}")
    if source.suffix.lower() not in SUPPORTED_SUFFIXES:
        raise ValueError("Formato nao suportado. Use um arquivo .shp ou .gpkg")

    gdf = _read_dataset(str(source))
    output = _build_output_path(source, output_path)
    return export_unique_values_from_dataframe(gdf, output, columns=columns)


def main():
    parser = ArgumentParser(
        description="Gera uma planilha com os valores unicos de cada atributo de um SHP ou GPKG."
    )
    parser.add_argument("input_path", help="Caminho para o arquivo .shp ou .gpkg")
    parser.add_argument(
        "--output",
        dest="output_path",
        help="Caminho opcional do arquivo .xlsx de saida",
    )
    parser.add_argument(
        "--columns",
        help="Lista opcional de atributos separados por virgula",
    )
    args = parser.parse_args()

    columns = None
    if args.columns:
        columns = [item.strip() for item in args.columns.split(",") if item.strip()]

    output = export_unique_values(args.input_path, args.output_path, columns=columns)
    print(f"Relatorio gerado com sucesso: {output}")


__all__ = [
    "DEFAULT_OUTPUT_SUFFIX",
    "SUPPORTED_SUFFIXES",
    "export_unique_values",
    "export_unique_values_from_dataframe",
    "main",
]
