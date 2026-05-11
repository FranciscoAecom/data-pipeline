from difflib import get_close_matches

from core.schema import target_column_name
from core.text import normalize_for_compare
from core.utils import log


def sa_car_transform_desc_condic(gdf, column, **_context):
    target_column = target_column_name(column)

    if column not in gdf.columns:
        log(f"Atributo {column} nao encontrado")
        return gdf

    def transform_value(value):
        if not isinstance(value, str):
            return value

        normalized = normalize_for_compare(value)
        if not normalized:
            return value.strip()
        if "CANCELADO" in normalized:
            return "Cancelado"
        if "AGUARDANDO ANALISE" in normalized:
            return "Aguardando analise"
        if "ANALISADO" in normalized:
            return "Analisado"

        candidates = ["ANALISADO", "CANCELADO", "AGUARDANDO ANALISE"]
        close = get_close_matches(normalized, candidates, n=1, cutoff=0.8)
        if close:
            match = close[0]
            if match == "ANALISADO":
                return "Analisado"
            if match == "CANCELADO":
                return "Cancelado"
            if match == "AGUARDANDO ANALISE":
                return "Aguardando analise"
        return value.strip()

    unique_values = gdf[column].drop_duplicates()
    replacements = {value: transform_value(value) for value in unique_values}
    gdf[target_column] = gdf[column].map(replacements)
    return gdf


PROJECT_OPTIONAL_FUNCTIONS = {
    "sa_car_transform_desc_condic": sa_car_transform_desc_condic,
}
