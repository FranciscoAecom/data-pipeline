import re
import unicodedata
from difflib import get_close_matches

from core.utils import log


def reserva_legal_car_transform_desc_condic(gdf, column):
    target_column = "acm_desc_condic"

    if column not in gdf.columns:
        log(f"Atributo {column} nao encontrado")
        return gdf

    def normalize_text(value):
        if not isinstance(value, str):
            return value
        text = value.strip()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if ord(ch) < 128)
        text = re.sub(r"\s+", " ", text)
        return text.upper()

    def transform_value(value):
        if not isinstance(value, str):
            return value

        normalized = normalize_text(value)
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
    "reserva_legal_car_transform_desc_condic": reserva_legal_car_transform_desc_condic,
}
