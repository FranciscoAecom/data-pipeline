import pandas as pd


def parse_date_series(series):
    cleaned = series.copy()
    if pd.api.types.is_datetime64_any_dtype(cleaned):
        return pd.to_datetime(cleaned, errors="coerce").dt.normalize()

    as_text = cleaned.astype("string").str.strip()
    as_text = as_text.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    parsed = pd.to_datetime(
        as_text,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    remaining_mask = parsed.isna() & as_text.notna()
    if remaining_mask.any():
        parsed.loc[remaining_mask] = pd.to_datetime(
            as_text.loc[remaining_mask],
            format="%d/%m/%Y",
            errors="coerce",
            dayfirst=True,
        )

    remaining_mask = parsed.isna() & as_text.notna()
    if remaining_mask.any():
        parsed.loc[remaining_mask] = pd.to_datetime(
            as_text.loc[remaining_mask],
            errors="coerce",
            dayfirst=True,
        )

    return parsed.dt.normalize()


def _target_column_name(column):
    if str(column).startswith("sdb_"):
        return f"acm_{str(column)[4:]}"
    return f"acm_{column}"


def validate_date_fields(gdf, column):
    target_column = _target_column_name(column)
    gdf[target_column] = parse_date_series(gdf[column])
    return gdf


PROJECT_OPTIONAL_FUNCTIONS = {
    "validate_date_fields": validate_date_fields,
}
