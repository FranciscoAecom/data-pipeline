import pandas as pd

from core.spatial.duplicates import check_attribute_geometric_duplicates


def check_attribute_duplicates(gdf):
    non_geom_columns = [c for c in gdf.columns if c != "geometry"]
    dup = gdf[gdf.duplicated(subset=non_geom_columns)]
    count = len(dup)
    return gdf, count


def get_duplicate_columns(gdf):
    exclude = {
        "acm_id",
        "acm_a_ha",
        "acm_prm_km",
        "acm_long",
        "acm_lat",
        "geometry",
    }
    return [c for c in gdf.columns if c not in exclude]


def get_attribute_duplicate_mask(gdf):
    compare_columns = get_duplicate_columns(gdf)
    if not compare_columns:
        return pd.Series(False, index=gdf.index)
    return gdf.duplicated(subset=compare_columns, keep=False)


def get_attribute_duplicate_records(gdf, dup_mask=None):
    if dup_mask is None:
        dup_mask = get_attribute_duplicate_mask(gdf)
    duplicates = gdf[dup_mask].copy()
    count = len(duplicates)
    return duplicates, count


def check_duplicates(gdf):
    attr_count = int(get_attribute_duplicate_mask(gdf).sum())
    gdf, geom_count = check_attribute_geometric_duplicates(gdf)
    return gdf, attr_count, geom_count


__all__ = [
    "check_attribute_duplicates",
    "check_duplicates",
    "get_attribute_duplicate_mask",
    "get_attribute_duplicate_records",
    "get_duplicate_columns",
]
