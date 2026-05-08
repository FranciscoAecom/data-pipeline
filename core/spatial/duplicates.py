def get_geometry_signature(gdf):
    try:
        return gdf.geometry.to_wkb()
    except Exception:
        return gdf.geometry.apply(lambda geom: geom.wkb if geom is not None else None)


def get_geometric_duplicate_mask(gdf):
    geom_signature = get_geometry_signature(gdf)
    return geom_signature.duplicated(keep=False)


def check_attribute_geometric_duplicates(gdf):
    dup_mask = get_geometric_duplicate_mask(gdf)
    count = int(dup_mask.sum())
    return gdf, count


def get_geometric_duplicate_records(gdf, dup_mask=None):
    if dup_mask is None:
        dup_mask = get_geometric_duplicate_mask(gdf)
    duplicates = gdf[dup_mask].copy()
    count = len(duplicates)
    return duplicates, count


__all__ = [
    "check_attribute_geometric_duplicates",
    "get_geometric_duplicate_mask",
    "get_geometric_duplicate_records",
    "get_geometry_signature",
]
