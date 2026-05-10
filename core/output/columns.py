from core.spatial.repair import INTERNAL_SAFE_REPAIR_FLAG


def drop_internal_output_columns(gdf):
    internal_columns = [INTERNAL_SAFE_REPAIR_FLAG]
    existing_columns = [column for column in internal_columns if column in gdf.columns]
    if not existing_columns:
        return gdf
    return gdf.drop(columns=existing_columns)
