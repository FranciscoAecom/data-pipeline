from pathlib import Path

import geopandas as gpd

from core.dataset_io import write_output_gpkg
from settings import GEOM_DUPLICATES_LAYER, OGC_INVALID_LAYER, OGC_REASON_FIELD
from core.spatial.spatial_functions import get_geometric_duplicate_records, get_invalid_ogc_records
from core.validation.validation_functions import get_attribute_duplicate_records


def export_duplicate_reports(
    gdf,
    output_dir,
    base_name,
    *,
    attr_duplicates=None,
    attr_count=None,
    geom_duplicates=None,
    geom_count=None,
    ogc_invalid=None,
    ogc_invalid_count=None,
    ogc_error_summary=None,
):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if attr_duplicates is None or attr_count is None:
        attr_duplicates, attr_count = get_attribute_duplicate_records(gdf)
    if geom_duplicates is None or geom_count is None:
        geom_duplicates, geom_count = get_geometric_duplicate_records(gdf)
    if ogc_invalid is None or ogc_invalid_count is None or ogc_error_summary is None:
        ogc_invalid, ogc_invalid_count, ogc_error_summary = get_invalid_ogc_records(gdf)

    def _prepare_export(df):
        export_df = df.copy()
        if "geometry" in export_df.columns:
            export_df["geometry_wkt"] = export_df.geometry.to_wkt()
            export_df = export_df.drop(columns="geometry")
        return export_df

    def _save(path, df, sheet_name):
        export_df = _prepare_export(df)
        try:
            export_df.to_excel(path, index=False, sheet_name=sheet_name)
        except ImportError:
            csv_path = Path(path).with_suffix(".csv")
            export_df.to_csv(csv_path, index=False)
            return str(csv_path)
        return str(path)

    def _save_gpkg(path, df, layer_name):
        export_df = gpd.GeoDataFrame(df.copy(), geometry="geometry", crs=gdf.crs)
        if OGC_REASON_FIELD in export_df.columns:
            export_df[OGC_REASON_FIELD] = export_df[OGC_REASON_FIELD].fillna("").astype(str)
        return write_output_gpkg(export_df, path, layer=layer_name)

    attr_file = None
    geom_file = None
    ogc_file = None

    if attr_count > 0:
        attr_report = output_path / f"{base_name}_duplicados_atributos.xlsx"
        attr_file = _save(attr_report, attr_duplicates, "atributos")

    if geom_count > 0:
        geom_report = output_path / f"{base_name}_duplicados_geometrias.gpkg"
        geom_file = _save_gpkg(geom_report, geom_duplicates, GEOM_DUPLICATES_LAYER)

    if ogc_invalid_count > 0:
        ogc_report = output_path / f"{base_name}_geometrias_invalidas_ogc.gpkg"
        ogc_file = _save_gpkg(ogc_report, ogc_invalid, OGC_INVALID_LAYER)

    return (
        attr_file,
        geom_file,
        ogc_file,
        attr_count,
        geom_count,
        ogc_invalid_count,
        ogc_error_summary,
    )
