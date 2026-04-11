import geopandas as gpd

from core.transforms.attribute_transforms import add_sequential_id
from core.optional_functions import apply_optional_functions
from settings import DEFAULT_INPUT_CRS
from core.spatial.spatial_functions import (
    add_centroid_coordinates,
    calculate_area_hectares,
    calculate_perimeter_km,
    force_geometry_2d,
    reproject_shapefile,
)
from core.utils import log


MANDATORY_FUNCTIONS = [
    "reproject_shapefile",
    "force_geometry_2d",
    "add_sequential_id",
    "calculate_area_hectares",
    "calculate_perimeter_km",
    "add_centroid_coordinates",
]


def _ensure_geodataframe(gdf):
    if isinstance(gdf, gpd.GeoDataFrame):
        return gdf
    return gpd.GeoDataFrame(gdf, geometry="geometry")


def _ensure_crs(gdf):
    if gdf.crs is None:
        log(f"CRS nao definido. Assumindo {DEFAULT_INPUT_CRS}")
        gdf = gdf.set_crs(DEFAULT_INPUT_CRS)
    return gdf


def run_pipeline(gdf, mapping, id_start=1, project_name=None):
    stats = {
        "optional_functions": [],
        "forced_to_2d": 0,
        "reprojected_to_wgs84": 0,
    }

    gdf = _ensure_geodataframe(gdf)
    gdf = _ensure_crs(gdf)

    gdf, reprojected = reproject_shapefile(gdf)
    stats["reprojected_to_wgs84"] = int(reprojected)

    gdf, forced_to_2d = force_geometry_2d(gdf)
    stats["forced_to_2d"] = int(forced_to_2d)

    gdf = add_sequential_id(gdf, start=id_start)
    gdf = calculate_area_hectares(gdf)
    gdf = calculate_perimeter_km(gdf)
    gdf = add_centroid_coordinates(gdf)

    if mapping:
        gdf = apply_optional_functions(gdf, mapping, stats, project_name=project_name)

    return _ensure_geodataframe(gdf), stats
