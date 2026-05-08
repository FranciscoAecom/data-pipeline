from dataclasses import dataclass

import geopandas as gpd


@dataclass(frozen=True)
class ProcessRecordResult:
    processed_count: int
    output_path: str | None
    final_gdf: gpd.GeoDataFrame | None


def failure_result():
    return ProcessRecordResult(0, None, None)


def success_result(context):
    return ProcessRecordResult(
        len(context.final_gdf),
        context.output_path,
        context.final_gdf,
    )
