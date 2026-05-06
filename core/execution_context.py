from dataclasses import dataclass, is_dataclass, replace
from types import SimpleNamespace

import geopandas as gpd


@dataclass(frozen=True)
class ProcessingContext:
    record: object
    output_dir: str
    project_config: dict
    rule_profile_name: str
    rule_profile: dict | None
    optional_functions: dict
    id_start: int = 1
    gdf: gpd.GeoDataFrame | None = None
    final_gdf: gpd.GeoDataFrame | None = None
    mapping: dict | None = None
    output_path: str | None = None

    @property
    def project_name(self):
        return self.project_config["project_name"]


ProcessingExecutionContext = ProcessingContext


def replace_context(context, **changes):
    if is_dataclass(context):
        return replace(context, **changes)
    return SimpleNamespace(**{**context.__dict__, **changes})
