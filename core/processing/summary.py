from core.utils import log


def log_dataset_overview(gdf):
    columns = list(gdf.columns)
    log(f"Atributos encontrados: {len(columns)}")
    for column in columns:
        log(f"  - {column} ({gdf[column].dtype})")
