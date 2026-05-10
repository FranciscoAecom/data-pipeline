from core.io.dataset import write_output_gpkg
from core.output.columns import drop_internal_output_columns
from core.output.paths import build_group_merged_output_path
from core.utils import log


def append_group_consolidated_output(record, final_gdf, output_dir, append=False):
    merged_output_path = build_group_merged_output_path(record, output_dir)
    export_gdf = drop_internal_output_columns(final_gdf)
    if append:
        log(f"Acrescentando resultado ao consolidado {merged_output_path}")
    else:
        log(f"Criando consolidado {merged_output_path}")
    write_output_gpkg(
        export_gdf,
        merged_output_path,
        append=append,
        overwrite_existing=not append,
    )
    return merged_output_path
