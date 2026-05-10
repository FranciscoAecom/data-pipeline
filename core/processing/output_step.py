from core.execution_context import replace_context
from core.output.persistence import save_outputs


def persist_outputs_step(
    context,
    use_configured_final_name=False,
    persist_dataset=True,
):
    output_path = save_outputs(
        context.final_gdf,
        context.record,
        context.output_dir,
        use_configured_final_name=use_configured_final_name,
        persist_dataset=persist_dataset,
    )
    return replace_context(context, output_path=output_path)
