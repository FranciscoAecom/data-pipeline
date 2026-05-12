from core.processing.context import replace_context
from core.input.preparation import load_and_prepare_input


def load_input_step(context):
    return replace_context(context, gdf=load_and_prepare_input(context.record))
