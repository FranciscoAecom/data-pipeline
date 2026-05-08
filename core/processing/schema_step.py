from core.execution_context import replace_context
from core.utils import log
from core.validation.tabular_schema import get_tabular_schema, normalize_input_schema


def validate_input_schema_step(context):
    if get_tabular_schema(context.rule_profile) is None:
        return context

    gdf, errors = normalize_input_schema(
        context.record,
        context.gdf,
        context.rule_profile,
    )
    if errors:
        message = "\n".join(f"- {error}" for error in errors)
        raise ValueError(
            f"Schema tabular invalido para {context.record.theme_folder}:\n{message}"
        )

    log(f"Validacao de schema tabular OK para {context.record.theme_folder}.")
    return replace_context(context, gdf=gdf)
