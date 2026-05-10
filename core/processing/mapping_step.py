from core.execution_context import replace_context
from core.rules.runtime import build_auto_mapping
from core.validation.attribute_mapping import prepare_validate_shapefile_attribute_mappings


def prepare_mapping_step(context):
    mapping = build_auto_mapping(list(context.gdf.columns), context.rule_profile)
    prepare_validate_shapefile_attribute_mappings(
        context.gdf,
        mapping,
        context.rule_profile,
        validation_session=getattr(context, "validation_session", None),
    )
    return replace_context(context, mapping=mapping)
