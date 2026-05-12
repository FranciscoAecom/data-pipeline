from core.processing.context import replace_context
from core.rules.engine import load_rule_profile


def attach_rule_profile_step(context):
    rule_profile = load_rule_profile(
        context.rule_profile_name,
        optional_functions=context.optional_functions,
    )
    return replace_context(context, rule_profile=rule_profile)
