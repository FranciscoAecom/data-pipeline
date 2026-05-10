from core.rules.engine import get_auto_function_mapping
from core.utils import log


def merge_function_mapping(base_mapping, new_mapping):
    merged = {column: list(funcs) for column, funcs in base_mapping.items()}

    for column, funcs in new_mapping.items():
        if column not in merged:
            merged[column] = list(funcs)
            continue

        for func in funcs:
            if func not in merged[column]:
                merged[column].append(func)

    return merged


def build_auto_mapping(columns, rule_profile):
    auto_mapping = {
        column: funcs
        for column, funcs in get_auto_function_mapping(rule_profile).items()
        if column in columns
    }
    mapping = merge_function_mapping({}, auto_mapping)

    if not auto_mapping:
        log(
            "Nenhuma auto_function configurada para o perfil ativo. "
            "Apenas funcoes obrigatorias serao executadas."
        )
    else:
        log(f"Auto_functions carregadas para {len(auto_mapping)} atributo(s).")

    return mapping
