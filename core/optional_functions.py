from importlib import import_module

from projects.configs import get_project_config
from core.utils import log
from core.validation.validation_functions import validate_date_fields, validate_shapefile_attribute

CORE_OPTIONAL_FUNCTIONS = {
    "validate_date_fields": validate_date_fields,
    "validate_shapefile_attribute": validate_shapefile_attribute,
}


_PROJECT_FUNCTION_CACHE = {}


def _load_project_optional_functions(project_name):
    if project_name in _PROJECT_FUNCTION_CACHE:
        return _PROJECT_FUNCTION_CACHE[project_name]

    config = get_project_config(project_name)
    module_name = config.get("optional_function_module")
    if not module_name:
        _PROJECT_FUNCTION_CACHE[project_name] = {}
        return {}

    module = import_module(module_name)
    functions = getattr(module, "PROJECT_OPTIONAL_FUNCTIONS", {})
    _PROJECT_FUNCTION_CACHE[project_name] = dict(functions)
    return _PROJECT_FUNCTION_CACHE[project_name]


def get_optional_functions(project_name=None):
    functions = dict(CORE_OPTIONAL_FUNCTIONS)
    functions.update(_load_project_optional_functions(project_name))
    return functions


def apply_optional_functions(gdf, mapping, stats, project_name=None):
    optional_functions = get_optional_functions(project_name)

    for column, funcs in mapping.items():
        if column not in gdf.columns:
            log(f"Atributo {column} nao encontrado")
            continue

        for func_name in funcs:
            func = optional_functions.get(func_name)
            if not func:
                if project_name and project_name != "default":
                    log(f"Funcao {func_name} nao registrada para o projeto {project_name}")
                else:
                    log(f"Funcao {func_name} nao registrada")
                continue

            try:
                gdf = func(gdf, column)
                stats["optional_functions"].append(func_name)
            except Exception as exc:
                log(f"Erro em {func_name}: {exc}")

    return gdf
