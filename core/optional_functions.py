from importlib import import_module

from core.date import validate_date_fields
from core.utils import log
from core.validation.validation_functions import validate_shapefile_attribute

CORE_OPTIONAL_FUNCTIONS = {
    "validate_date_fields": validate_date_fields,
    "validate_shapefile_attribute": validate_shapefile_attribute,
}

_QUALIFIED_FUNCTION_CACHE = {}


def get_optional_functions(project_name=None):
    functions = dict(CORE_OPTIONAL_FUNCTIONS)
    if not project_name or project_name == "default":
        return functions

    try:
        project_module = import_module(f"projects.functions.{project_name}")
    except ModuleNotFoundError:
        return functions

    project_functions = getattr(project_module, "PROJECT_OPTIONAL_FUNCTIONS", {})
    if isinstance(project_functions, dict):
        functions.update(project_functions)
    return functions


def get_registered_optional_function_names(project_name=None):
    return set(get_optional_functions(project_name).keys())


def is_optional_function_registered(func_name, project_name=None):
    optional_functions = get_optional_functions(project_name)
    return _resolve_optional_function(func_name, optional_functions) is not None


def _resolve_optional_function(func_name, optional_functions):
    func = optional_functions.get(func_name)
    if func:
        return func

    if "." not in str(func_name):
        return None

    if func_name in _QUALIFIED_FUNCTION_CACHE:
        return _QUALIFIED_FUNCTION_CACHE[func_name]

    module_name, function_name = str(func_name).rsplit(".", 1)
    module = import_module(module_name)
    func = getattr(module, function_name, None)
    if func:
        _QUALIFIED_FUNCTION_CACHE[func_name] = func
    return func


def apply_optional_functions(gdf, mapping, stats, project_name=None):
    optional_functions = get_optional_functions(project_name)

    for column, funcs in mapping.items():
        if column not in gdf.columns:
            log(f"Atributo {column} nao encontrado")
            continue

        for func_name in funcs:
            func = _resolve_optional_function(func_name, optional_functions)
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
