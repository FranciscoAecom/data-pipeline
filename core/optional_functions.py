from core.utils import log
from core.date import validate_date_fields
from core.pipeline_operations import build_pipeline_operation
from core.validation.validation_functions import validate_shapefile_attribute

_QUALIFIED_FUNCTION_CACHE = {}
CORE_OPTIONAL_FUNCTIONS = {
    "validate_date_fields": validate_date_fields,
    "validate_shapefile_attribute": validate_shapefile_attribute,
}


def get_optional_functions(project_name=None):
    return dict(CORE_OPTIONAL_FUNCTIONS)


def get_registered_optional_function_names(optional_functions=None):
    if optional_functions is None:
        optional_functions = get_optional_functions()
    return set(optional_functions.keys())


def is_optional_function_registered(func_name, optional_functions=None):
    optional_functions = optional_functions or get_optional_functions()
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
    from importlib import import_module

    module = import_module(module_name)
    func = getattr(module, function_name, None)
    if func:
        _QUALIFIED_FUNCTION_CACHE[func_name] = func
    return func


def resolve_pipeline_operation(func_name, optional_functions, source_column=None):
    func = _resolve_optional_function(func_name, optional_functions)
    if not func:
        return None
    return build_pipeline_operation(
        func_name,
        func,
        source_column=source_column,
    )


def build_pipeline_operations(mapping, optional_functions=None, project_name=None):
    optional_functions = optional_functions or get_optional_functions(project_name)
    operations = {}
    for column, funcs in mapping.items():
        column_operations = []
        for func_name in funcs:
            operation = resolve_pipeline_operation(
                func_name,
                optional_functions,
                source_column=column,
            )
            if operation:
                column_operations.append(operation)
        if column_operations:
            operations[column] = column_operations
    return operations


def apply_optional_functions(
    gdf,
    mapping,
    stats,
    project_name=None,
    optional_functions=None,
    **context,
):
    optional_functions = optional_functions or get_optional_functions(project_name)

    for column, funcs in mapping.items():
        if column not in gdf.columns:
            log(f"Atributo {column} nao encontrado")
            continue

        for func_name in funcs:
            operation = resolve_pipeline_operation(
                func_name,
                optional_functions,
                source_column=column,
            )
            if not operation:
                if project_name and project_name != "default":
                    log(f"Funcao {func_name} nao registrada para o projeto {project_name}")
                else:
                    log(f"Funcao {func_name} nao registrada")
                continue

            try:
                gdf = operation.execute(gdf, column, **context)
                stats["optional_functions"].append(operation.name)
            except Exception as exc:
                log(f"Erro em {func_name}: {exc}")

    return gdf
