from importlib import import_module

from core.optional_functions import get_optional_functions

_PROJECT_FUNCTIONS_CACHE = {}


def _load_project_functions(project_name):
    if not project_name or project_name == "default":
        return {}
    if project_name in _PROJECT_FUNCTIONS_CACHE:
        return _PROJECT_FUNCTIONS_CACHE[project_name]

    try:
        project_module = import_module(f"projects.functions.{project_name}")
    except ModuleNotFoundError:
        _PROJECT_FUNCTIONS_CACHE[project_name] = {}
        return {}

    project_functions = getattr(project_module, "PROJECT_OPTIONAL_FUNCTIONS", {})
    if not isinstance(project_functions, dict):
        project_functions = {}

    _PROJECT_FUNCTIONS_CACHE[project_name] = dict(project_functions)
    return _PROJECT_FUNCTIONS_CACHE[project_name]


def get_project_optional_functions(project_name=None):
    functions = get_optional_functions()
    functions.update(_load_project_functions(project_name))
    return functions
