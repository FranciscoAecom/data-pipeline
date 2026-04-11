from functools import lru_cache


DEFAULT_PROJECT_CONFIG = {
    "project_name": "default",
    "theme_prefixes": (),
    "output_name_template": "{input_stem}_validado",
    "reference_date": None,
    "optional_function_module": None,
}


PROJECT_CONFIGS = {
    "app_car": {
        "project_name": "app_car",
        "theme_prefixes": ("app_car_",),
        "output_name_template": "pol_pcd_{theme_folder}_{date_yyyymmdd}",
        "reference_date": "20260301",
        "optional_function_module": "projects.functions.app_car",
    },
}


@lru_cache(maxsize=None)
def get_project_config(project_name=None):
    if project_name and project_name in PROJECT_CONFIGS:
        config = dict(DEFAULT_PROJECT_CONFIG)
        config.update(PROJECT_CONFIGS[project_name])
        return config
    return dict(DEFAULT_PROJECT_CONFIG)


def resolve_project_name(theme_folder):
    theme_folder_text = str(theme_folder or "").strip().lower()
    for project_name, config in PROJECT_CONFIGS.items():
        for prefix in config.get("theme_prefixes", ()):
            if theme_folder_text.startswith(str(prefix).lower()):
                return project_name
    return DEFAULT_PROJECT_CONFIG["project_name"]


def resolve_project_config(theme_folder):
    project_name = resolve_project_name(theme_folder)
    return get_project_config(project_name)
