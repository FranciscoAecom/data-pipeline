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
        "display_name": "Areas de Preservacao Permanentes (APP) nos imoveis rurais",
        "theme_prefixes": ("app_car_",),
        "output_name_template": "pol_pcd_{theme_folder}_{date_yyyymmdd}",
        "reference_date": "20260301",
        "optional_function_module": "projects.functions.app_car",
    },
    "estado": {
        "project_name": "estado",
        "display_name": "Limites das unidades da federacao do Brasil",
        "theme_prefixes": ("estado",),
        "output_name_template": "pol_loc_{theme_folder}_{date_yyyymmdd}",
        "reference_date": "20241215",
        "optional_function_module": None,
    },
    "reserva_legal_car": {
        "project_name": "reserva_legal_car",
        "display_name": "Reserva Legal (RL) nos imoveis rurais",
        "theme_prefixes": ("rl_car_",),
        "output_name_template": "pol_pcd_{theme_folder}_{date_yyyymmdd}",
        "reference_date": "20260301",
        "optional_function_module": "projects.functions.reserva_legal_car",
    },

    "autorizaçao_para_supressao_vegetal": {
        "project_name": "autorizaçao_para_supressao_vegetal",
        "display_name": "Autorização para Supressão Vegetal",
        "theme_prefixes": ("auth_supn",),
        "output_name_template": "pol_env_{theme_folder}_{date_yyyymmdd}",
        "reference_date": "20250701",
        "optional_function_module": "projects.functions.date",
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
