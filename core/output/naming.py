import os
import re
from datetime import datetime

from projects.configs import resolve_project_config


def sanitize_output_name(value):
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", str(value).strip())
    sanitized = re.sub(r"\s+", "_", sanitized)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_") or "saida"


def build_theme_output_dir(base_output_dir, theme_folder):
    folder_name = str(theme_folder).strip() or "sem_theme_folder"
    folder_name = re.sub(r'[<>:"/\\|?*]', "_", folder_name)
    return os.path.join(base_output_dir, folder_name)


def resolve_output_name_template(theme_folder):
    project_config = resolve_project_config(theme_folder)
    return project_config["output_name_template"]


def resolve_output_reference_date(theme_folder):
    project_config = resolve_project_config(theme_folder)
    return project_config["reference_date"]


def build_final_output_base_name(record):
    input_stem = os.path.splitext(os.path.basename(record.input_path))[0]
    source_stem = os.path.basename(os.path.normpath(str(record.source_path))) or input_stem
    rule_profile = str(record.rule_profile).strip()
    theme_folder = str(record.theme_folder).strip()
    state_code = theme_folder.split("_")[-1].lower() if "_" in theme_folder else theme_folder.lower()
    date_yyyymmdd = resolve_output_reference_date(theme_folder) or datetime.now().strftime("%Y%m%d")

    template = resolve_output_name_template(theme_folder)
    rendered = template.format(
        input_stem=sanitize_output_name(input_stem),
        source_stem=sanitize_output_name(source_stem),
        rule_profile=sanitize_output_name(rule_profile),
        theme_folder=sanitize_output_name(theme_folder),
        state_code=sanitize_output_name(state_code),
        date_yyyymmdd=date_yyyymmdd,
    )
    return sanitize_output_name(rendered)


__all__ = [
    "build_final_output_base_name",
    "build_theme_output_dir",
    "resolve_output_name_template",
    "resolve_output_reference_date",
    "sanitize_output_name",
]
