import os

from core.naming import build_final_output_base_name, build_theme_output_dir


def build_processing_group_key(record):
    return (
        record.sheet_row,
        str(record.record_id),
        str(record.theme_folder),
        str(record.source_path),
    )


def build_group_merged_output_path(record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    return os.path.join(theme_output_dir, f"{build_final_output_base_name(record)}.gpkg")


def build_group_log_path(record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    base_name = build_final_output_base_name(record)
    return os.path.join(theme_output_dir, f"{base_name}.txt")


def resolve_output_path(record, output_dir, use_configured_final_name):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)

    if use_configured_final_name:
        base_name = build_final_output_base_name(record)
    else:
        base_name = os.path.splitext(os.path.basename(record.input_path))[0] + "_validado"

    output_path = os.path.join(theme_output_dir, f"{base_name}.gpkg")
    return theme_output_dir, base_name, output_path
