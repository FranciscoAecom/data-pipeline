from settings import DEFAULT_RULE_PROFILE
from core.rules.normalization import normalize_profile_name


def _validate_component_errors(component_name, errors):
    if errors:
        message = "\n".join(f"- {error}" for error in errors)
        raise ValueError(f"Componente '{component_name}' invalido:\n{message}")


def validate_profile_component(profile, normalized_profile_name):
    errors = []
    if not profile:
        errors.append("profile.json deve conter metadados do perfil.")
    if "profile_name" not in profile:
        errors.append("Campo 'profile_name' e obrigatorio em profile.json.")
    if "theme_folder" not in profile:
        errors.append("Campo 'theme_folder' e obrigatorio em profile.json.")
    _validate_profile_name_entry(profile, normalized_profile_name, errors)
    _validate_theme_folder_entry(profile, normalized_profile_name, errors)
    _validate_project_name_entry(profile, errors)
    _validate_component_errors("profile.json", errors)


def validate_input_schema_component(input_schema):
    errors = []

    if not input_schema:
        return

    columns = input_schema.get("columns", {})
    if not isinstance(columns, dict):
        errors.append("Campo 'columns' deve ser um objeto JSON.")
        columns = {}

    for column, rule in columns.items():
        if not isinstance(column, str) or not column.strip():
            errors.append("Chaves de 'columns' devem ser strings nao vazias.")
            continue

        if isinstance(rule, str):
            if not rule.strip():
                errors.append(f"Tipo de '{column}' deve ser uma string nao vazia.")
            continue

        if not isinstance(rule, dict):
            errors.append(f"Regra de coluna '{column}' deve ser string ou objeto JSON.")
            continue

        dtype = rule.get("dtype", "string")
        if not isinstance(dtype, str) or not dtype.strip():
            errors.append(f"'dtype' de '{column}' deve ser uma string nao vazia.")

        for key in ("required", "nullable"):
            value = rule.get(key, True)
            if not isinstance(value, bool):
                errors.append(f"'{key}' de '{column}' deve ser booleano.")

    for key in ("require_geometry", "allow_extra_columns"):
        value = input_schema.get(key, True)
        if not isinstance(value, bool):
            errors.append(f"Campo '{key}' deve ser booleano.")

    _validate_component_errors("input_schema.json", errors)


def validate_domains_component(domains):
    errors = []
    fields = domains.get("fields", domains)
    _validate_fields_entry(fields, errors)
    _validate_component_errors("domains.json", errors)


def validate_relations_component(relations, fields):
    errors = []
    relation_entries = relations.get("relations", relations)
    _validate_relations_entry(relation_entries, fields, errors)
    _validate_component_errors("relations.json", errors)


def validate_pipeline_component(pipeline, fields):
    errors = []
    auto_functions = pipeline.get("auto_functions", pipeline)
    _validate_auto_functions_entry(auto_functions, fields, errors)
    _validate_component_errors("pipeline.json", errors)


def validate_modular_components(
    profile,
    input_schema,
    domains,
    relations,
    pipeline,
    normalized_profile_name,
):
    validate_profile_component(profile, normalized_profile_name)
    validate_input_schema_component(input_schema)
    validate_domains_component(domains)

    fields = domains.get("fields", domains)
    validate_relations_component(relations, fields)
    validate_pipeline_component(pipeline, fields)


def validate_rule_profile_structure(profile, profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not isinstance(profile, dict):
        raise ValueError(
            f"Perfil de regras invalido '{normalized_profile_name}': o conteudo deve ser um objeto JSON."
        )

    errors = []
    _validate_profile_name_entry(profile, normalized_profile_name, errors)
    _validate_theme_folder_entry(profile, normalized_profile_name, errors)
    _validate_project_name_entry(profile, errors)
    _validate_fields_entry(profile.get("fields", {}), errors)
    _validate_input_schema_entry(profile.get("input_schema", {}), errors)
    _validate_relations_shape(profile.get("relations", {}), errors)
    _validate_auto_functions_shape(profile.get("auto_functions", {}), errors)
    _raise_profile_errors(normalized_profile_name, errors)


def validate_rule_profile_semantics(profile, profile_name, optional_functions=None):
    normalized_profile_name = normalize_profile_name(profile_name)
    errors = []
    fields = profile.get("fields", {})
    _validate_relations_entry(profile.get("relations", {}), fields, errors)
    _validate_auto_functions_entry(
        profile.get("auto_functions", {}),
        fields,
        errors,
        optional_functions=optional_functions,
    )
    _raise_profile_errors(normalized_profile_name, errors)


def _validate_profile_name_entry(profile, normalized_profile_name, errors):
    profile_name = profile.get("profile_name")
    if profile_name is None:
        return

    if not isinstance(profile_name, str) or not profile_name.strip():
        errors.append("Campo 'profile_name' deve ser uma string nao vazia.")


def _validate_theme_folder_entry(profile, normalized_profile_name, errors):
    theme_folder = profile.get("theme_folder")
    if theme_folder is None:
        return

    normalized_theme_folder = normalize_profile_name(theme_folder)
    profile_stem = normalized_profile_name.rsplit("/", 1)[-1]

    if not normalized_theme_folder:
        errors.append("Campo 'theme_folder' deve ser uma string nao vazia.")
    elif normalized_theme_folder != profile_stem:
        errors.append(
            f"Campo 'theme_folder' deve ser '{profile_stem}' quando informado."
        )


def _validate_project_name_entry(profile, errors):
    project_name = profile.get("project_name")
    if not isinstance(project_name, str) or not project_name.strip():
        errors.append("Campo 'project_name' deve ser uma string nao vazia.")
        return DEFAULT_RULE_PROFILE

    return project_name.strip()


def _validate_fields_entry(fields, errors):
    if fields is None:
        return {}
    if not isinstance(fields, dict):
        errors.append("Campo 'fields' deve ser um objeto JSON.")
        return {}

    for field_name, field_rules in fields.items():
        if not isinstance(field_name, str) or not field_name.strip():
            errors.append("Chaves de 'fields' devem ser strings nao vazias.")
            continue

        if not isinstance(field_rules, dict):
            errors.append(f"Configuracao de field '{field_name}' deve ser um objeto.")
            continue

        accepted_values = field_rules.get("accepted_values", [])
        aliases = field_rules.get("aliases", {})

        if accepted_values is None:
            accepted_values = []
        if not isinstance(accepted_values, list):
            errors.append(f"'accepted_values' de '{field_name}' deve ser uma lista.")
            accepted_values = []

        if not all(isinstance(value, str) for value in accepted_values):
            errors.append(
                f"'accepted_values' de '{field_name}' deve conter apenas strings."
            )

        if aliases is None:
            aliases = {}
        if not isinstance(aliases, dict):
            errors.append(f"'aliases' de '{field_name}' deve ser um objeto.")
            aliases = {}

        if not all(isinstance(key, str) for key in aliases.keys()):
            errors.append(f"'aliases' de '{field_name}' deve usar chaves string.")
        if not all(isinstance(value, str) for value in aliases.values()):
            errors.append(f"'aliases' de '{field_name}' deve usar valores string.")

        accepted_values_set = set(accepted_values)
        for alias, canonical in aliases.items():
            if not isinstance(alias, str) or not isinstance(canonical, str):
                continue
            if canonical not in accepted_values_set:
                errors.append(
                    f"Alias '{alias}' de '{field_name}' aponta para valor fora de 'accepted_values': {canonical}."
                )

    return fields


def _validate_input_schema_entry(input_schema, errors):
    if not input_schema:
        return
    try:
        validate_input_schema_component(input_schema)
    except ValueError as exc:
        errors.append(str(exc))


def _validate_relations_shape(relations, errors):
    if relations is None:
        return
    if not isinstance(relations, dict):
        errors.append("Campo 'relations' deve ser um objeto JSON.")
        return
    for relation_name, relation_mapping in relations.items():
        if not isinstance(relation_name, str) or not relation_name.strip():
            errors.append("Chaves de 'relations' devem ser strings nao vazias.")
            continue
        if not isinstance(relation_mapping, dict):
            errors.append(f"Relacao '{relation_name}' deve ser um objeto.")
            continue
        for source_value, target_value in relation_mapping.items():
            if not isinstance(source_value, str) or not isinstance(target_value, str):
                errors.append(
                    f"Relacao '{relation_name}' deve conter apenas pares string -> string."
                )
                break


def _validate_auto_functions_shape(auto_functions, errors):
    if auto_functions is None:
        return
    if not isinstance(auto_functions, dict):
        errors.append("Campo 'auto_functions' deve ser um objeto JSON.")
        return
    for column, functions in auto_functions.items():
        if not isinstance(column, str) or not column.strip():
            errors.append("Chaves de 'auto_functions' devem ser strings nao vazias.")
            continue
        if not isinstance(functions, list) or not functions:
            errors.append(
                f"'auto_functions.{column}' deve ser uma lista nao vazia de funcoes."
            )
            continue
        for func_name in functions:
            if not isinstance(func_name, str) or not func_name.strip():
                errors.append(
                    f"'auto_functions.{column}' deve conter apenas nomes de funcao string."
                )


def _validate_relations_entry(relations, fields, errors):
    if relations is None:
        return
    if not isinstance(relations, dict):
        errors.append("Campo 'relations' deve ser um objeto JSON.")
        return

    known_fields = set(fields.keys())
    for relation_name, relation_mapping in relations.items():
        if not isinstance(relation_name, str) or not relation_name.strip():
            errors.append("Chaves de 'relations' devem ser strings nao vazias.")
            continue

        if not isinstance(relation_mapping, dict):
            errors.append(f"Relacao '{relation_name}' deve ser um objeto.")
            continue

        if "_to_" not in relation_name:
            errors.append(
                f"Relacao '{relation_name}' deve seguir o padrao '<origem>_to_<destino>'."
            )
        else:
            source_token, target_token = relation_name.split("_to_", 1)
            source_field = f"sdb_{source_token}"
            target_field = f"sdb_{target_token}"
            if known_fields and source_field not in known_fields and source_token not in known_fields:
                errors.append(
                    f"Relacao '{relation_name}' referencia campo de origem nao configurado: {source_field}."
                )
            if known_fields and target_field not in known_fields and target_token not in known_fields:
                errors.append(
                    f"Relacao '{relation_name}' referencia campo de destino nao configurado: {target_field}."
                )

        for source_value, target_value in relation_mapping.items():
            if not isinstance(source_value, str) or not isinstance(target_value, str):
                errors.append(
                    f"Relacao '{relation_name}' deve conter apenas pares string -> string."
                )
                break


def _validate_auto_functions_entry(auto_functions, fields, errors, optional_functions=None):
    if auto_functions is None:
        return
    if not isinstance(auto_functions, dict):
        errors.append("Campo 'auto_functions' deve ser um objeto JSON.")
        return

    known_fields = set(fields.keys())
    for column, functions in auto_functions.items():
        if not isinstance(column, str) or not column.strip():
            errors.append("Chaves de 'auto_functions' devem ser strings nao vazias.")
            continue

        if not isinstance(functions, list) or not functions:
            errors.append(
                f"'auto_functions.{column}' deve ser uma lista nao vazia de funcoes."
            )
            continue

        for func_name in functions:
            if not isinstance(func_name, str) or not func_name.strip():
                errors.append(
                    f"'auto_functions.{column}' deve conter apenas nomes de funcao string."
                )
                continue

            if (
                optional_functions is not None
                and func_name not in optional_functions
                and _resolve_qualified_function(func_name) is None
            ):
                errors.append(
                    f"Funcao '{func_name}' em 'auto_functions.{column}' nao esta registrada."
                )
                continue

            if (
                func_name == "validate_shapefile_attribute"
                and known_fields
                and column not in known_fields
            ):
                errors.append(
                    f"Campo '{column}' usa 'validate_shapefile_attribute' mas nao possui configuracao correspondente em 'fields'."
                )


def _resolve_qualified_function(func_name):
    if "." not in str(func_name):
        return None
    try:
        from importlib import import_module

        module_name, function_name = str(func_name).rsplit(".", 1)
        module = import_module(module_name)
    except ModuleNotFoundError:
        return None
    return getattr(module, function_name, None)


def validate_rule_profile(profile, profile_name, optional_functions=None):
    normalized_profile_name = normalize_profile_name(profile_name)
    validate_rule_profile_structure(profile, normalized_profile_name)
    validate_rule_profile_semantics(
        profile,
        normalized_profile_name,
        optional_functions=optional_functions,
    )


def _raise_profile_errors(normalized_profile_name, errors):
    if errors:
        message = "\n".join(f"- {error}" for error in errors)
        raise ValueError(
            f"Perfil de regras invalido '{normalized_profile_name}':\n{message}"
        )
