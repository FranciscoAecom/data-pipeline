def project_name(context):
    if hasattr(context, "project_name"):
        return context.project_name
    return context.project_config["project_name"]
