def is_source_column(column):
    return str(column).startswith("sdb_")


def target_column_name(column):
    column_text = str(column)
    if is_source_column(column_text):
        return f"acm_{column_text[4:]}"
    return f"acm_{column_text}"


__all__ = ["is_source_column", "target_column_name"]
