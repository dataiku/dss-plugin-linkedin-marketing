def build_column_names(query: dict) -> list():
    content_columns = query.get("elements", None)
    if content_columns:
        column_names = list(content_columns[0].keys())
    else:
        column_names = ["exception"]
    return column_names
