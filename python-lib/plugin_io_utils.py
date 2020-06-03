def build_column_names(query:dict)->list():
    try:
        content_columns = query["elements"]
        if content_columns:
            column_names = list(content_columns[0].keys()).append("API_response")
        else:
            column_names = []
    except KeyError:
        column_names = ["API_response","id"]
    return column_names



