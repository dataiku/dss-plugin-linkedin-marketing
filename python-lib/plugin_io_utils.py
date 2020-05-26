def build_column_names(query:dict)->list():
    try:
        content_columns = query["elements"]
        column_names = list(content_columns[0].keys()).append("API_response")
    except KeyError:
        column_names = ["API_response"]
    return column_names



