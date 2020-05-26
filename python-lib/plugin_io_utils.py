def build_column_names(query:dict)->list():
    try:
        content_columns = query["elements"]
        column_names = list(content_columns[0].keys()).append("API_response")
    except KeyError:
        column_names = ["API_response"]
    return column_names

def check_account_id(account_id:int,accounts:list()):
    if account_id not in accounts:
        raise Exception("Wrong account id or not accessible with the current access token")