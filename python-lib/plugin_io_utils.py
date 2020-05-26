from api_call import get_query
from api_format import LinkedInAPIFormatter


def build_column_names(query:dict)->list():
    try:
        content_columns = query["elements"]
        column_names = list(content_columns[0].keys()).append("API_response")
    except KeyError:
        column_names = ["API_response"]
    return column_names

def check_account_id(account_id:int):
    account = get_query(HEADERS, granularity = "ACCOUNT")
    api_formatter = LinkedInAPIFormatter(group)
    account_df = api_formatter.format_to_df()
    if account_id not in account_df.id.values:
        raise Exception("Wrong account id or not accessible with the current access token")