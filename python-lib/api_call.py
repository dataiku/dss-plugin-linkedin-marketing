import requests
import numpy as np
import pandas as pd
from math import ceil
import logging
from datetime import datetime
from api_format import LinkedInAPIFormatter

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='LinkedIn Marketing plugin %(levelname)s - %(message)s')


def filter_query(headers: dict, granularity: str, mother: pd.DataFrame, batch_size: int = 1000, start_date: datetime = None, end_date: datetime = None) -> dict():
    """
    Handle search queries filtered by ids. The list of ids is derived from a mother database.

        Inputs:
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        account_id       ID of the sponsored ad account
        mother           Dataframe which contains the ids used to filter the query.  Ex - mother : campaign groups -> child: campaigns
        batch_size       Number of ids by batch query (ex - 100)

    Outputs:
        response         Output of the API call with the appropriate content, for ex- dateRange, impressions...
    """

    try:
        ids = mother.id.values
        response = get_query(headers, granularity=granularity, ids=ids, batch_size=batch_size, start_date=start_date, end_date=end_date)
    except AttributeError as e:
        logger.info(e)
        response = {"API_response": "No relevant output - perhaps, decrease the batch size"}
    return response


def get_query(headers: dict, granularity: str, account_id: int = 0, ids: list() = [], batch_size: int = 100, start_date: datetime = None, end_date: datetime = None):
    url, initial_params = set_up_query(granularity, ids, account_id, start_date, end_date)
    if granularity == "ACCOUNT" or granularity == "GROUP" or granularity == "CAMPAIGN" or granularity == "CREATIVES":
        params = {**initial_params, **get_analytics_parameters(ids, granularity)}
        response = query_with_pagination(url, headers, params)
    elif granularity == "CAMPAIGN_ANALYTICS" or granularity == "CREATIVES_ANALYTICS":
        response = query_per_batch(url, headers, initial_params, granularity, ids, batch_size)
    return response


def query_with_pagination(url: str, headers: dict, parameters: dict, max_entities: int = 100) -> dict():
    """
    Handle queries with pagination. Pagination is only supported for campaign groups, campaigns and creatives

    Inputs:
        url              Url used in the GET query
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        parameters       Parameters needed for the get query
        max_entities     Max entities per query

    Outputs:
        response         Output of the API call with the appropriate content, for ex- dateRange, impressions...
    """
    parameters.update({"count": str(max_entities)})
    response = query(url, headers, parameters)
    total_entities = response["paging"]["total"]
    if total_entities and total_entities > max_entities:
        for start in range(max_entities, total_entities, max_entities):
            parameters.update({"start": str(start)})
            response["elements"].extend(query(url, headers, parameters)["elements"])
    return response


def query(url: str, headers: dict, parameters: dict) -> dict():
    """
    Retrieve a json with data pulled from the API

    Inputs:
        url                 Url used in the GET query
        headers             Headers of the GET query, containing the access token for the OAuth2 identification
        parameters          Parameters needed for the get query

    Outputs:
        formatted_response  Output of the API call with the appropriate content, for ex- dateRange, impressions...
    """
    response = requests.get(url=url, headers=headers, params=parameters)
    return response.json()


def query_per_batch(url, headers, initial_params, granularity, ids, batch_size):
    count = len(ids)
    if count >= batch_size:
        response = handle_batch_query(batch_size, ids, granularity, url, headers, initial_params)
    else:
        params = {**initial_params, **get_analytics_parameters(ids, granularity)}
        response = query(url, headers, params)
    return response


def handle_batch_query(batch_size: int, ids: list(), granularity: str, url: str, headers: dict, initial_params: dict) -> dict():
    """
    Perfom a batch get query

    Inputs:
        batch_size       Number of ids by batch query (ex - 100)
        ids              Ids of the queried entities  (campaign ids, group ids...)
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        url              Url used in the GET query
        granularity      Granularity of the data : ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        initial_params   Default parameters for the query. For ex -  q: search

    Outputs:
        response         Output of the API call with the appropriate contents, for ex- dateRange, impressions...
    """
    count = len(ids)
    chunks = [*np.array_split(ids, ceil(count/batch_size))]
    response = dict({"elements": [], "exception": []})
    for batch in chunks:
        params = {**initial_params, **get_analytics_parameters(batch, granularity)}
        query_output = query(url, headers, params)
        elements = query_output.get("elements", None)
        if elements:
            response["elements"].extend(elements)
        else:
            response["exception"].append({"API_response": query_output})
            break
    return response


def get_analytics_parameters(ids: list(), granularity: str) -> dict:
    """
    Given a list of campaign ids or creative ids, returns a dictionary with a parameters' dictionary in a proper format

    Inputs:
        ids              List of campaign ids or creative ids that you want to add in the get query to retrieve their key metrics
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    Outputs:
        params           Parameters used to query the AdAnalytics LinkedIn API
    """

    params = {}
    key_prefix = {"GROUP": "", "CAMPAIGN": "search.campaignGroup.values[{}]", "CAMPAIGN_ANALYTICS": "campaigns[{}]",
                  "CREATIVES": "search.campaign.values[{}]", "CREATIVES_ANALYTICS": "creatives[{}]"}
    urn_prefix = {"GROUP": "", "CAMPAIGN": "urn:li:sponsoredCampaignGroup:", "CAMPAIGN_ANALYTICS": "urn:li:sponsoredCampaign:",
                  "CREATIVES": "urn:li:sponsoredCampaign:", "CREATIVES_ANALYTICS": "urn:li:sponsoredCreative:"}
    for i, id_value in enumerate(ids):
        params[key_prefix[granularity].format(str(i))] = urn_prefix[granularity] + str(id_value)
    return params


def check_input_values(account_id: int, headers: dict):
    """
    Check if the account and the access tokens are valid
    headers          Headers of the GET query, containing the access token for the OAuth2 identification
    account_id       ID of the sponsored ad account
    """
    account = get_query(headers, granularity="ACCOUNT")
    if "serviceErrorCode" in account.keys():
        raise ValueError(str(account))
    else:
        api_formatter = LinkedInAPIFormatter(account)
        account_df = api_formatter.format_to_df()
        if account_id not in account_df.id.values:
            raise ValueError(
                "Wrong account id or you don't have the permission to access this account")


def set_up_query(granularity: str, ids: list(), account_id: int = 0, start_date: datetime = None, end_date: datetime = None):
    url = {
        "ACCOUNT": "https://api.linkedin.com/v2/adAccountsV2",
        "GROUP": "https://api.linkedin.com/v2/adCampaignGroupsV2",
        "CAMPAIGN": "https://api.linkedin.com/v2/adCampaignsV2/",
        "CAMPAIGN_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2",
        "CREATIVES": "https://api.linkedin.com/v2/adCreativesV2/",
        "CREATIVES_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2"
    }
    initial_param = {
        "ACCOUNT": {
            "q": "search"
        },
        "GROUP": {
            "q": "search",
            "search.account.values[0]": "urn:li:sponsoredAccount:" + str(account_id)
        },
        "CAMPAIGN": {
            "q": "search"
        },
        "CREATIVES": {
            "q": "search"
        },
        "CAMPAIGN_ANALYTICS": {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "dateRange.start.day": "1",
            "dateRange.start.month": "1",
            "dateRange.start.year": "2006",
            "timeGranularity": "DAILY"
        },
        "CREATIVES_ANALYTICS": {
            "q": "analytics",
            "pivot": "CREATIVE",
            "dateRange.start.day": "1",
            "dateRange.start.month": "1",
            "dateRange.start.year": "2006",
            "timeGranularity": "DAILY"
        }
    }

    try:
        initial_param[granularity]
    except KeyError as e:
        logger.error(e)
        raise ValueError(
            "Granularity value is not valid : should be either ACCOUNT, GROUP, CAMPAIGN, CAMPAIGN_ANALYTICS, CREATIVES or CREATIVES_ANALYTICS")

    if granularity == "CAMPAIGN_ANALYTICS" or granularity == "CREATIVES_ANALYTICS":
        initial_param = date_filter(granularity, initial_param, start_date, end_date)

    url = url[granularity]
    params = initial_param[granularity]
    return url, params


def date_filter(granularity: str, initial_param: dict(), start_date: datetime, end_date: datetime):
    if start_date and end_date:
        if start_date.year < 2006 or start_date > datetime.now():
            raise ValueError("Please select a valid start date")
        if start_date > end_date:
            raise ValueError("Please select a valid time range")

    if start_date:
        initial_param[granularity]["dateRange.start.day"] = start_date.day
        initial_param[granularity]["dateRange.start.month"] = start_date.month
        initial_param[granularity]["dateRange.start.year"] = start_date.year
    if end_date:
        initial_param[granularity]["dateRange.end.day"] = end_date.day
        initial_param[granularity]["dateRange.end.month"] = end_date.month
        initial_param[granularity]["dateRange.end.year"] = end_date.year
    return initial_param
