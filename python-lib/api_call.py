import requests
import numpy as np
import pandas as pd
from math import ceil
import logging
from datetime import datetime
from api_format import format_to_df
from constants import Constants

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")


def check_params(headers: dict, account_id: int, start_date: datetime, end_date: datetime):
    """Check if the account id and the access tokens are valid

    :param dict headers:  Headers of the GET query, it contains the access token for the OAuth2 identification
    :param int account_id:  ID of the sponsored ad account
    :param datetime start_date:  First day of the chosen time range (None for all time)
    :param datetime end_date:  Last day of the time range (None for today)
    :raises: :class:`ValueError`: Invalid parameters
    """
    account = query_ads(headers, category="ACCOUNT", account_id=account_id)
    if "serviceErrorCode" in account.keys():
        raise ValueError(str(account))
    else:
        account_df = format_to_df(account)
        if account_id not in account_df.id.values:
            raise ValueError("Wrong account id or you don't have the permission to access this account")

    if start_date:
        if start_date.year < 2006 or start_date > datetime.now():
            raise ValueError("Please select a valid start date")
        if end_date:
            if start_date > end_date:
                raise ValueError("Please select a valid time range")
    if end_date:
        if end_date.year < 2006:
            raise ValueError("Please select a valid end date")


def query_ads(headers: dict, category: str, account_id: int) -> dict:
    """Query the LinkedIn ad API. LinkedIn ad handles pagination

    :param str category: granularity of the data that you want to get -> ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    :returns: Response of the API
    :rtype: dict
    """
    url, params = set_up_query(category, account_id)
    response = query_with_pagination(url, headers, params)
    return response


def query_ad_analytics(headers: dict, category: str, parent: pd.DataFrame, batch_size: int = Constants.DEFAULT_BATCH_SIZE, start_date: datetime = None, end_date: datetime = None) -> dict:
    """Query the ad analytics API. As it doesn't handle pagination, a batch query is performed.
    The batch size indicates how many entities (campaigns or creatives) should be returned by batch query.

    :param pd.DataFrame parent: Dataframe which contains the ids used to filter the query.  Ex - parent : campaign -> child: campaign_analytics
    :param int batch_size: number of ids by batch query (ex - 100)
    :raises: :class:`ValueError`: The parent dataframe is missing

    :returns: Response of the API
    :rtype: dict
    """
    if not parent.empty:
        url, initial_params = set_up_query(category)
        initial_params = date_filter(initial_params, start_date, end_date)
        ids = parent["id"].values
        count = len(ids)
        if count >= batch_size:
            response = query_by_batch(batch_size, ids, category, url, headers, initial_params)
        else:
            params = {**initial_params, **get_analytics_parameters(ids, category)}
            response = query(url, headers, params)
    else:
        raise ValueError("The parent dataframe is empty")
    return response


def query_with_pagination(url: str, headers: dict, parameters: dict, page_size: int = 100) -> dict:
    """Handles queries with pagination. Pagination is only supported for campaign groups, campaigns and creatives

    :param str url: Url used for the GET query
    :param dict parameters: Parameters used for the GET query
    :param int page_size: Max entities per query (set to 100 in the case of the LinkedIn API)

    :returns: Response of the API
    :rtype: dict
    """
    parameters.update({"count": str(page_size)})
    response = query(url, headers, parameters)
    total_entities = response["paging"]["total"]
    if total_entities and total_entities > page_size:
        for start in range(page_size, total_entities, page_size):
            parameters.update({"start": str(start)})
            response["elements"].extend(query(url, headers, parameters)["elements"])
    return response


def query_by_batch(batch_size: int, ids: list, category: str, url: str, headers: dict, initial_params: dict) -> dict:
    """
    Perfom a batch get query

    Inputs:
        batch_size       Number of ids by batch query (ex - 100)
        ids              Ids of the queried entities  (campaign ids, group ids...)
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        url              Url used in the GET query
        category         category of the data : ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        initial_params   Default parameters for the query. For ex -  q: search

    Outputs:
        response         Output of the API call with the appropriate contents, for ex- dateRange, impressions...
    """
    count = len(ids)
    chunks = [*np.array_split(ids, ceil(count/batch_size))]
    response = {"elements": [], "exception": []}
    for chunk in chunks:
        params = {**initial_params, **get_analytics_parameters(chunk, category)}
        query_output = query(url, headers, params)
        elements = query_output.get("elements", None)
        if elements:
            response["elements"].extend(elements)
        elif "elements" in query_output:
            response["exception"].append({"Empty output": query_output})
        else:
            response = {**{"Hint": "consider decreasing the sample size"}, **query_output}
            break
    return response


def query(url: str, headers: dict, parameters: dict) -> dict:
    """
    Retrieve a json with data pulled from the API

    Inputs:
        url                 Url used in the GET query
        headers             Headers of the GET query, containing the access token for the OAuth2 identification
        parameters          Parameters needed for the get query

    Outputs:
        response            Output of the API call with the appropriate content, for ex- dateRange, impressions...
    """
    response = requests.get(url=url, headers=headers, params=parameters)
    return response.json()


def set_up_query(category: str, account_id: int = 0) -> (str, dict):
    """
    Retrieve the proper url and initial parameters for a given category

    Inputs
        category         category of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        account_id       ID of the sponsored ad account
        start_date       First day of the chosen time range (None for all time)
        end_date         Last day of the time range (None for today)

    Outputs
        url              Url used in the GET query
        params           Initial parameters of the GET query
    """
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
            "q": "search",
            "search.account.values[0]": "urn:li:sponsoredAccount:" + str(account_id)
        },
        "CREATIVES": {
            "q": "search",
            "search.account.values[0]": "urn:li:sponsoredAccount:" + str(account_id)
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

    if category not in initial_param:
        raise ValueError("category value is not valid : should be either ACCOUNT, GROUP, CAMPAIGN, CAMPAIGN_ANALYTICS, CREATIVES or CREATIVES_ANALYTICS")

    url = url[category]
    params = initial_param[category]
    return url, params


def date_filter(param: dict(), start_date: datetime, end_date: datetime) -> dict:
    """
    Update the query paramaters with the chosen timerange

    Inputs
        category         category of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        initial_params   Default parameters for the query. For ex -  q: search
        start_date       First day of the chosen time range (None for all time)
        end_date         Last day of the time range (None for today)

    Outputs
        initial_params   Default paramaters with date components
    """
    if start_date:
        param["dateRange.start.day"] = start_date.day
        param["dateRange.start.month"] = start_date.month
        param["dateRange.start.year"] = start_date.year
    if end_date:
        param["dateRange.end.day"] = end_date.day
        param["dateRange.end.month"] = end_date.month
        param["dateRange.end.year"] = end_date.year
    return param


def get_analytics_parameters(ids: list, category: str) -> dict:
    """
    Given a list of campaign ids or creative ids, returns a dictionary with a parameters" dictionary in a proper format

    Inputs:
        ids              List of campaign ids or creative ids that you want to add in the get query to retrieve their key metrics
        category         category of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    Outputs:
        params           Parameters used to query the AdAnalytics LinkedIn API
    """

    params = {}
    key_prefix = {"GROUP": "", "CAMPAIGN": "search.campaignGroup.values[{}]", "CAMPAIGN_ANALYTICS": "campaigns[{}]",
                  "CREATIVES": "search.campaign.values[{}]", "CREATIVES_ANALYTICS": "creatives[{}]"}
    urn_prefix = {"GROUP": "", "CAMPAIGN": "urn:li:sponsoredCampaignGroup:", "CAMPAIGN_ANALYTICS": "urn:li:sponsoredCampaign:",
                  "CREATIVES": "urn:li:sponsoredCampaign:", "CREATIVES_ANALYTICS": "urn:li:sponsoredCreative:"}
    for i, id_value in enumerate(ids):
        params[key_prefix[category].format(str(i))] = urn_prefix[category] + str(id_value)
    return params
