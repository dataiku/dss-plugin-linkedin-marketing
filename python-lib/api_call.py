import requests
import numpy as np
import pandas as pd
from math import ceil
import logging
import time
from datetime import datetime
from api_format import format_to_df
from constants import Constants, Category

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")


class LinkedinPluginError(ValueError):
    pass

def check_params(headers: dict, account_ids: list, batchsize: int, start_date: datetime, end_date: datetime):
    """Check if the account id and the access tokens are valid

    :param dict headers:  Headers of the GET query, it contains the access token for the OAuth2 identification
    :param list account_ids:  list of IDs of the sponsored ad accounts
    :param int batch_size: number of ids by batch query (ex - 100)
    :param datetime start_date:  First day of the chosen time range (None for all time)
    :param datetime end_date:  Last day of the time range (None for today)
    :raises: :class:`ValueError`: Invalid parameters
    """
    for account_id in account_ids:
        if not account_id.strip().isnumeric():
            raise ValueError("Wrong format for the account id"+account_id +
                             ". It should be an integer that you can find using the following url : https://www.linkedin.com/campaignmanager/accounts")
    account = query_ads(headers, Category.ACCOUNT, {})
    exception = account.get("exception", None)
    if exception:
        raise ValueError(str(exception))
    else:
        account_df = format_to_df(account, Category.ACCOUNT, False)
        for account_id in account_ids:
            if int(account_id) not in account_df.id.values:
                raise ValueError("Wrong account id or you don't have the permission to access the account "+account_id)

    if batchsize:
        if batchsize < 0 or batchsize > 600:
            raise ValueError("Batchsize should be between 1 and 600")
    else:
        raise ValueError("Batchsize is invalid or missing. It should be between 1 and 600")

    if start_date:
        if start_date.year < 2006 or start_date > datetime.now():
            raise ValueError("Please select a valid start date")
        if end_date:
            if start_date > end_date:
                raise ValueError("Please select a valid time range")
    if end_date:
        if end_date.year < 2006:
            raise ValueError("Please select a valid end date")


def query_ads(headers: dict, category: str, accounts_filter: dict) -> dict:
    """Query the LinkedIn ad API. LinkedIn ad handles pagination

    :param str category: granularity of the data that you want to get -> ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
    :param dict accounts_filter: parameters used in the GET query to filter on ad accounts

    :returns: Response of the API
    :rtype: dict
    """
    url, params = set_up_query(category, accounts_filter)
    response = query_with_pagination(url, headers, params)
    return response


def query_ad_analytics(headers: dict, category: str, parent: pd.DataFrame, batch_size: int = Constants.DEFAULT_BATCH_SIZE, start_date: datetime = None, end_date: datetime = None) -> dict:
    """Query the ad analytics API. As it doesn't handle pagination, a batch query is performed.
    The batch size indicates how many entities (campaigns or creatives) should be returned by batch query.

    :param pd.DataFrame parent: Dataframe which contains the ids used to filter the query.  Ex - parent : campaign -> child: campaign_analytics
    :param int batch_size: number of ids by batch query (ex - 100)

    :returns: Response of the API
    :rtype: dict
    """
    if parent.empty:
        response = {"exception": "The parent dataframe is empty, so this dataset cannot be retrieved"}
    else:
        invalid_parent = (parent["exception"].values.size == 0)
        if invalid_parent:
            response = {"exception": "The parent dataframe is invalid, so this dataset cannot be retrieved"}
        else:
            url, initial_params = set_up_query(category)
            initial_params = date_filter(initial_params, start_date, end_date)
            ids = parent["id"].values
            count = len(ids)
            if count >= batch_size:
                response = query_by_batch(batch_size, ids, category, url, headers, initial_params)
            else:
                params = {**initial_params, **get_analytics_parameters(ids, category)}
                response = query(url, headers, params)
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
    paging = response.get("paging", None)
    if paging:
        total_entities = paging.get("total", None)
        if total_entities and total_entities > page_size:
            for start in range(page_size, total_entities, page_size):
                parameters.update({"start": str(start)})
                response["elements"].extend(query(url, headers, parameters)["elements"])
    else:
        response["exception"] = response
    return response


def query_by_batch(batch_size: int, ids: list, category: str, url: str, headers: dict, initial_params: dict) -> dict:
    """Perfom a batch get query with multiple filters
    A request should not return more than 1,000 entities (campaigns, creatives...).
    When the API raises a error 400 : Request would return too many entities, consider decreasing the batch size.

    :param list ids: list of entities used to filter the query
    :param dict initial_params: default params for the query. Only the filter fields are missing.

    :returns : API's response
    :rtype: dict
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
            logger.warning("Empty output for params: " + str(params))
        else:
            response = {**query_output, **{"Hint": "consider decreasing the sample size"}}
            break
    return response


def query(url: str, headers: dict, parameters: dict) -> dict:
    """Performs the get query. Response is returned in a json format

    :returns: API's response
    :rtype: dict
    """
    successful_get = False
    attempt_number = 0
    while not successful_get and attempt_number <= Constants.MAX_RETRIES:
        try:
            attempt_number += 1
            response = requests.get(url=url, headers=headers, params=parameters)
            successful_get = True
        except Exception as err:
            logger.warning("ERROR:{}".format(err))
            logger.warning("URl={} on attempt #{}".format(url, attempt_number))
            if attempt_number == Constants.MAX_RETRIES:
                raise LinkedinPluginError("Error while accessing {} on attempt #{}: {}".format(url, attempt_number, err))
            time.sleep(Constants.WAIT_TIME_BEFORE_RETRY_SEC)
    if response.status_code < 400:
        return response.json()
    elif response.status_code == 400:
        return {"error": "Error 400. Consider decreasing the number of account ids or the batch size."}
    else:
        return {"error": "Error{}".format(response.status_code)}


def set_up_query(category: str, accounts_filter: dict = {}) -> (str, dict):
    """Retrieve the proper url and initial parameters for a given category

    :returns: URL and initial parameters for the GET query
    :retype: tuple
    """
    url = {
        "ACCOUNT": "https://api.linkedin.com/v2/adAccountsV2",
        "GROUP": "https://api.linkedin.com/v2/adCampaignGroupsV2",
        "CAMPAIGN": "https://api.linkedin.com/v2/adCampaignsV2/",
        "CAMPAIGN_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2",
        "CREATIVES": "https://api.linkedin.com/v2/adCreativesV2/",
        "CREATIVES_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2"
    }
    if category == Category.ACCOUNT:
        params = {"q": "search"}
    elif category == Category.GROUP or category == Category.CAMPAIGN or category == Category.CREATIVE:
        params = accounts_filter
    elif category == Category.CAMPAIGN_ANALYTICS:
        params = {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "dateRange.start.day": "1",
            "dateRange.start.month": "1",
            "dateRange.start.year": "2006",
            "timeGranularity": "DAILY",
            "projection": "(*,elements*(dateRange(*),costInUsd,impressions,clicks,externalWebsitePostClickConversions,externalWebsitePostViewConversions,pivotValue~(localizedName)))",
            "fields": "dateRange,costInUsd,impressions,clicks,externalWebsitePostClickConversions,externalWebsitePostViewConversions,pivotValue"
        }
    elif category == Category.CREATIVE_ANALYTICS:
        params = {"q": "analytics",
                  "pivot": "CREATIVE",
                  "dateRange.start.day": "1",
                  "dateRange.start.month": "1",
                  "dateRange.start.year": "2006",
                  "timeGranularity": "DAILY",
                  "projection": "(*,elements*(dateRange(*),costInUsd,impressions,clicks,externalWebsitePostClickConversions,externalWebsitePostViewConversions,pivotValue~(localizedName)))",
                  "fields": "dateRange,costInUsd,impressions,clicks,externalWebsitePostClickConversions,externalWebsitePostViewConversions,pivotValue"
                  }
    else:
        raise ValueError("category value is not valid : should be either ACCOUNT, GROUP, CAMPAIGN, CAMPAIGN_ANALYTICS, CREATIVES or CREATIVES_ANALYTICS")
    url = url[category]
    return url, params


def set_accounts_filter(account_ids: list) -> dict:
    """Given a list of account ids, returns parameters with corresponding filters.

    :returns: accounts_filter: Parameters used to filter the query for given account ids
    :rtype: dict
    """
    accounts_filter = {"q": "search"}
    for i, account_id in enumerate(account_ids):
        accounts_filter["search.account.values[{}]".format(i)] = "urn:li:sponsoredAccount:{}".format(int(account_id))
    return accounts_filter


def date_filter(param: dict(), start_date: datetime, end_date: datetime) -> dict:
    """Update the query parameters with the chosen timerange

    :returns: Default parameters for the GET query with date components
    :rtype: dict
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
    """Given a list of campaign ids or creative ids, returns parameters with corresponding filters.

    :returns: params: Parameters used to filter the query to the AdAnalytics LinkedIn API
    :rtype: dict
    """

    params = {}
    key_prefix = {"GROUP": "", "CAMPAIGN": "search.campaignGroup.values[{}]", "CAMPAIGN_ANALYTICS": "campaigns[{}]",
                  "CREATIVES": "search.campaign.values[{}]", "CREATIVES_ANALYTICS": "creatives[{}]"}
    urn_prefix = {"GROUP": "", "CAMPAIGN": "urn:li:sponsoredCampaignGroup:", "CAMPAIGN_ANALYTICS": "urn:li:sponsoredCampaign:",
                  "CREATIVES": "urn:li:sponsoredCampaign:", "CREATIVES_ANALYTICS": "urn:li:sponsoredCreative:"}
    for i, id_value in enumerate(ids):
        params[key_prefix[category].format(str(i))] = urn_prefix[category] + str(id_value)
    return params
