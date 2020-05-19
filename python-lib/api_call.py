import requests
import pandas as pd
import numpy as np
from math import ceil
import logging

# ==============================================================================
# CLASS AND FUNCTION DEFINITION
# ==============================================================================


def get_query(headers: dict, ids: list() = [], granularity: str = "GROUP", batch_size: int = 1000) -> pd.DataFrame:
    """
    Perfom a Get query and return the data related to the creative or campaign ids given. When the query is too voluminous, lower the batch size to perform a batch query. 

    Inputs:
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        ids              List of campaign groups, campaigns or creative ids - ex : [601956786, 602189436] for campaign groups
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        batch_size       Number of ids by batch query (ex - 100)

    Outputs: 
        df               A Pandas dataframe containing the LinkedIn data related to the input ids
    """

    initial_param = {"GROUP": {"q": "search","search.account.values[0]":"urn:li:sponsoredAccount:507690462"}, "CAMPAIGN": {"q": "search"}, "CREATIVES": {"q": "search"}, "CAMPAIGN_ANALYTICS": {"q": "analytics", "pivot": "CAMPAIGN", "dateRange.start.day": "1",
                                                                                                                                 "dateRange.start.month": "1", "dateRange.start.year": "2006", "timeGranularity": "DAILY"}, "CREATIVES_ANALYTICS": {"q": "analytics", "pivot": "CREATIVE", "dateRange.start.day": "1", "dateRange.start.month": "1", "dateRange.start.year": "2006", "timeGranularity": "DAILY"}}

    try:
        initial_param[granularity]
    except KeyError as e:
        logging.error(e)
        raise ValueError("Granularity value is not valid : should be either GROUP, CAMPAIGN, CAMPAIGN_ANALYTICS, CREATIVES or CREATIVES_ANALYTICS")

    count = len(ids)
    if count >= batch_size:
        df = batch_query(batch_size, count, ids, headers,
                         granularity, initial_param[granularity])
    else:
        params = {**initial_param[granularity], **
                  get_analytics_parameters(ids, granularity)}
        df = query_to_df(headers, params, granularity)
    return df


def batch_query(batch_size: int, count: int, ids: list(), headers: dict, granularity: str, initial_params: dict) -> pd.DataFrame:
    """
    Perfom a batch get query 

    Inputs:
        batch_size       Number of ids by batch query (ex - 100)
        count            Number of ids 
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        initial_params   Fefault parameters for the query. For ex -  q: search

    Outputs: 
        df               A Pandas dataframe containing the LinkedIn data related to the input ids
    """
    df = pd.DataFrame()
    chunks = [*np.array_split(ids, ceil(count/batch_size))]
    for batch in chunks:
        params = {**initial_params, **
                  get_analytics_parameters(batch, granularity)}
        df = df.append(query_to_df(headers, params, granularity), sort=False)
    return df


def query_to_df(headers: dict, parameters: dict, granularity: str) -> pd.DataFrame:
    """
    Retrieve a dataframe with data pulled from the API

    Inputs:
        parameters       Parameters needed for the get query
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    Outputs: 
        df               A Pandas dataframe containing the data from the LinkedIn Api 
    """
    url = {"GROUP": "https://api.linkedin.com/v2/adCampaignGroupsV2", "CAMPAIGN": "https://api.linkedin.com/v2/adCampaignsV2/", "CAMPAIGN_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2",
           "CREATIVES": "https://api.linkedin.com/v2/adCreativesV2/", "CREATIVES_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2"}

    query = requests.get(url=url[granularity],
                         headers=headers, params=parameters)
    try:
        df = pd.DataFrame.from_dict(query.json()["elements"])
        return df
    except KeyError as e:
        logging.error(e)
        raise ValueError("Could not convert to dataframe: "+str(query.json()))


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
        params[key_prefix[granularity].format(
            str(i))] = urn_prefix[granularity] + str(id_value)
    return params
