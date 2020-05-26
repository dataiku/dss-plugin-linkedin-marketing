import requests
import numpy as np
from math import ceil
import logging
from api_format import LinkedInAPIFormatter


def get_query(headers: dict, granularity: str = "GROUP", account_id : int=0, ids: list() = [], batch_size: int = 1000) -> list():
    """
    Perfom a Get query and return the data related to the creative or campaign ids given. When the query is too voluminous, lower the batch size to perform a batch query. 

    Inputs:
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        ids              List of campaign groups, campaigns or creative ids - ex : [601956786, 602189436] for campaign groups
        batch_size       Number of ids by batch query (ex - 100)

    Outputs: 
        query_output     Output of the API call with the appropriate contents, for ex- dateRange, impressions... 
    """

    initial_param = {"ACCOUNT":{"q": "search"},"GROUP": {"q": "search","search.account.values[0]":"urn:li:sponsoredAccount:"+str(account_id)}, "CAMPAIGN": {"q": "search"}, "CREATIVES": {"q": "search"}, "CAMPAIGN_ANALYTICS": {"q": "analytics", "pivot": "CAMPAIGN", "dateRange.start.day": "1",
                                                                                                                                 "dateRange.start.month": "1", "dateRange.start.year": "2006", "timeGranularity": "DAILY"}, "CREATIVES_ANALYTICS": {"q": "analytics", "pivot": "CREATIVE", "dateRange.start.day": "1", "dateRange.start.month": "1", "dateRange.start.year": "2006", "timeGranularity": "DAILY"}}

    try:
        initial_param[granularity]
    except KeyError as e:
        logging.error(e)
        raise ValueError("Granularity value is not valid : should be either GROUP, CAMPAIGN, CAMPAIGN_ANALYTICS, CREATIVES or CREATIVES_ANALYTICS")

    count = len(ids)
    if count >= batch_size:
        query_output = batch_query(batch_size, ids, headers,
                         granularity, initial_param[granularity])
    else:
        params = {**initial_param[granularity], **
                  get_analytics_parameters(ids, granularity)}
        query_output = query(headers, params, granularity)
    return query_output


def query(headers: dict, parameters: dict, granularity: str) -> list():
    """
    Retrieve a dataframe with data pulled from the API

    Inputs:
        parameters       Parameters needed for the get query
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    Outputs: 
        query_output               A Pandas dataframe containing the data from the LinkedIn Api 
    """
    url = {"ACCOUNT":"https://api.linkedin.com/v2/adAccountsV2","GROUP": "https://api.linkedin.com/v2/adCampaignGroupsV2", "CAMPAIGN": "https://api.linkedin.com/v2/adCampaignsV2/", "CAMPAIGN_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2",
           "CREATIVES": "https://api.linkedin.com/v2/adCreativesV2/", "CREATIVES_ANALYTICS": "https://api.linkedin.com/v2/adAnalyticsV2"}

    query = requests.get(url=url[granularity],
                         headers=headers, params=parameters)
    return query.json()


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


def batch_query(batch_size: int, ids: list(), headers: dict, granularity: str, initial_params: dict) -> list():
    """
    Perfom a batch get query 

    Inputs:
        batch_size       Number of ids by batch query (ex - 100)
        ids              Ids of the queried entities  (campaign ids, group ids...)
        headers          Headers of the GET query, containing the access token for the OAuth2 identification
        granularity      Granularity of the data : GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
        initial_params   Default parameters for the query. For ex -  q: search

    Outputs: 
        query_output     Output of the API call with the appropriate contents, for ex- dateRange, impressions... 
    """
    count = len(ids)
    chunks = [*np.array_split(ids, ceil(count/batch_size))]
    query_output = dict({'paging': {'start': 0, 'count': 0, 'links': [], 'total': 0},"elements":[],"exceptions":[]})
    for batch in chunks:
        params = {**initial_params, **
                  get_analytics_parameters(batch, granularity)}
        try:
            query_output["elements"].extend(query(headers,params,granularity)["elements"])
        except KeyError as e:
            logging.info(e)
            query_output["exceptions"].extend([{"id":id,"API_response":query(headers,params,granularity)} for id in batch])
    return query_output


def check_input_values(account_id:int,headers:dict):
    account = get_query(headers, granularity = "ACCOUNT")
    try:
        account["serviceErrorCode"]
        raise ValueError(account)
    except:
        api_formatter = LinkedInAPIFormatter(account)
        account_df = api_formatter.format_to_df()
        if account_id not in account_df.id.values:
            raise ValueError("Wrong account id or you don't have the permission to access this account")
        


