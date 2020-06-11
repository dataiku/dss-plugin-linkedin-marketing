# -*- coding: utf-8 -*-
from api_format import LinkedInAPIFormatter
from api_call import get_query ,check_input_values, filter_query

import dataiku
from dataiku.customrecipe import (
    get_recipe_config,
    get_output_names_for_role
)


# ==============================================================================
# SETUP
# ==============================================================================
config = get_recipe_config()

authentication_method = config.get("authentication_method")

if authentication_method == "token":
    if config.get('linkedin_access_token'):
        HEADERS = {"authorization" : "Bearer " + config.get('linkedin_access_token')["access_token"]} 
    else:
        raise ValueError("Please specify an access token")
elif authentication_method == "oauth":
    try:
        access_token = config.get('linkedin-oauth')['linkedin-oauth']
        HEADERS = {'Authorization': 'Bearer ' + access_token}
    except Exception as err:
        logger.error("ERROR [-] Error while reading your LinkedIn access token from Project Variables")
        logger.error(str(err))
        raise Exception("Authentication error")       
account_id = config.get("account_id")
batch_size = config.get("batch_size")
check_input_values(account_id,HEADERS)

## ===============================================================================
## RUN
## ===============================================================================

group = get_query(HEADERS,granularity= "GROUP",account_id = account_id)
api_formatter = LinkedInAPIFormatter(group)
campaign_groups_df = api_formatter.format_to_df()

if get_output_names_for_role("campaign_dataset") or get_output_names_for_role("campaign_analytics_dataset") or get_output_names_for_role("creative_dataset") or get_output_names_for_role("creatives_analytics_dataset"):
    campaign = filter_query(HEADERS, granularity= "CAMPAIGN", mother=campaign_groups_df)
    api_formatter = LinkedInAPIFormatter(campaign)
    campaigns_df = api_formatter.format_to_df()

if get_output_names_for_role("campaign_analytics_dataset"):
    campaign_analytics = filter_query(HEADERS, granularity= "CAMPAIGN_ANALYTICS", mother =campaigns_df)
    api_formatter = LinkedInAPIFormatter(campaign_analytics)
    campaign_analytics_df = api_formatter.format_to_df()

if get_output_names_for_role("creative_dataset") or get_output_names_for_role("creatives_analytics_dataset"):
    creative = filter_query(HEADERS, granularity= "CREATIVES", mother =campaigns_df, batch_size = batch_size)
    api_formatter = LinkedInAPIFormatter(creative)
    creatives_df = api_formatter.format_to_df()

if get_output_names_for_role("creatives_analytics_dataset"):
    creative_analytics = filter_query(HEADERS, granularity= "CREATIVES_ANALYTICS", mother=creatives_df, batch_size = batch_size) 
    api_formatter = LinkedInAPIFormatter(creative_analytics)
    creative_analytics_df = api_formatter.format_to_df()


## ===============================================================================
## WRITE
## ===============================================================================

if get_output_names_for_role("campaign_group_dataset"):
    groups_name = get_output_names_for_role("campaign_group_dataset")[0]
    groups_dataset = dataiku.Dataset(groups_name)
    groups_dataset.write_with_schema(campaign_groups_df)

if get_output_names_for_role("campaign_dataset"):
    campaigns_names =  get_output_names_for_role("campaign_dataset")[0]
    campaigns_dataset = dataiku.Dataset(campaigns_names)
    campaigns_dataset.write_with_schema(campaigns_df)

if get_output_names_for_role("creative_dataset"):
    creatives_names =  get_output_names_for_role("creative_dataset")[0]
    creatives_dataset = dataiku.Dataset(creatives_names)
    creatives_dataset.write_with_schema(creatives_df)

if get_output_names_for_role("campaign_analytics_dataset"):
    campaigns_analytics_names =  get_output_names_for_role("campaign_analytics_dataset")[0]
    campaign_analytics_dataset = dataiku.Dataset(campaigns_analytics_names) 
    campaign_analytics_dataset.write_with_schema(campaign_analytics_df)

if get_output_names_for_role("creatives_analytics_dataset"):
    creatives_analytics_names =  get_output_names_for_role("creatives_analytics_dataset")[0]
    creatives_analytics_dataset = dataiku.Dataset(creatives_analytics_names) 
    creatives_analytics_dataset.write_with_schema(creative_analytics_df)
