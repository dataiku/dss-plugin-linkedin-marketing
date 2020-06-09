# -*- coding: utf-8 -*-
import pandas as pd
import logging

from api_format import LinkedInAPIFormatter
from api_call import get_query, check_input_values,filter_query

import dataiku
from dataiku.customrecipe import (
    get_recipe_config,
    get_output_names_for_role
)


# ==============================================================================
# SETUP
# ==============================================================================
config = get_recipe_config()
#api_configuration_preset = get_recipe_config().get("api_configuration_preset")
api_configuration_preset = config.get("authentication_method")
if api_configuration_preset is None or api_configuration_preset == {}:
    raise ValueError("Please specify an API configuration preset")
print("****************")
print(config.get('authentication_method'))
HEADERS = {"authorization" : "Bearer " + config.get('authentication_method').get("access_token")} 
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
