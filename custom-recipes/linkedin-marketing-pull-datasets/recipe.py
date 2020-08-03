# -*- coding: utf-8 -*-
from api_format import format_to_df
from api_call import check_params, query_ads, query_ad_analytics
from constants import AuthenticationType, Constants, Category
from datetime import datetime

import dataiku
from dataiku.customrecipe import (
    get_recipe_config,
    get_output_names_for_role
)

# ==============================================================================
# SETUP
# ==============================================================================
config = get_recipe_config()

authentication_method = AuthenticationType(config.get("authentication_method"))

if authentication_method == AuthenticationType.TOKEN:
    if config.get("linkedin_access_token"):
        HEADERS = {"authorization": "Bearer " + config.get("linkedin_access_token")["access_token"]}
    else:
        raise ValueError("Please specify an access token preset")

elif authentication_method == AuthenticationType.OAUTH:
    if config.get("linkedin_oauth"):
        access_token = config.get("linkedin_oauth")["linkedin_oauth"]
        HEADERS = {"Authorization": "Bearer " + access_token}
    else:
        raise ValueError("Please specify an Oauth preset")

account_id = config.get("account_id")
batch_size = config.get("batch_size")
raw_reponse = config.get("raw_response")

if config.get("date_manager") == "timerange":
    start_date = config.get("start")
    end_date = config.get("end")
    if start_date:
        start_date = datetime.strptime(start_date, "%d/%m/%Y")
    if end_date:
        end_date = datetime.strptime(end_date, "%d/%m/%Y")
elif config.get("date_manager") == "everyday":
    start_date = None
    end_date = None

check_params(HEADERS, account_id, batch_size, start_date, end_date)

# ===============================================================================
# RUN
# ===============================================================================

group = query_ads(HEADERS, Category.GROUP, account_id)
campaign_groups_df = format_to_df(group, Category.GROUP, raw_reponse)

if get_output_names_for_role(Constants.CAMPAIGN_DATASET) or get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET):
    campaign = query_ads(HEADERS, Category.CAMPAIGN, account_id)
    campaigns_df = format_to_df(campaign, Category.CAMPAIGN, raw_reponse)

if get_output_names_for_role(Constants.CREATIVE_DATASET) or get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    creative = query_ads(HEADERS, Category.CREATIVE, account_id)
    creatives_df = format_to_df(creative, Category.CREATIVE, raw_reponse)

if get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET):
    campaign_analytics = query_ad_analytics(HEADERS, Category.CAMPAIGN_ANALYTICS, campaigns_df, batch_size=batch_size, start_date=start_date, end_date=end_date)
    campaign_analytics_df = format_to_df(campaign_analytics, Category.CAMPAIGN_ANALYTICS, raw_reponse)

if get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    creative_analytics = query_ad_analytics(HEADERS, Category.CREATIVE_ANALYTICS, creatives_df, batch_size=batch_size, start_date=start_date, end_date=end_date)
    creative_analytics_df = format_to_df(creative_analytics, Category.CREATIVE_ANALYTICS, raw_reponse)


# ===============================================================================
# WRITE
# ===============================================================================

if get_output_names_for_role(Constants.CAMPAIGN_GROUP_DATASET):
    groups_name = get_output_names_for_role(Constants.CAMPAIGN_GROUP_DATASET)[0]
    groups_dataset = dataiku.Dataset(groups_name)
    groups_dataset.write_with_schema(campaign_groups_df)

if get_output_names_for_role(Constants.CAMPAIGN_DATASET):
    campaigns_names = get_output_names_for_role(Constants.CAMPAIGN_DATASET)[0]
    campaigns_dataset = dataiku.Dataset(campaigns_names)
    campaigns_dataset.write_with_schema(campaigns_df)

if get_output_names_for_role(Constants.CREATIVE_DATASET):
    creatives_names = get_output_names_for_role(Constants.CREATIVE_DATASET)[0]
    creatives_dataset = dataiku.Dataset(creatives_names)
    creatives_dataset.write_with_schema(creatives_df)

if get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET):
    campaigns_analytics_names = get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET)[0]
    campaign_analytics_dataset = dataiku.Dataset(campaigns_analytics_names)
    campaign_analytics_dataset.write_with_schema(campaign_analytics_df)

if get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    creatives_analytics_names = get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET)[0]
    creative_analytics_dataset = dataiku.Dataset(creatives_analytics_names)
    creative_analytics_dataset.write_with_schema(creative_analytics_df)
