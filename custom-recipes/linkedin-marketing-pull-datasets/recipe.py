# -*- coding: utf-8 -*-
from api_format import format_to_df
from api_call import get_query, check_input_values, filter_query
from constants import authentication_type, Constants
import logging
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

authentication_method = authentication_type(config.get("authentication_method"))
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")

if authentication_method == authentication_type.TOKEN:
    if config.get("linkedin_access_token"):
        HEADERS = {"authorization": "Bearer " + config.get("linkedin_access_token")["access_token"]}
    else:
        raise ValueError("Please specify an access token preset")

elif authentication_method == authentication_type.OAUTH:
    if config.get("linkedi_oauth"):
        access_token = config.get("linkedi_oauth")["linkedin_oauth"]
        HEADERS = {"Authorization": "Bearer " + access_token}
    else:
        raise ValueError("Please specify an Oauth preset")

account_id = config.get("account_id")
batch_size = config.get("batch_size")

if config.get("date_manager"):
    start_date = config.get("start")
    end_date = config.get("end")
    if start_date:
        start_date = datetime.strptime(start_date, "%d/%m/%Y")
    if end_date:
        end_date = datetime.strptime(end_date, "%d/%m/%Y")

check_input_values(account_id, HEADERS)

# ===============================================================================
# RUN
# ===============================================================================

group = get_query(HEADERS, category="GROUP", account_id=account_id)
campaign_groups_df = format_to_df(group)

if get_output_names_for_role(Constants.CAMPAIGN_DATASET) or get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET) or get_output_names_for_role(Constants.CREATIVE_DATASET) or get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    campaign = filter_query(HEADERS, category="CAMPAIGN", mother=campaign_groups_df)
    campaigns_df = format_to_df(campaign)

if get_output_names_for_role(Constants.CAMPAIGN_ANALYTICS_DATASET):
    campaign_analytics = filter_query(HEADERS, category="CAMPAIGN_ANALYTICS", mother=campaigns_df,
                                      batch_size=batch_size, start_date=start_date, end_date=end_date)
    campaign_analytics_df = format_to_df(campaign_analytics)

if get_output_names_for_role(Constants.CREATIVE_DATASET) or get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    creative = filter_query(HEADERS, category="CREATIVES", mother=campaigns_df, batch_size=batch_size)
    creatives_df = format_to_df(creative)

if get_output_names_for_role(Constants.CREATIVE_ANALYTICS_DATASET):
    creative_analytics = filter_query(HEADERS, category="CREATIVES_ANALYTICS", mother=creatives_df,
                                      batch_size=batch_size, start_date=start_date, end_date=end_date)
    creative_analytics_df = format_to_df(creative_analytics)


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
