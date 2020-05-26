# -*- coding: utf-8 -*-
import pandas as pd

#from api_formatting import get_query
from api_call import get_query
from api_format import LinkedInAPIFormatter

import dataiku
from dataiku.customrecipe import (
    get_recipe_config,
    get_output_names_for_role
)


# ==============================================================================
# SETUP
# ==============================================================================
api_configuration_preset = get_recipe_config().get("api_configuration_preset")
if api_configuration_preset is None or api_configuration_preset == {}:
    raise ValueError("Please specify an API configuration preset")
HEADERS = {"authorization" : "Bearer " + api_configuration_preset.get("access_token")} 

groups_name = get_output_names_for_role("campaign_group_dataset")[0]
groups_dataset = dataiku.Dataset(groups_name)

campaigns_names =  get_output_names_for_role("campaign_dataset")[0]
campaigns_dataset = dataiku.Dataset(campaigns_names)

#creatives_names =  get_output_names_for_role("creative_dataset")[0]
#creatives_dataset = dataiku.Dataset(creatives_names) 
#
#campaigns_analytics_names =  get_output_names_for_role("campaign_analytics_dataset")[0]
#campaign_analytics_dataset = dataiku.Dataset(campaigns_analytics_names) 
#
#creatives_analytics_names =  get_output_names_for_role("creatives_analytics_dataset")[0]
#creatives_analytics_dataset = dataiku.Dataset(creatives_analytics_names) 
#
## ===============================================================================
## RUN
## ===============================================================================

group_query = get_query(HEADERS,account_id = 507690462)
api_formatter = LinkedInAPIFormatter(group_query)
campaign_groups_df = api_formatter.format_to_df()

campaign_query = get_query(HEADERS, granularity= "CAMPAIGN", ids=campaign_groups_df.id.values)
api_formatter = LinkedInAPIFormatter(campaign_query)
campaigns_df = api_formatter.format_to_df()

#groups = list(set(campaigns_df.campaignGroup.values))
#filtered_groups = groups
#filtered_campaign_ids = list(set(campaigns_df[campaigns_df['campaignGroup'].isin(filtered_groups)]["id"].values))
#campaign_analytics_df = get_query(HEADERS, filtered_campaign_ids, "CAMPAIGN_ANALYTICS")
#
#creatives_df = get_query(HEADERS, filtered_campaign_ids,"CREATIVES", batch_size=100)
#
#creative_ids = creatives_df.id.values
#creative_analytics_df = get_query(HEADERS, creative_ids, "CREATIVES_ANALYTICS", batch_size=100)
#campaign_analytics_dataset.write_with_schema(campaign_analytics_df)
#
## ===============================================================================
## WRITE
## ===============================================================================

groups_dataset.write_with_schema(campaign_groups_df)
campaigns_dataset.write_with_schema(campaigns_df)
#campaign_analytics_dataset.write_with_schema(campaign_analytics_df)
#creatives_dataset.write_with_schema(campaign_analytics_df)
#creatives_analytics_dataset.write_with_schema(creative_analytics_df)


