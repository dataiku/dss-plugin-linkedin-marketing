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

creatives_names =  get_output_names_for_role("creative_dataset")[0]
creatives_dataset = dataiku.Dataset(creatives_names) 

campaigns_analytics_names =  get_output_names_for_role("campaign_analytics_dataset")[0]
campaign_analytics_dataset = dataiku.Dataset(campaigns_analytics_names) 

creatives_analytics_names =  get_output_names_for_role("creatives_analytics_dataset")[0]
creatives_analytics_dataset = dataiku.Dataset(creatives_analytics_names) 

## ===============================================================================
## RUN
## ===============================================================================
account = 507690462
group = get_query(HEADERS,account_id = 50769046)
api_formatter = LinkedInAPIFormatter(group)
campaign_groups_df = api_formatter.format_to_df()

campaign = get_query(HEADERS, granularity= "CAMPAIGN", ids=campaign_groups_df.id.values)
api_formatter = LinkedInAPIFormatter(campaign)
campaigns_df = api_formatter.format_to_df()

campaign_analytics = get_query(HEADERS, granularity= "CAMPAIGN_ANALYTICS", ids=campaigns_df.id.values)
api_formatter = LinkedInAPIFormatter(campaign_analytics)
campaign_analytics_df = api_formatter.format_to_df()

creative = get_query(HEADERS, granularity= "CREATIVES", ids=campaigns_df.id.values, batch_size = 50)
api_formatter = LinkedInAPIFormatter(creative)
creatives_df = api_formatter.format_to_df()

creative_analytics = get_query(HEADERS, granularity= "CREATIVES_ANALYTICS", ids=creatives_df.id.values, batch_size = 50)
api_formatter = LinkedInAPIFormatter(creative_analytics)
creative_analytics_df = api_formatter.format_to_df()

#creative_analytics_df = get_query(HEADERS, creative_ids, "CREATIVES_ANALYTICS", batch_size=100)

## ===============================================================================
## WRITE
## ===============================================================================

groups_dataset.write_with_schema(campaign_groups_df)
campaigns_dataset.write_with_schema(campaigns_df)
campaign_analytics_dataset.write_with_schema(campaign_analytics_df)
creatives_dataset.write_with_schema(creatives_df)
creatives_analytics_dataset.write_with_schema(creative_analytics_df)


