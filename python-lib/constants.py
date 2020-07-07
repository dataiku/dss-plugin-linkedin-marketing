from enum import Enum


class authentication_type(Enum):
    TOKEN = "token"
    OAUTH = "oauth"


class Constants(object):
    CAMPAIGN_GROUP_DATASET = "campaign_group_dataset"
    CAMPAIGN_DATASET = "campaign_dataset"
    CAMPAIGN_ANALYTICS_DATASET = "campaign_analytics_dataset"
    CREATIVE_DATASET = "creative_dataset"
    CREATIVE_ANALYTICS_DATASET = "creative_analytics_dataset"
