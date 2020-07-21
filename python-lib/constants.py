from enum import Enum


class AuthenticationType(Enum):
    TOKEN = "token"
    OAUTH = "oauth"


class Constants(object):
    CAMPAIGN_GROUP_DATASET = "campaign_group_dataset"
    CAMPAIGN_DATASET = "campaign_dataset"
    CAMPAIGN_ANALYTICS_DATASET = "campaign_analytics_dataset"
    CREATIVE_DATASET = "creative_dataset"
    CREATIVE_ANALYTICS_DATASET = "creatives_analytics_dataset"
    DEFAULT_BATCH_SIZE = 80


class Category(object):
    ACCOUNT = "ACCOUNT"
    GROUP = "GROUP"
    CAMPAIGN = "CAMPAIGN"
    CREATIVE = "CREATIVES"
    CAMPAIGN_ANALYTICS = "CAMPAIGN_ANALYTICS"
    CREATIVE_ANALYTICS = "CREATIVES_ANALYTICS"
