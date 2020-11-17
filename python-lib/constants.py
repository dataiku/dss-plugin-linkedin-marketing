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


COLUMN_NAMES_DICT = {
    Category.ACCOUNT:  ["test", "notifiedOnCreativeRejection", "notifiedOnEndOfCampaign", "servingStatuses", "notifiedOnCampaignOptimization",
                        "type", "version", "reference", "notifiedOnCreativeApproval", "changeAuditStamps", "name", "currency", "id", "status", "exception"],
    Category.GROUP: ["runSchedule", "test", "changeAuditStamps", "name", "servingStatuses", "backfilled", "id", "account", "status", "exception"],
    Category.CAMPAIGN: ["test",
                        "format",
                        "targetingCriteria",
                        "servingStatuses",
                        "locale",
                        "type",
                        "version",
                        "objectiveType",
                        "associatedEntity",
                        "runSchedule",
                        "targeting",
                        "optimizationTargetType",
                        "campaignGroup",
                        "changeAuditStamps",
                        "dailyBudget",
                        "unitCost",
                        "creativeSelection",
                        "costType",
                        "name",
                        "offsiteDeliveryEnabled",
                        "id",
                        "audienceExpansionEnabled",
                        "account",
                        "status",
                        "exception"],
    Category.CREATIVE: ["reference",
                        "variables",
                        "test",
                        "changeAuditStamps",
                        "review",
                        "servingStatuses",
                        "campaign",
                        "id",
                        "type",
                        "version",
                        "status",
                        "exception"],
    Category.CAMPAIGN_ANALYTICS: ["pivotValue",
                                  "costInUsd",
                                  "impressions",
                                  "clicks",
                                  "dateRange",
                                  "externalWebsitePostClickConversions",
                                  "externalWebsitePostViewConversions",
                                  "exception"],
    Category.CREATIVE_ANALYTICS: ["pivotValue",
                                  "costInUsd",
                                  "impressions",
                                  "clicks",
                                  "dateRange",
                                  "externalWebsitePostClickConversions",
                                  "externalWebsitePostViewConversions",
                                  "exception"]
}
