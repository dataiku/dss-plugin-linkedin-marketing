import pandas as pd
import logging
from constants import COLUMN_NAMES_DICT


def format_to_df(request_query, category) -> pd.DataFrame:
    """Format the elements returned from the query

    :param dict request_query:  response from the LinkedIn API
    :param str category: granularity of the data that you want to get -> ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS

    :returns: a formatted dataframe
    :rtype: pd.Dataframe
    """
    api_column_names = COLUMN_NAMES_DICT[category]
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")
    df = pd.DataFrame(columns=api_column_names)
    logger.info("Formatting API results...")
    elements = request_query.get("elements", None)
    if elements:
        df = df.append(pd.DataFrame(request_query["elements"], columns=api_column_names))
    elif "elements" in request_query:
        df = pd.DataFrame(columns=api_column_names)
    else:
        exception_message = str(request_query)
        logger.warning(str(exception_message))
        df = df.append(pd.DataFrame({"exception": exception_message}, columns=api_column_names, index=range(1)))
    logger.info("Formatting API results: Done.")
    return df
