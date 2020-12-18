import pandas as pd
import logging
from typing import List

from constants import COLUMN_NAMES_DICT


def format_to_df(request_query: dict, category: str, raw_response: bool) -> pd.DataFrame:
    """Format the elements returned from the query

    :param dict request_query:  response from the LinkedIn API
    :param str category: granularity of the data that you want to get -> ACCOUNT, GROUP, CAMPAIGN, CREATIVES, CAMPAIGN_ANALYTICS, CREATIVES_ANALYTICS
    :param bool raw_response: true if the user wants to retrieve raw response in a separate column

    :returns: a formatted dataframe
    :rtype: pd.Dataframe
    """
    api_column_names = build_column_names(category, raw_response)

    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")
    logger.info("Formatting API results...")

    elements = request_query.get("elements", None)
    if elements:
        elements = add_raw_response(raw_response, elements)
        df = pd.DataFrame(request_query["elements"], columns=api_column_names)
    elif "elements" in request_query:
        df = pd.DataFrame(columns=api_column_names)
    else:
        exception_message = str(request_query)
        logger.warning(str(exception_message))
        if raw_response:
            output = {"exception": exception_message, "raw_response": exception_message}
        else:
            output = {"exception": exception_message}
        df = pd.DataFrame(output, columns=api_column_names, index=range(1))
    logger.info("Formatting API results: Done.")
    return df


def build_column_names(category: str, raw_response: bool) -> List[str]:
    """Retrieve the column names

    :returns: column names
    :rtype: list
    """
    api_column_names = COLUMN_NAMES_DICT[category]
    if raw_response:
        api_column_names.append("raw_response")
    return api_column_names


def add_raw_response(raw_response: bool, elements: List[bool]) -> List[bool]:
    """Add a field with the raw response (json)
    :param List[bool] elements:  elements retrieved by the GET query

    :returns: elements with a raw_response field (when relevant)
    :rtype: List[bool]
    """
    if raw_response:
        for i in range(len(elements)):
            elements[i]["raw_response"] = str(elements[i])
    return elements
