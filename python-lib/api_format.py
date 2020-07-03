import pandas as pd
import logging


def format_to_df(request_query) -> pd.DataFrame:
    api_column_names = build_column_names(request_query)
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format="LinkedIn Marketing plugin %(levelname)s - %(message)s")
    df = pd.DataFrame(columns=api_column_names)
    logger.info("Formatting API results...")
    elements = request_query.get("elements", None)
    if elements:
        df = df.append(pd.DataFrame(request_query["elements"], columns=api_column_names))
    else:
        df = df.append(pd.DataFrame({"exception": request_query}))
    logger.info("Formatting API results: Done.")
    return df


def build_column_names(query: dict) -> list():
    content_columns = query.get("elements", None)
    if content_columns:
        column_names = list(content_columns[0].keys())
    else:
        column_names = ["exception"]
    return column_names
