import pandas as pd
import logging
from plugin_io_utils import build_column_names


class LinkedInAPIFormatter:
    """
    Geric Formatter class for API responses:
    - initialize with generic parameters
    - compute generic column descriptions
    - apply format_row to dataframe
    """

    def __init__(
        self,
        input_query: list()
    ):
        self.input_query = input_query
        self.api_column_names = build_column_names(input_query)

    def format_to_df(self) -> pd.DataFrame:
        df = pd.DataFrame(columns=self.api_column_names)
        logging.info("Formatting API results...")
        try:
            if self.input_query["elements"]:
                df = df.append(pd.DataFrame(self.input_query["elements"], columns=self.api_column_names))
        except KeyError:
            df = df.append(pd.DataFrame({"API_response": self.input_query}))
        logging.info("Formatting API results: Done.")
        return df
