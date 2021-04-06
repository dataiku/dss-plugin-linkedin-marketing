import pytest
import logging

from dku_plugin_test_utils import dss_scenario


pytestmark = pytest.mark.usefixtures("plugin", "dss_target")


test_kwargs = {
    "user": "data_scientist_1",
    "project_key": "PLUGINTESTLINKEDINMARKETING"
}


def test_run_linkedin_marketing_update_all_ids(user_dss_clients):
    dss_scenario.run(user_dss_clients, scenario_id="UpdateAllIDs", **test_kwargs)
