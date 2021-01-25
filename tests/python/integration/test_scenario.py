import pytest
import logging

from dku_plugin_test_utils import dss_scenario


pytestmark = pytest.mark.usefixtures("plugin", "dss_target")


test_kwargs = {
    "user": "user1",
    "project_key": "PLUGINTESTLINKEDINMARKETING",
    "logger": logging.getLogger("dss-plugin-test.linkedin-marketing.test_scenario"),
}


def test_run_linkedin_marketing_update_all_ids(user_clients):
    test_kwargs["client"] = user_clients[test_kwargs["user"]]
    dss_scenario.run(scenario_id="UpdateAllIDs", **test_kwargs)
