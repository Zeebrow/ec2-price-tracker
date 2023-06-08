from unittest.mock import MagicMock, patch
from threading import Thread

import pytest
from selenium.webdriver.common.by import By
# from selenium.webdriver.firefox.service import Service
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common import action_chains
from selenium import webdriver

from scrpr import scrpr


mock_ec2_datacollector = MagicMock(spec=scrpr.EC2DataCollector)
mock_thread_divvier = MagicMock(spec=scrpr.ThreadDivvier)
mock_drivers = MagicMock(spec=list)
mock_thread = MagicMock(spec=Thread)

mock_webdriver = MagicMock(spec=webdriver.Chrome)


@pytest.fixture
def td_config():
    scrpr.DataCollectorConfig(
        '1999-12-31',
        url='test'
    )


# @mock.patch('scrpr.ThreadDivvier', mock_thread_divvier)
# @mock.patch('threading.Thread', mock_thread)
# @patch('scrpr.EC2DataCollector', mock_ec2_datacollector)
@pytest.mark.skip
@patch('scrpr.ThreadDivvier', mock_thread_divvier)
def test_patience(data_collector_config: scrpr.DataCollectorConfig):
    t = scrpr.ThreadDivvier(thread_count=2)
    t.init_scrapers_of(config=data_collector_config)
    mock_thread_divvier.init_scraper.assert_called()
    # mock_ec2_datacollector.assert_called_with(data_collector_config)
    # mock_thread_divvier.init_scraper.assert_called()
    # mock_thread_divvier.init_scrapers.assert_not_called()
    # mock_thread_divvier.init_scrapers.assert_called()
