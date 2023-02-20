from unittest import mock

import pytest

import scrpr


mock_ec2_datacollector = mock.Mock(spec=scrpr.EC2DataCollector)
mock_thread_divvier = mock.Mock(spec=scrpr.ThreadDivvier)


@mock.patch.object(mock_thread_divvier, 'init_scraper')
def fake_init_scraper():
    pass


@pytest.fixture
def td_config():
    scrpr.DataCollectorConfig(
        '1999-12-31',
        url='test'
    )


@pytest.mark.skip
@mock.patch('scrpr.ThreadDivvier', mock_thread_divvier)
def test_thread_divvier():
    scrpr.ThreadDivvier()
