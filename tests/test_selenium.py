import pytest

from selenium.webdriver.common.by import By
from selenium import webdriver

import scrpr


URL = "https://aws.amazon.com/ec2/pricing/on-demand/"


@pytest.mark.selenium
def test_browser_config(data_collector_config):
    dc = scrpr.DataCollector('test_browser_config', data_collector_config)
    assert dc.driver.get_window_size()['height'] == data_collector_config.window_h
    assert dc.driver.get_window_size()['width'] == data_collector_config.window_w


@pytest.mark.selenium
def test_url_appears_scrapable(chrome_driver: webdriver.Chrome):
    chrome_driver.get(URL)
    iframe = chrome_driver.find_element(By.ID, "iFrameResizer0")
    chrome_driver.switch_to.frame(iframe)


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        (800, 600),    # 4:3
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_get_regions(data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    data_collector_config.window_w = window_w
    data_collector_config.window_h = window_h
    dc = scrpr.DataCollector('test_get_regions', data_collector_config)
    regions = dc.get_available_regions()
    assert regions is not None
    assert 'us-east-1' in regions
    assert list(set(regions)).sort() == regions.sort()


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        (800, 600),    # 4:3
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_get_operating_systems(data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    data_collector_config.window_w = window_w
    data_collector_config.window_h = window_h
    dc = scrpr.DataCollector('test_get_regions', data_collector_config)
    oses = dc.get_available_operating_systems()
    assert oses is not None
    assert 'Linux' in oses
    assert 'Windows' in oses
    assert list(set(oses)).sort() == oses.sort()


@pytest.mark.selenium
def test_get_regions_and_oses(data_collector: scrpr.DataCollector):
    tgt_oses, tgt_regions = data_collector.get_available_regions_and_os()
    assert isinstance(tgt_oses, list)
    assert isinstance(tgt_regions, list)
    assert 'Linux' in tgt_oses
    assert 'Windows' in tgt_oses
    assert 'us-east-1' in tgt_regions
    assert list(set(tgt_oses)).sort() == tgt_oses.sort()
    assert list(set(tgt_regions)).sort() == tgt_regions.sort()


@pytest.mark.selenium
def test_scrape_and_store(data_collector: scrpr.DataCollector):
    assert not data_collector.lock.locked()  # assumed by scrape_and_store function
    data_collector.lock.acquire()
    assert data_collector.scrape_and_store(_os='Linux', region='ap-southeast-4')
    assert not data_collector.lock.locked()  # ready to accept another job


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        (800, 600),    # 4:3 # bad
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_scrape(data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    data_collector_config.window_w = window_w
    data_collector_config.window_h = window_h
    dc = scrpr.DataCollector('test_scrape', data_collector_config)
    try:
        instances = dc.scrape(_os='Linux', region='ap-southeast-4')
    except Exception as e:
        dc.driver.save_screenshot('screenshots/err_test_scrape_and_store.png')
        raise e

    assert instances is not None
    for i in instances:
        assert i.region == 'ap-southeast-4'
        assert i.operating_system == 'Linux'

