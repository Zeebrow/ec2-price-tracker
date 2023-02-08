import psycopg2
import os
import tempfile
import shutil

from unittest import mock
import pytest

from selenium.webdriver.common.by import By
# from selenium.webdriver.firefox.service import Service
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common import actions, action_chains
from selenium import webdriver

from ec2.instance import PGInstance
from scrpr import DataCollector, DataCollectorConfig, PostgresConfig


def pytest_addoption(parser):
    parser.addoption(
        "--run-selenium", action="store_true", default=False, help="run tests which require selenium"
    )

def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-selenium"):
        return
    skip_selenium = pytest.mark.skip(reason="need --run-selenium option to run")
    for item in items:
        if "selenium" in item.keywords:
            item.add_marker(skip_selenium)


with open(os.path.join(os.path.dirname(__file__), 'ec2_data.sql'), 'rb') as f:
    _data_sql = f.read().decode('utf8')

@pytest.fixture
def db():
    """
    Creates a new table and populates it with test data.
    The table is created and destroyed with each test the fixture is used in.
    """
    c = psycopg2.connect("host=localhost dbname=scrpr_test user=scrpr_test")
    curr = c.cursor()
    curr.execute(_data_sql)
    c.commit()

    yield c

    curr.execute("DROP TABLE ec2_instance_pricing")
    c.commit()
    c.close()


@pytest.fixture
def pg_database_config():
    yield PostgresConfig(
        host='localhost',
        port=5432,
        dbname='scrpr_test',
        user='scrpr_test',
    )


@pytest.fixture
def data_collector_config(request, pg_database_config):
    data_dir = tempfile.mkdtemp()
    _, log_file = tempfile.mkstemp()
    # print(f"=====> {request.function.__name__} {log_file}")
    yield DataCollectorConfig(
        human_date='1901-01-01',
        csv_data_dir=data_dir,
        db_config=pg_database_config,
        log_file=log_file,
        headless=True,
        window_w=1920,
        window_h=1080,
    )
    shutil.rmtree(data_dir)
    os.unlink(log_file)

@pytest.fixture
def chrome_driver():
    options = webdriver.ChromeOptions()
    options.binary_location = '/usr/bin/google-chrome'
    options.add_argument('-headless')
    options.add_argument("window-size=1920,1080")
    driverService = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=driverService, options=options)
    driver.implicitly_wait(10)

    yield driver

    driver.quit()



@pytest.fixture
def data_collector(request, data_collector_config):
    dc = DataCollector('test', config=data_collector_config)
    os.makedirs('screenshots', 0o0755, exist_ok=True)

    yield dc

    fname = f"{request.function.__name__}.png"
    dc.driver.save_screenshot(os.path.join('screenshots', fname))
    dc.driver.close()
    del dc

@pytest.fixture
def pg_dbconfig():
    yield PostgresConfig(
        host='localhost',
        port=5432,
        dbname='scrpr_test',
        user='scrpr_test'
    )
