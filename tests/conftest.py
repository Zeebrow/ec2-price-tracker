import psycopg2
from psycopg2 import sql
import os
import tempfile
import shutil
import json

import pytest
import dotenv

from ec2.instance import PGInstance
import scrpr
# from scrpr import DataCollector, DataCollectorConfig, DatabaseConfig


TEST_CONFIG_FILE = '.env-test'
TEST_TABLE_NAME = 'ec2_instance_pricing_test'


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
    Connects to a Postgres database, creates a new table, and populates it with
    test data.  The table is created and destroyed with each test the fixture is
    used in.
    """
    config = dotenv.dotenv_values(TEST_CONFIG_FILE)
    assert "_test" in config.get("db_dbname")

    dsl = "host={} port={} dbname={} user={} password={}".format(
        config.get("db_host", "localhost"),
        config.get("db_port", 5432),
        config.get("db_dbname", "scrpr_test"),
        config.get("db_user", "scrpr_test"),
        config.get("db_password", None)
    )

    c = psycopg2.connect(dsl)
    curr = c.cursor()
    curr.execute(_data_sql)
    c.commit()

    yield c
    drop_query = sql.SQL(f"DROP TABLE {TEST_TABLE_NAME}")
    curr.execute(drop_query)
    c.commit()
    c.close()


@pytest.fixture
def data_collector_config(pg_dbconfig):
    data_dir = tempfile.mkdtemp()
    _, log_file = tempfile.mkstemp()
    # print(f"=====> {request.function.__name__} {log_file}")
    yield scrpr.DataCollectorConfig(
        human_date='1901-01-01',
        csv_data_dir=data_dir,
        db_config=pg_dbconfig,
        headless=True,
        window_w=1920,
        window_h=1080,
    )
    shutil.rmtree(data_dir)
    os.unlink(log_file)


@pytest.fixture
def ec2_data_collector(request, data_collector_config):
    dc = scrpr.EC2DataCollector(request.function.__name__, config=data_collector_config)
    os.makedirs('screenshots', 0o0755, exist_ok=True)

    yield dc

    fname = f"{request.function.__name__}.png"
    dc.driver.save_screenshot(os.path.join('screenshots', fname))
    dc.driver.close()
    del dc


@pytest.fixture
def pg_dbconfig():
    """valid config"""
    config = scrpr.DatabaseConfig()
    assert config.load('.env-test')
    assert '_test' in config.user
    assert '_test' in config.dbname
    assert config.password is not None

    yield config

@pytest.fixture
def fake_pg_config():
    """returns gibberish"""
    fd, dotenvf = tempfile.mkstemp()
    with open(dotenvf, 'w') as de:
        de.write(os.linesep.join([
            "log_file=~/.local/share/scrpr/csv-data/ec2",
            "console_log=1",
            "thread_count=8",
            "ignore_max_thread_warning=0",
            "# omit to collect data for all",
            "# regions=us-east-1,ap-southeast-4,eu-central-2",
            "# operating_systems=Linux,Windows",
            "",
            "Save_csv=1",
            "Csv_data_dir=~/.local/share/scrpr/csv-data/ec2",
            "Compress_csv=1",
            "",
            "Save_db=0",

            "db_host = some-host",
            "db_port = 1234",
            "db_user = some-user",
            "db_password = password123four",
            "db_dbname = some_db",
        ]))
    
    yield dotenvf

    os.close(fd)
    os.unlink(dotenvf)


@pytest.fixture
def dc_no_driver(request, data_collector_config):
    dc = scrpr.DataCollector(request.function.__name__, config=data_collector_config, _test_driver="nodriver")
    yield dc

@pytest.fixture
def data_dir():
    csv_data_dir = tempfile.mkdtemp()
    yield csv_data_dir
    shutil.rmtree(csv_data_dir)


@pytest.fixture
def one_date_instances():
    return [
        scrpr.Instance("1999-12-31", "test-region-1", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-1", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-1", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-1", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-2", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-2", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-2", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.Instance("1999-12-31", "test-region-2", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
    ]
