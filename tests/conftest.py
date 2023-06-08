import psycopg2
from psycopg2 import sql
import os
import tempfile
import shutil
import json
from pathlib import Path

import pytest
import dotenv

from scrpr import scrpr
# from scrpr import DataCollector, DataCollectorConfig, DatabaseConfig


# For testing, use the same postgres host as production, but use a test table.
TEST_CONFIG_FILE = '.env-test'
TEST_TABLE_NAME = 'ec2_instance_pricing_test'

@pytest.fixture
def command_line():
    return scrpr.MainConfig(
        follow=False,
        thread_count=-1,
        overdrive_madness=False,
        compress=False,
        regions=["test"],
        operating_systems=["test"],
        get_operating_systems=False,
        get_regions=False,
        store_csv=False,
        store_db=False,
        v=-1,
        check_size=False,
        log_file="test",
        csv_data_dir="test",
    )

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
    NOTE: not safe for pytest-xdist
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

    conn = psycopg2.connect(dsl)
    curr = conn.cursor()
    curr.execute(_data_sql)
    conn.commit()

    yield conn

    drop_query = sql.SQL(f"DROP TABLE {TEST_TABLE_NAME}")
    curr.execute(drop_query)
    drop_query = sql.SQL(f"DROP TABLE metric_data")
    curr.execute(drop_query)
    conn.commit()
    conn.close()


@pytest.fixture
def ec2_data_collector_config(pg_dbconfig):
    data_dir = tempfile.mkdtemp()
    _, log_file = tempfile.mkstemp()
    # print(f"=====> {request.function.__name__} {log_file}")
    yield scrpr.EC2DataCollectorConfig(
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
def ec2_data_collector(request, ec2_data_collector_config):
    dc = scrpr.EC2DataCollector(request.function.__name__, config=ec2_data_collector_config)
    os.makedirs('screenshots', 0o0755, exist_ok=True)

    yield dc

    fname = f"{request.function.__name__}.png"
    try:
        dc.driver.save_screenshot(os.path.join('screenshots', fname))
    except:
        pass
    dc.driver.close()
    del dc

@pytest.fixture
def metrics_cmd_line():
    """ew"""
    return {
        'follow': False,
        'thread_count': 4,
        'overdrive_madness': False,
        'compress': True,
        'regions': None,
        'operating_systems': None,
        'get_operating_systems': False,
        'get_regions': False,
        'store_csv': True,
        'store_db': True,
        'v': 1,
        'check_size': False,
        'log_file': '/some/log/file/full/path.log',
        'csv_data_dir': '/some/csv/data/dir/full/path'
    }

@pytest.fixture
def pg_dbconfig():
    config = scrpr.DatabaseConfig()
    # config.get("db_host", "localhost"),
    # config.get("db_port", 5432),
    # config.get("db_dbname", "scrpr_test"),
    # config.get("db_user", "scrpr_test"),
    # config.get("db_password", None)
    assert config.load(TEST_CONFIG_FILE)
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
            "save_csv=1",
            "csv_data_dir=~/.local/share/scrpr/csv-data/ec2",
            "compress_csv=1",
            "",
            "save_db=0",

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
def ec2_driverless_dc(request, ec2_data_collector_config):
    dc = scrpr.EC2DataCollector(request.function.__name__, config=ec2_data_collector_config, _test_driver="nodriver")
    yield dc

@pytest.fixture
def data_dir():
    csv_data_dir = tempfile.mkdtemp()
    yield csv_data_dir
    shutil.rmtree(csv_data_dir)


@pytest.fixture
def indexed_instances():
    """
    NOTE filtering options provided on the ec2 pricing page are "region" and "operating system"
    NOTE any particular instance type may not be available in every region
    """
    return [
        ("test-region-1", "test os 1", [
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type3", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        ]),
        ("test-region-1", "test os 2", [
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type3", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        ]),
        ("test-region-2", "test os 1", [
            scrpr.PGInstance("1999-12-31", "test-region-2", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-2", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        ]),
        ("test-region-2", "test os 2", [
            scrpr.PGInstance("1999-12-31", "test-region-2", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
            scrpr.PGInstance("1999-12-31", "test-region-2", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        ])
    ]

@pytest.fixture
def instances():
    """
    NOTE filtering options provided on the ec2 pricing page are "region" and "operating system"
    NOTE any particular instance type may not be available in every region
    """
    return [
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 1", "ts1.type3", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-1", "test os 2", "ts1.type3", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-2", "test os 1", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-2", "test os 1", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-2", "test os 2", "ts1.type1", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
        scrpr.PGInstance("1999-12-31", "test-region-2", "test os 2", "ts1.type2", "$0.010203004", 1, "2 GiB", "test storage type 1", "test throughput 1"),
    ]

@pytest.fixture
def fake_csv_dir_tree():
    dd = Path(tempfile.mkdtemp())

    n1 = dd / "nested1"
    n20 = n1 / "nested2.0"
    n20.mkdir(parents=True, exist_ok=True)
    n21 = dd / "nested1" / "nested2.1"
    n21.mkdir(parents=True, exist_ok=True)

    n0f = dd / "f0.txt"
    n1f = n1 / "f1.txt"
    n20f1 = n20 / "f1.txt"
    n20f2 = n20 / "f2.txt"
    n21f1 = n21 / "f1.txt"

    with n0f.open('w') as f:
        f.write('a' * 10)
    with n1f.open('w') as f:
        f.write('a' * 10)
    with n20f1.open('w') as f:
        f.write('a' * 10)
    with n20f2.open('w') as f:
        f.write('a' * 10)
    with n21f1.open('w') as f:
        f.write('a' * 10)
    
    yield dd
