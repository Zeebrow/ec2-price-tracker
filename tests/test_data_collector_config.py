import tempfile
import os
import psycopg2
import pytest
# from psycopg2.errors import OperationalError

import scrpr



def test_config_url_is_correct(data_collector_config):
    assert data_collector_config.url == "https://aws.amazon.com/ec2/pricing/on-demand/"


def test_postgres_config_sets_from_dotenv(fake_pg_config):

    db_config = scrpr.DatabaseConfig()
    db_config.load(fake_pg_config)
    assert db_config.host == 'some-host'
    assert db_config.port == 1234
    assert db_config.user == 'some-user'
    assert db_config.dbname == 'some_db'
    assert db_config.password == 'password123four'


def test_postgres_config_dsl():
    db_config = scrpr.DatabaseConfig(
        host='some-host',
        port=1234,
        user='some-user',
        dbname='some_db'
    )
    assert db_config.get_dsl() == "host=some-host port=1234 dbname=some_db user=some-user"
    db_config.password = 'password123four'
    assert db_config.get_dsl() == "host=some-host port=1234 dbname=some_db user=some-user password=password123four"
    assert 'password=password123four' not in str(db_config)  # should be replaced with asterisks


def test_database_connection(pg_dbconfig):
    conn = psycopg2.connect(pg_dbconfig.get_dsl())
    conn.close()
