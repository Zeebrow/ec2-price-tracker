from scrpr import PostgresConfig
import psycopg2


def test_config_url_is_correct(data_collector_config):
    assert data_collector_config.url == "https://aws.amazon.com/ec2/pricing/on-demand/"


def test_postgres_config_dsl(pg_dbconfig: PostgresConfig):
    assert pg_dbconfig.get_dsl() == "host=localhost port=5432 dbname=scrpr_test user=scrpr_test"
    pg_dbconfig.password = 'test_password'
    assert pg_dbconfig.get_dsl() == "host=localhost port=5432 dbname=scrpr_test user=scrpr_test password=test_password"
    assert 'test_password' not in str(pg_dbconfig)  # should be replaced with asterisks


def test_database_connection(data_collector_config):
    conn = psycopg2.connect(data_collector_config.db_config.get_dsl())
    conn.close()
