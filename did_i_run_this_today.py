import psycopg2
from psycopg2 import sql
import dotenv
import datetime

from scrpr import DatabaseConfig

table_name = 'metric_data'

# columns
#  run_no |    date    | threads | oses | regions |       t_init       |       t_run        |  s_csv   |   s_db    | reported_errors | command_line 
config = DatabaseConfig()
config.load()

conn = psycopg2.connect(config.get_dsl())



query = sql.SQL("SELECT * FROM {} where date = '{}'".format(table_name, datetime.datetime.now().strftime("%Y-%m-%d")))

cur = conn.cursor()
cur.execute(query)
r = cur.fetchall()

if r == []:
    print('No')
    raise SystemExit(1)
else:
    print('Ye')
    print(r)
