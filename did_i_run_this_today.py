import psycopg2
from psycopg2 import sql
import dotenv
import datetime
from collections import defaultdict
from datetime import date, timedelta
from math import floor

from scrpr import DatabaseConfig, get_table_size, get_data_dir_size, DEFAULT_CSV_DATA_DIR, get_date_as_datetime

##############################
# Globals
##############################

table_name = 'metric_data'
ec2_pricing_table = 'ec2_instance_pricing'
config = DatabaseConfig()
config.load()

conn = psycopg2.connect(config.get_dsl())

# columns
#  run_no |    date    | threads | oses | regions |       t_init       |       t_run        |  s_csv   |   s_db    | reported_errors | command_line 

##############################
# Functions
##############################

def weight(threads, oses, regions, t_run):
    return round(t_run / (oses * regions * threads), 3)

def sortish():
    query = sql.SQL("SELECT * FROM {} ".format(table_name))

    cur = conn.cursor()
    cur.execute(query)
    r = cur.fetchall()

    weights = []
    for n in r:
        if str(n[1]) == '1999-12-31':
            continue
        weights.append((n[1], n[2],n[3],n[4],n[6], weight(n[2],n[3],n[4],n[6])))
    weights.sort(key=lambda x: x[1])
    print("date\t\tthreads\toses\tregions\tt_run\t\t\tweight")
    for n in weights:
        print("{}\t{}\t{}\t{}\t{}\t{}".format( n[0], n[1], n[2], n[3], n[4], n[5],))
        #print("{}\t{}".format(n[0], n[-1]))

def did_i_run_this_tday():
    query = sql.SQL("SELECT * FROM {} where date = '{}'".format(table_name, datetime.datetime.now().strftime("%Y-%m-%d")))
    cur = conn.cursor()
    cur.execute(query)
    r = cur.fetchall()
    if r == []:
        print('No')
        return 1
    else:
        print('Ye')
        return 0

def all_threads_counts():
    cur = conn.cursor()
    query = sql.SQL("SELECT threads FROM {}".format(table_name))
    cur.execute(query)
    r = cur.fetchall()
    ts = defaultdict(int)
    for n in r:
        ts[n[0]] += 1

    tss = dict(sorted(ts.items()))
    print("thread count\tnumber of runs w/ count")
    total_num_runs = 0
    total_num_threads = 0
    for k, v in tss.items():
        print(f"{k}:\t\t\t{v}")
        total_num_runs += v
        total_num_threads += k * v
    print("-----------------------------------")
    print("avg. threads per run: {:.2f}".format(total_num_threads/total_num_runs))
    print("total number of runs: {}".format(total_num_runs))
    query = sql.SQL("SELECT * FROM {}".format(table_name))
    cur.execute(query)
    r = cur.fetchall()
    print(r[0])


def report_change_on(dt=None, region='us-east-1', operating_system='Linux'):
    delta = timedelta(days=1)
    if dt is None:
        dt = get_date_as_datetime()

    begin_at = dt - delta
    end_at = dt

    cur = conn.cursor()
    query = sql.SQL(f"""
        SELECT * FROM (
            SELECT *, (cph_after - cph_before) as diff FROM (
                select cost_per_hr as cph_before, instance_type
                from {ec2_pricing_table} 
                where date = '{begin_at}' 
                and region = '{region}' 
                and operating_system = '{operating_system}'
            ) a JOIN (
                select cost_per_hr as cph_after, instance_type
                from {ec2_pricing_table}
                where date = '{end_at}' 
                and region = '{region}' 
                and operating_system = '{operating_system}'
            ) b ON a.instance_type = b.instance_type
                
        )c WHERE diff > 0;
    """)
    cur.execute(query)
    r = cur.fetchall()
    if len(r) == 0:
        print("no changes")
    else:
        for row in r:
            print(row)


##############################
# Main
##############################
if __name__ == '__main__':
    # import click
    import argparse

    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--threads-count", "-t", required=False, action='store_true', help="print number of times scrpr has been run with/per thread count")
    grp.add_argument("--did-i-run-this", "-q", required=False, action='store_true', help="did you run python3 scrpr.py today (get date)")
    grp.add_argument("--sortish", "-s", required=False, action='store_true', help="idk")
    grp.add_argument("--sizes", "-z", required=False, action='store_true', help="figure out how much space the compressed CSVs and these awful schemas are currently using")
    grp.add_argument("--report", "-r", required=False, action='store_true', help="show something GOOD, yo")

    args = parser.parse_args()
    print(args)

    if args.threads_count:
        all_threads_counts()
        raise SystemExit()
    if args.did_i_run_this:
        raise SystemExit(did_i_run_this_tday())
    if args.sortish:
        sortish()
        raise SystemExit()
    if args.sizes:
        print("tables:\t\t{:.2f} MB".format(get_table_size(db_config=config) / 1024 / 1024))
        print("CSVs:\t\t{:.2f} MB".format(get_data_dir_size() / 1024 / 1024))
        raise SystemExit()
    if args.report:
        report_change_on()
        raise SystemExit()

    raise SystemExit(did_i_run_this_tday())
