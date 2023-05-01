import argparse
import sys
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
        weights.append((n[1], n[2], n[3], n[4], n[6], weight(n[2], n[3], n[4], n[6])))
    weights.sort(key=lambda x: x[1])
    print("date\t\tthreads\toses\tregions\tt_run\t\t\tweight")
    for n in weights:
        print("{}\t{}\t{}\t{}\t{}\t{}".format(n[0], n[1], n[2], n[3], n[4], n[5],))


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


def display_for_threadcount(t):
    cur = conn.cursor()
    query = sql.SQL("SELECT * FROM {} where threads = {} and date != '1999-12-31'".format(table_name, t))
    cur.execute(query)
    r = cur.fetchall()
    _print_header()
    for row in r:
        _print_row(row)
    return 0


def all_threads_counts():
    """
    -t
    report performance statistics
    """
    cur = conn.cursor()
    query = sql.SQL("SELECT threads, t_run FROM {} where date != '1999-12-31' ORDER BY threads ASC".format(table_name))
    cur.execute(query)
    r = cur.fetchall()
    threads = defaultdict(int)
    # build map of # of threads in run -> num of times run w/ key thread count
    for n in r:
        threads[n[0]] += 1
    # sorts by number of threads

    thread_time_sums = defaultdict(int)
    runs_per_thread = defaultdict(int)
    total_num_runs = len(r)
    total_num_threads_run = 0
    for row in r:
        thread_time_sums[row[0]] += row[1]
        runs_per_thread[row[0]] += 1
        total_num_threads_run += row[0]

    print("threads in run\t# of runs\tavg. run time (sec)")
    for thread_count, num_thread_runs in threads.items():
        print("{}\t\t{}\t\t{:.2f}".format(thread_count, num_thread_runs, thread_time_sums[thread_count] / threads[thread_count]))

    print("-----------------------------------")
    print("total number of runs: {}".format(total_num_runs))
    print("avg. threads per run: {:.2f}".format(total_num_threads_run / total_num_runs))


def _print_header():
    header_line = ['run_no', 'date', 'threads', 'oses', 'regions', 't_init', 't_run', 's_csv', 's_db', 'reported_errors', 'command_line']
    _print_row(header_line)


def _print_row(row):
    print("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
        str(row[0]).center(8),  # run_no
        str(row[1]).center(12),  # date
        str(row[2]).center(9),  # threads
        str(row[3]).center(6),  # oses
        str(row[4]).center(9),  # regions
        str(row[5]).center(20),  # t_init
        str(row[6]).center(20),  # t_run
        str(row[7]).center(10),  # s_csv
        str(row[8]).center(11),  # s_db
        str(row[9]).center(17),  # reported_errors
    ))


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


def do_args(command_line: list):
    prog = command_line[0]
    cli_args = command_line[1:]

    parser = argparse.ArgumentParser(prog=prog)
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--threads-count", "-t", required=False, action='store_true', help="print number of times scrpr has been run with/per thread count")
    grp.add_argument("--did-i-run-this", "-q", required=False, action='store_true', help="did you run python3 scrpr.py today (get date)")
    grp.add_argument("--sortish", "-s", required=False, action='store_true', help="idk")
    grp.add_argument("--sizes", "-z", required=False, action='store_true', help="figure out how much space the compressed CSVs and these awful schemas are currently using")
    grp.add_argument("--report", "-r", required=False, action='store_true', help="show something GOOD, yo")
    grp.add_argument("--by-thread", required=False, action='store', help="show stats for a specific threadcount")

    args, _ = parser.parse_known_args(cli_args)

    if args.threads_count:
        all_threads_counts()
        return 0

    if args.did_i_run_this:
        did_i_run_this_tday()
        return 0

    if args.sortish:
        sortish()
        return 0

    if args.sizes:
        print("tables:\t\t{:.2f} MB".format(get_table_size(db_config=config) / 1024 / 1024))
        print("CSVs:\t\t{:.2f} MB".format(get_data_dir_size() / 1024 / 1024))
        return 0

    if args.report:
        for region in [
            'us-east-1',
            'us-east-2',
            'us-west-1',
            'us-west-2',
            'ap-southeast-1',
            'ap-southeast-2',
            'ap-southeast-3',
            'ap-southeast-4',
            'eu-west-1'
            'eu-west-2'
        ]:
            report_change_on(region=region)
        return 0

    if args.by_thread:
        display_for_threadcount(args.by_thread)

    raise SystemExit(did_i_run_this_tday())


##############################
# Main
##############################
if __name__ == '__main__':
    # import click
    # import click
    raise SystemExit(do_args(sys.argv))
