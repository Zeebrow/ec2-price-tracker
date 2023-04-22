import psycopg2
from psycopg2 import sql
import dotenv
import datetime
from collections import defaultdict

from scrpr import DatabaseConfig

table_name = 'metric_data'
config = DatabaseConfig()
config.load()

conn = psycopg2.connect(config.get_dsl())

# columns
#  run_no |    date    | threads | oses | regions |       t_init       |       t_run        |  s_csv   |   s_db    | reported_errors | command_line 

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
    query = sql.SQL("SELECT threads FROM {}".format(table_name))
    cur = conn.cursor()
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


if __name__ == '__main__':
    # import click
    import argparse

    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--threads-count", "-t", required=False, action='store_true', help="print number of times scrpr has been run with/per thread count")
    grp.add_argument("--did-i-run-this", "-q", required=False, action='store_true', help="did you run python3 scrpr.py today (get date)")
    grp.add_argument("--sortish", "-s", required=False, action='store_true', help="idk")
    args = parser.parse_args()

    if args.threads_count:
        all_threads_counts()
        raise SystemExit()
    if args.did_i_run_this:
        raise SystemExit(did_i_run_this_tday())
    if args.sortish:
        sortish()
        raise SystemExit()
    raise SystemExit(did_i_run_this_tday())
