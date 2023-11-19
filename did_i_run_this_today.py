import argparse
import psycopg2
from psycopg2 import sql
import datetime
import json
import sys

from scrpr.scrpr import DatabaseConfig, get_table_size, get_data_dir_size, DEFAULT_CSV_DATA_DIR, get_date, MetricData


##############################
# Globals
##############################

table_name = 'metric_data'
ec2_pricing_table = 'ec2_instance_pricing'


def get_config(env_file=".env"):
    config = DatabaseConfig()
    config.load(env_file=env_file)
    return config


def get_conn(env_file=".env"):
    config = get_config(env_file=env_file)
    conn = psycopg2.connect(config.get_dsl())
    return conn


##############################
# Functions
##############################


def weight(threads, oses, regions, t_run):
    return round(t_run / (oses * regions * threads), 3)


def sortish(env_file=".env"):
    query = sql.SQL("SELECT * FROM {} ".format(table_name))

    conn = get_conn(env_file)
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


def did_i_run_this_tday(env_file=".env"):
    query = sql.SQL("SELECT * FROM {} where date = '{}'".format(table_name, datetime.datetime.now().strftime("%Y-%m-%d")))
    conn = get_conn(env_file)
    cur = conn.cursor()
    cur.execute(query)
    r = cur.fetchall()
    if r == []:
        print('No')
        return 1
    else:
        print('Ye')
        return 0


def display_for_threadcount(t, env_file=".env"):
    conn = get_conn(env_file)
    cur = conn.cursor()
    query = sql.SQL("SELECT * FROM {} where threads = {} and date != '1999-12-31'".format(table_name, t))
    cur.execute(query)
    r = cur.fetchall()
    _print_header()
    for row in r:
        _print_row(row)
    return 0


def _as_json(with_command_line=True, env_file=".env"):
    """need to update db with proper json strings, without single quotes"""
    conn = get_conn(env_file)
    cur = conn.cursor()
    query = sql.SQL("SELECT * FROM {} where date == '2023-05-02' ORDER BY threads ASC".format(table_name))
    cur.execute(query)
    r = cur.fetchall()
    print(r)

    for row in r:
        cli = json.loads(row[10]),
        yield {
            "run_no": row[0],
            "date": row[1],
            "threads": row[2],
            "oses": row[3],
            "regions": row[4],
            "t_init": row[5],
            "t_run": row[6],
            "s_csv": row[7],
            "s_db": row[8],
            "reported_errors": row[9],
            "command_line": cli,
        }


def get_table_sizes(pprint=False, env_file=".env"):
    """
    returns [(table_name, table_size)]
    """
#    tables = [
#        'ec2_thread_times',
#        'ec2_instance_types',
#        'ec2_instance_pricing',
#        'metric_data',
#        'network_throughputs',
#        'storage_types',
#    ]
    if pprint:
        q = "pg_size_pretty(pg_total_relation_size('public.'||a.table_name))"
    else:
        q = "pg_total_relation_size('public.'||a.table_name)"
    conn = get_conn(env_file)
    cur = conn.cursor()
    query = sql.SQL("""
        SELECT table_name, {} FROM (
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name ASC
        )a""".format(q))
    cur.execute(query)
    r = cur.fetchall()
    for row in r:
        yield row


def runtime_averages(env_file=".env"):
    """
    -t
    report performance statistics
    """
    # columns
    # threads | run_no | t_run_avg | t_run_hi | t_run_lo | diff
    query = sql.SQL("""\
            SELECT threads, COUNT(run_no) as num_runs, ROUND(AVG(t_run)) as t_run_avg, ROUND(MAX(t_run)) as t_run_hi, ROUND(MIN(t_run)) as t_run_lo, ROUND((MAX(t_run) - MIN(t_run))) as diff
            FROM metric_data
            WHERE date != '1999-12-31'
            AND oses >= 16
            AND regions >= 20
            GROUP BY threads
            ORDER BY threads ASC
            """.format(table_name))
    conn = get_conn(env_file)
    cur = conn.cursor()
    cur.execute(query)
    r = cur.fetchall()

    header = [
        "thread ct  ",
        "# runs  ",
        "avg run time  ",
        "hi run time  ",
        "lo run time  ",
        "hi - lo  ",
    ]
    table = [row for row in r]
    table_print = []
    table.insert(0, header)
    cols = len(table[0])

    col_max_chars = []
    for c in range(cols):
        col_max_chars.append(max([len(str(f[c])) for f in table]))

    for row in table:
        print_row = []
        for i, f in enumerate(row):
            print_row.append(f'{str(f):{col_max_chars[i]}}')  # lmao
        table_print.append(print_row)

    WH_ON_BLU = '\033[44m'
    WH_ON_L_BLU = '\033[104m'
    WH_ON_GREY = '\033[100m'
    NRML = '\033[0m'

    line_ct = 0
    for f in table_print:
        line = ''.join(f)

        # print header in different color
        if line_ct == 0:
            sys.stdout.write(f"{WH_ON_BLU}{line}{NRML}")
        else:
            if line_ct % 2:
                sys.stdout.write(f"{WH_ON_GREY}{line}{NRML}")
            else:
                sys.stdout.write(line)
        sys.stdout.write("\n")
        line_ct += 1

    sys.stdout.write("-" * len(''.join(table_print[0])))
    sys.stdout.write("\n")
    table.pop(0)
    run_ct = sum([int(row[1]) for row in table ])
    num_threads = sum([int(row[0]) * int(row[1]) for row in table ])
    print("total number of runs: {}".format(run_ct))
    print("avg. threads per run: {:.2f}".format(num_threads / run_ct))


def print_table_sizes():
    _longest = 0
    for row in get_table_sizes():
        if len(row[0]) > _longest:
            _longest = len(row[0])
    _longest = _longest + 4  # padding
    print("{}{}".format("table".ljust(_longest, ' '), "size"))
    print("-" * _longest * 2)
    for row in get_table_sizes(pprint=True):
        print("{}{}".format(row[0].ljust(_longest, ' '), row[1]))


def _print_header():
    header_line = ['run_no', 'date', 'threads', 'oses', 'regions', 't_init', 't_run', 's_csv', 's_db', 'reported_errors', 'command_line']
    _print_row(header_line)


def _print_row(row):
    """without command_line (11th field)"""
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


def report_change_on(dt=None, region='us-east-1', operating_system='Linux', env_file=".env"):
    """
    Compare change in instance pricing between last run and today
    TODO: threadable
    """
    delta = datetime.timedelta(days=1)
    if dt is None:
        dt = get_date()

    begin_at = dt - delta
    end_at = dt

    conn = get_conn(env_file)
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
    parser.add_argument("-f", "--env-file", required=False, action='store', help="path to database credential env file")

    args, _ = parser.parse_known_args(cli_args)

    if args.threads_count:
        runtime_averages(env_file=args.env_file)
        return 0

    if args.did_i_run_this:
        did_i_run_this_tday(env_file=args.env_file)
        return 0

    if args.sortish:
        sortish(env_file=args.env_file)
        return 0

    if args.sizes:
        print()
        print("CSVs:\t\t\t{:.2f} MB".format(get_data_dir_size() / 1024 / 1024))
        print()
        print_table_sizes()
        print('------------------------------------------------')
        print("total:\t\t\t{:.2f} MB".format(get_table_size(db_config=get_config(args.env_file)) / 1024 / 1024))
        return 0

    if args.report:
        for region in [
                'us-east-1',
                'us-east-2',
                'us-west-1',
                'us-west-2',
                'ca-central-1',
                'us-gov-east-1',
                'us-gov-west-1',
                'af-south-1',
                'ap-east-1',
                'ap-south-2',
                'ap-southeast-3',
                'ap-southeast-4',
                'ap-south-1',
                'ap-northeast-3',
                'ap-northeast-2',
                'ap-southeast-1',
                'ap-southeast-2',
                'ap-northeast-1',
                'eu-central-1',
                'eu-west-1',
                'eu-west-2',
                'eu-south-1',
                'eu-west-3',
                'eu-south-2',
                'eu-north-1',
                'eu-central-2',
                'me-south-1',
                'me-central-1',
                'sa-east-1'
        ]:
            report_change_on(region=region, env_file=args.env_file)
        return 0

    if args.by_thread:
        display_for_threadcount(args.by_thread, env_file=args.env_file)

    raise SystemExit(did_i_run_this_tday(env_file=args.env_file))


##############################
# Main
##############################
if __name__ == '__main__':
    raise SystemExit(do_args(sys.argv))
