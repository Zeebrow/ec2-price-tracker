from os import get_terminal_size
from datetime import date
import csv
import tempfile
import time

from sqlalchemy.orm import Session
from sqlalchemy import func, text, desc
from sqlalchemy.exc import ProgrammingError

from . import models, schemas, util


def get_latest_metric(db: Session):
    run_no = db.query(func.max(models.Metric.run_no)).scalar()
    return db.query(models.Metric).filter(models.Metric.run_no == run_no).first()

def get_metric_on(db: Session, date: date):
    return db.query(models.Metric).filter(models.Metric.date == date).first()

def get_metric_range(db: Session, from_date: date, to_date: date | None = None):
    if not to_date:
        to_date = from_date
    return db.query(models.Metric).filter(models.Metric.date.between(from_date, to_date)).all()
    # if to_date:
    #     return db.query(models.Metric).filter(models.Metric.date.between(from_date, to_date)).all()
    # return db.query(models.Metric).filter(models.Metric.date == date).first()

def get_metric(db: Session, run_no: int):
    return db.query(models.Metric).filter(models.Metric.run_no == run_no).first()

def get_command_line(db: Session, run_no: int):
    """assume run_no is valid"""
    return db.query(models.CommandLine).filter(models.CommandLine.run_no == run_no).first()

def get_csv(db: Session, from_date: date, to_date: date):
    query = db.query(models.Metric).filter(models.Metric.date.between(from_date, to_date)).all()
    print(f"{query[0].command_line=}")
    print(f"{query[0].__table__.columns.keys()=}")
    print(f"{query[0].__table__.columns.get('run_no')=}")
    print("------------------------------------------")

    _, f = tempfile.mkstemp(".csv")
    print(f)

    with open(f, "w") as csvf:
        csv_writer = csv.DictWriter(csvf, fieldnames=query[0].__table__.columns.keys())
        csv_writer.writeheader()
        for q in query:
            d = {}
            for k in q.__table__.columns.keys():
                d[k] = q.__getattribute__(k)
            csv_writer.writerow(d)
    return f

def get_current_size(db: Session, tables: list[str]):
    query_string = ["SELECT"]

    for n, t in enumerate(tables):
        if n == len(tables) - 1:
            query_string.append(f"pg_total_relation_size('public.{t}') as {t}")
        else:
            query_string.append(f"pg_total_relation_size('public.{t}') as {t},")

    try:
        with db.connection() as conn:
            r = conn.execute(text(' '.join(query_string))).fetchone()
        return { k: v for k, v in zip(tables, r) }
    except ProgrammingError as e:
        e.add_detail("some detail")
        e.add_detail(f"tables requested: {tables}")
        return {"error": e}
    except Exception as e:
        return {"error": e}


def get_instance_pricing(
        instance_type: str,
        region: str,
        os: str,
        from_date: date,
        to_date: str | None,
        db: Session
    ):
    T = models.Instance
    return  db.query(T).\
        filter(T.date.between(from_date, to_date)).\
        filter(T.instance_type == instance_type).\
        filter(T.operating_system == os).\
        filter(T.region == region).\
        all()


def get_instance_pricing_diffs_ooo_pretty_fancy(
        instance_types: list[str] | None,
        oses: list[str] | None,
        regions: list[str] | None,
        db: Session
    ):
    T = models.Instance
    diffs: list[T] = []

    if not instance_types:
        instance_types = util.get_instance_types()

    if not oses:
        oses = util.get_operating_systems()

    if not regions:
        regions = util.get_regions()
            
    for _ic, inst in enumerate(instance_types):
        print(f"type: {inst}                 \t\t({_ic}/{len(instance_types)})                       ")
        for _oc, o in enumerate(oses):
            print(f"os: {o}                 \t\t({_oc}/{len(oses)})                                  ")
            for _rc, region in enumerate(regions):
                print(f"region: {region}             \t\t({_rc}/{len(regions)})                     ", end='\r')

                query = db.query(T).\
                    filter(T.instance_type == inst).\
                    filter(T.operating_system == o).\
                    filter(T.region == region).\
                    order_by(desc(T.date)).\
                    all()

                last: T  = None
                for n, i in enumerate(query):
                    if n == 0:
                        last = i
                        continue
                    if i.cost_per_hr != last.cost_per_hr:
                        diffs.append((i, last))
                    last = i
            print(f"done{' '*(get_terminal_size()[0] - 5)}")
            print("\x1b[1A", end='')
            print("\x1b[1A", end='')
        print("\x1b[1A", end='')
    print()
    print()
    print()
    return diffs

def get_instance_pricing_diffs(
        db: Session
    ) -> list[models.Instance]:
    """
    This shit never returns anything. I'm almost convinced amazon never changes their prices. FeelsBadMan
    """
    T = models.Instance
    diffs: list[T] = []

    # if not instance_types:
    #     instance_types = util.get_instance_types()

    # if not oses:
    #     oses = util.get_operating_systems()

    regions = util.get_regions()
            
    # print(len(regions) * len(oses) * len(instance_types))
    for region in regions:
        _oses = util.get_operating_systems(region)
        for os in _oses:
            _instance_types = util.get_instance_types(region, os)
            print(len(regions) * len(_oses) * len(_instance_types), end='\r')
            for inst in _instance_types:
                query = db.query(T).\
                    filter(T.instance_type == inst).\
                    filter(T.operating_system == os).\
                    filter(T.region == region).\
                    order_by(desc(T.date)).\
                    all()

                last: T  = None
                for n, i in enumerate(query):
                    if n == 0:
                        last = i
                        continue
                    if i.cost_per_hr != last.cost_per_hr:
                        print(f"Found: {i.date} != {last.date} \n\t{i.cost_per_hr=}\n\t{last.cost_per_hr=}")
                        diffs.append((i, last))
                    last = i
    return diffs
