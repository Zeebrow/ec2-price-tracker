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

def set_system_status(new_status: str, db: Session):
    system_status = db.query(models.SystemStatus).first()
    system_status.status = new_status
    db.commit()
    db.refresh(system_status)  # performs a SELECT and unmarshals
    return system_status.status

def get_system_status(db: Session):
    return db.query(models.SystemStatus).first().status