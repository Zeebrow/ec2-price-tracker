from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Double, Date
from sqlalchemy.orm import relationship

from .database import Base


class CommandLine(Base):
    __tablename__ = "command_line"

    run_no = Column(Integer, primary_key=True)
    follow = Column(Boolean)
    thread_count = Column(Integer)
    overdrive_madness = Column(Boolean)
    compress = Column(Boolean)
    regions = Column(String)
    operating_systems = Column(String)
    get_operating_systems = Column(Boolean)
    get_regions = Column(Boolean)
    store_csv = Column(Boolean)
    store_db = Column(Boolean)
    v = Column(Integer)
    check_size = Column(Boolean)
    log_file = Column(String)
    csv_data_dir = Column(String)


class Metric(Base):
    __tablename__ = "metric_data"

    run_no = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True)
    threads = Column(Integer)
    oses = Column(Integer)
    regions = Column(Integer)
    t_init = Column(Double)
    t_run = Column(Double)
    s_csv = Column(Integer)
    s_db = Column(Integer)
    reported_errors = Column(Integer)

class SystemStatus(Base):
    __tablename__ = "system_status"
    status = Column(String, primary_key=True)

class RunStatus(Base):
    __tablename__ = "run_status"
    run_id = Column(Integer, primary_key=True, index=True)
    run_no = Column(Integer)
    datetime = Column(Date)
    succeeded = Column(Boolean)
    threads_done = Column(Integer)
    threads_total = Column(Integer)
    status = Column(String)
