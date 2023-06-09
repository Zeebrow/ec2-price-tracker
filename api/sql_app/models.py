from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Double, Date
from sqlalchemy.orm import relationship

from .database import Base

class Instance(Base):
    __tablename__ = "ec2_instance_pricing"

    pk = Column(String, primary_key=True)
    date = Column(Date, index=True)
    instance_type = Column(String)
    operating_system = Column(String)
    region = Column(String)
    storage_type = Column(String)
    network_throughput = Column(String)
    cost_per_hr = Column(Double)
    cpu_ct = Column(Integer)
    ram_size_gb = Column(Integer)


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
#
#class Run(Base):
#    __tablename__ = "runs"
#    run_id = Column(Integer, primary_key=True, index=True)
#    run_no = Column(Integer)
#    date = Column(Date)
#    succeeded = Column(Boolean)
#