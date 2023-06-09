import json
from enum import Enum
from datetime import date

from pydantic import BaseModel


class Instance(BaseModel):
    # pk: str
    date: date
    instance_type: str
    operating_system: str
    region: str
    cost_per_hr: float
    cpu_ct: int
    ram_size_gb: int
    storage_type: str
    network_throughput: str

    class Config:
        orm_mode = True


class CommandLine(BaseModel):
    run_no: int
    follow: bool
    thread_count: int
    overdrive_madness: bool
    compress: bool
    regions: str
    operating_systems: str
    get_operating_systems: bool
    get_regions: bool
    store_csv: bool
    store_db: bool
    v: int
    check_size: bool
    log_file: str
    csv_data_dir: str

    class Config:
        orm_mode = True


class MetricBase(BaseModel):
    run_no: int
    date: date
    threads: int
    oses: int
    regions: int
    t_init: float
    t_run: float
    s_csv: int
    s_db: int
    reported_errors: int

    class Config:
        orm_mode = True
