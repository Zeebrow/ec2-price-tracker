import json
from enum import Enum
from datetime import date
from typing import List

from pydantic import BaseModel

# input:
#     {
#   "run_no": 0,
#   "follow": true,
#   "thread_count": 0,
#   "overdrive_madness": true,
#   "compress": true,
#   "regions": "string",
#   "operating_systems": "string",
#   "get_operating_systems": true,
#   "get_regions": true,
#   "store_csv": true,
#   "store_db": true,
#   "v": 0,
#   "check_size": true,
#   "log_file": "string",
#   "csv_data_dir": "string"
# }

class CommandLine(BaseModel):
    """
    Same as scrpr.MainConfig
    """

    log_file: str | None = None
    csv_data_dir: str | None = None
    thread_count: int = 6
    follow: bool = False
    overdrive_madness: bool = False
    compress: bool = True
    regions: List[str] | None = None
    operating_systems: List[str] | None = None
    get_operating_systems: bool = False
    get_regions: bool = False
    store_csv: bool = False
    store_db: bool = True
    v: int = 0
    check_size: bool = False

    class Config:
        orm_mode = True

class SystemStatus(BaseModel):
    status = str

    class Config:
        orm_mode = True