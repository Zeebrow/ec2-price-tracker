import psycopg2
from psycopg2 import sql
from psycopg2.errors import UniqueViolation
from collections import OrderedDict
from typing import Tuple
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Instance(object):
    __slots__ = ("datestamp", "region", "operating_system", "instance_type", "cost_per_hr", "cpu_ct", "ram_size", "storage_type", "network_throughput")

    @classmethod
    def get_fields(self):
        return [
            "date",
            "instance_type",
            "operating_system",
            "region",
            "cost_per_hr",
            "cpu_ct",
            "ram_size_gb",
            "storage_type",
            "network_throughput"
        ]

    def __init__(self, datestamp: str, region: str, operating_system: str, instance_type: str, cost_per_hr: str, cpu_ct: str, ram_size: str, storage_type: str, network_throughput: str) -> None:
        self.region = region
        self.operating_system = operating_system
        self.instance_type = instance_type
        self.cost_per_hr = cost_per_hr
        self.cpu_ct = cpu_ct
        self.ram_size = ram_size
        self.storage_type = storage_type
        self.network_throughput = network_throughput
        self.datestamp = datestamp  # YYYY-MM-DD

    def __repr__(self) -> str:
        return f"{self.datestamp} {self.instance_type} {self.operating_system} {self.region}"

    def as_dict(self):
        return OrderedDict({
            "date": self.datestamp,
            "instance_type": self.instance_type,
            "operating_system": self.operating_system,
            "region": self.region,
            "cost_per_hr": self.cost_per_hr,
            "cpu_ct": self.cpu_ct,
            "ram_size_gb": self.ram_size,
            "storage_type": self.storage_type,
            "network_throughput": self.network_throughput,
        })
    

class PGInstance(Instance):
    """
    Allow instance data to be stored efficiently.
    """

    def __init__(self, datestamp: str, region: str, operating_system: str, instance_type: str, cost_per_hr: str, cpu_ct: str, ram_size: str, storage_type: str, network_throughput: str) -> None:
        super().__init__(datestamp, region, operating_system, instance_type, cost_per_hr, cpu_ct, ram_size, storage_type, network_throughput)

    def get_region_ids(self, conn):
        curr = conn.cursor()
        curr.execute("SELECT * FROM regions_lookup")
        regions = curr.fetchall()
        PGInstance.regions_lookup = {r: i for i, r in regions}
        curr.close()

    def store(self, conn, table='ec2_instance_pricing'):
        """
        Throws UniqueViolation 
        """
        curr = conn.cursor()
        ins = sql.SQL("""
            INSERT INTO {table} (pk, date, instance_type, operating_system, region, cost_per_hr, cpu_ct, ram_size_gb, storage_type, network_throughput)
            VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)
            """.format(table=table))
        curr.execute(ins,
            ('-'.join([self.datestamp, self.region, self.operating_system, self.instance_type]), *self.prep_data())
        )
        conn.commit()
        curr.close()

    def prep_data(self) -> Tuple[str, str, str, str, float, int, float, str, str]:
        ready_data = self.as_dict()
        ready_data['cost_per_hr'] = float(self.cost_per_hr.replace('$', ''))
        ready_data['cpu_ct'] = int(self.cpu_ct)
        ready_data['ram_size_gb'] = float(self.ram_size.split(' ')[0])
        return (
            ready_data["date"],
            ready_data["instance_type"],
            ready_data["operating_system"],
            ready_data["region"],
            ready_data["cost_per_hr"],
            ready_data["cpu_ct"],
            ready_data["ram_size_gb"],
            ready_data["storage_type"],
            ready_data["network_throughput"],
        )
