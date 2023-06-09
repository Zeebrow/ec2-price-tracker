from sqlalchemy import create_engine, distinct, text
from sqlalchemy.orm import sessionmaker

from . import models, schemas

engine = create_engine("postgresql://fastapi_read:fastapi_read@localhost/scrpr")
SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)

def get_operating_systems(region: str):
    oses = []
    with engine.connect() as conn:
        r = conn.execute(text(f"SELECT DISTINCT operating_system FROM ec2_instance_pricing WHERE region = '{region}'"))
        for row in r:
            oses.append(row[0])
    return oses


def get_instance_types(region: str, os: str):
    T = models.Instance
    its = []
    with engine.connect() as conn:
        r = conn.execute(text(f"SELECT DISTINCT instance_type FROM ec2_instance_pricing where region = '{region}' AND operating_system = '{os}'"))
        for row in r:
            its.append(row[0])
    return its

def get_regions():
    T = models.Instance
    regions = []
    # with SessionLocal.begin() as s:
        # for r in s.query(T.region).distinct().all():
        #     regions.append(r)
    with engine.connect() as conn:
        r = conn.execute(text("SELECT DISTINCT region FROM ec2_instance_pricing"))
        for row in r:
            regions.append(row[0])
    return regions