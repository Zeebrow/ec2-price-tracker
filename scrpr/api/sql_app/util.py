from sqlalchemy import create_engine, distinct, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

from . import models, schemas

#engine = create_engine("postgresql://fastapi_read:fastapi_read@localhost/scrpr")
engine = create_engine("postgresql://fast_api:fast_api@localhost/scrpr_test")
SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)


def create_tables():
    """
    CREATE USER fast_api WITH PASSWORD 'fast_api';
    CREATE SCHEMA scrpr_api;
    GRANT CONNECT ON DATABASE scrpr_test TO fast_api;
    GRANT ALL ON SCHEMA scrpr_api TO fast_api;
    """
    models.SystemStatus.metadata.create_all(bind=engine)
    db = SessionLocal()
    status = db.query(models.SystemStatus).first()
    if status is None:
        # creating database for first time
        initial_system_status = models.SystemStatus(status="exited")
        db.add(initial_system_status)
        db.commit()
        db.refresh(initial_system_status)  # performs a SELECT and unmarshals
        print(f"created table '{models.SystemStatus.__tablename__}' with initial value '{initial_system_status.status}'")
    else:
        _statuses = db.query(models.SystemStatus).all()
        if len(_statuses) > 1:
            print("Found more than one row in system_status table")
