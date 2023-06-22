from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, DeclarativeBase


#engine = create_engine("postgresql://fastapi_read:fastapi_read@localhost/scrpr")
engine = create_engine("postgresql://fast_api:fast_api@localhost/scrpr_test")
SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)

class Base(DeclarativeBase):
    metadata = MetaData()
    type_annotation_map = {}
