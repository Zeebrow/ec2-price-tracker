from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, DeclarativeBase


engine = create_engine("postgresql://fastapi_read:fastapi_read@localhost/scrpr")
SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)

class Base(DeclarativeBase):
    metadata = MetaData()
    type_annotation_map = {}
