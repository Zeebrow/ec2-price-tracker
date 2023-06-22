import json
from datetime import date
from typing import Union
import subprocess

from fastapi import FastAPI, Depends, HTTPException, Response, status, Request
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import dotenv

from .sql_app import crud, models, schemas
from .sql_app.database import SessionLocal, engine


app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def read_root():
    return {"Hello": "World"}


# @app.get("/metrics/{run_no}", response_model=schemas.MetricBase, status_code=200)
# @app.get("/metrics/{run_no}", status_code=200)
@app.get("/metrics/", status_code=200, response_model=Union[schemas.MetricBase, list[schemas.MetricBase]])
async def read_metrics(
        response: Response,
        latest: bool | None = None,
        run_no: int | None = None,
        from_date: date | None = None,
        to_date: str | None = None,
        db: Session = Depends(get_db)
    ):
    if from_date:
        if to_date:
            return crud.get_metric_range(db=db, to_date=to_date, from_date=from_date)
        return crud.get_metric_on(db=db, date=from_date)
    if run_no:
        return crud.get_metric(db, run_no=run_no)
    if latest:
        return crud.get_latest_metric(db)
    return crud.get_latest_metric(db)


@app.get("/metrics/{from_date}", status_code=200, response_model=Union[schemas.MetricBase, list[schemas.MetricBase]])
async def read_metrics(
        response: Response,
        from_date: date,
        to_date: str | None = None,
        db: Session = Depends(get_db)
    ):
    return crud.get_metric_range(db=db, to_date=to_date, from_date=from_date)
    # if from_date:
    #     if to_date:
    #     return crud.get_metric_on(db=db, date=from_date)
    # if run_no:
    #     return crud.get_metric(db, run_no=run_no)
    # if latest:
    #     return crud.get_latest_metric(db)
    # return crud.get_latest_metric(db)
    


@app.get("/reports/")
async def report_between(
        from_date: date,
        to_date: str,
        db: Session = Depends(get_db)
    ):

    csvf = crud.get_csv(db=db, to_date=to_date, from_date=from_date)
    return {"file": csvf}
    # fr = FileResponse()

@app.get("/reports/size", status_code=status.HTTP_200_OK)
async def report_table_sizes(
        request: Request,
        response: Response,
        tables: str = "ec2_instance_pricing",
        db: Session = Depends(get_db)
    ):
    sizes = crud.get_current_size(db, tables.split(','))
    if "error" in sizes.keys():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return sizes
    
    print(request.headers.get('host'))

    print(f"{response.headers=}")
    return {f"size_bytes": sizes}


@app.get("/prices/", status_code=200, response_model=Union[schemas.Instance, list[schemas.Instance]])
async def read_metrics(
        response: Response,
        instance_type: str,
        region: str,
        os: str,
        from_date: date = date.today(),
        to_date: date = date.today(),
        # to_date: str | None = None,
        db: Session = Depends(get_db)
    ):
    return crud.get_instance_pricing(instance_type, region, os, from_date, to_date, db)

@app.post("/run/", response_model=schemas.CommandLine)
async def run_scrpr(run_args: schemas.CommandLine):
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
    # put lock in db
    # run
    de = dotenv.dotenv_values(".env-test")  # note probably from the directory you started uvicorn
    interpreter = de.get('python3_executable')
    script_path = de.get('api_run_script')
    # print(f"{interpreter=}")
    # print(f"{script_path=}")
    # print("api: running with args:")
    # print(run_args.json())
    result = subprocess.run(
        [interpreter, script_path, run_args.json()],
        capture_output=True,
        encoding='utf_8'
    )
    print(f"{result=}")
    print("stderr:")
    for line in result.stdout.split("\n"):
        print(line)
    print("stderr:")
    for line in result.stderr.split("\n"):
        print(line)
    # remove lock

    return run_args