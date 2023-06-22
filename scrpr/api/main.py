import json
from datetime import date
from typing import Union
import subprocess
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, Response, status, Request
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import dotenv

from .sql_app import crud, models, schemas
from .sql_app.database import SessionLocal, engine


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.description = "description"
    db = SessionLocal()
    print(app.version)
    print(app.description)
    print(app.docs_url)
    print(app.servers)
    print(app.state.__dict__)
    try:
        crud.set_system_status("idle", db)
    finally:
        yield
    print("attempting to shutdown gracefully...")
    try:
        crud.set_system_status("exited", db)
    finally:
        print(app.version)
        print(app.description)
        print(app.docs_url)
        print(app.servers)
        print(app.state.__dict__)
        db.close()


app = FastAPI(lifespan=lifespan)
print(app.version)
print(app.description)
print(app.docs_url)
print(app.servers)
print(app.state.__dict__)


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


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/status/")
async def get_scrpr_status(db: Session = Depends(get_db)):
    return crud.get_system_status(db)


@app.post("/run/", response_model=schemas.CommandLine)
async def run_scrpr(run_args: schemas.CommandLine, db: Session = Depends(get_db)):
    current_status = crud.get_system_status(db)
    print(run_args.dict())
    if current_status != "idle":
        print(f"already running ({current_status})")
        return f"already running ({current_status})"
    crud.set_system_status("starting", db)
    # run
    de = dotenv.dotenv_values(".env-test")  # note probably from the directory you started uvicorn
    interpreter = de.get('python3_executable')
    script_path = de.get('api_run_script')
    if run_args.regions is not None:
        run_args.regions = ",".join(run_args.regions)
    if run_args.operating_systems is not None:
        run_args.operating_systems = ",".join(run_args.operating_systems)

    api_run_arg_dict = json.dumps(run_args.dict())
    # note killing uvicorn also kills this
    subprocess.Popen(
        [interpreter, script_path, api_run_arg_dict],
    )
    return run_args.json()