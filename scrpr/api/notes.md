# run app
`uvicorn main:app --reload`

# create database
```sql
CREATE DATABASE scrpr_v2
    WITH
    OWNER = scrpr_api
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
```
# create schema

```sql
CREATE SCHEMA aws_data
    AUTHORIZATION scrpr_api;
```

# fastapi_read user
```sql
CREATE ROLE fastapi_read WITH PASSWORD 'fastapi_read';
GRANT CONNECT ON DATABASE scrpr TO fastapi_read;
GRANT USAGE ON SCHEMA public TO fastapi_read;
GRANT SELECT ON metric_data TO fastapi_read;
ALTER ROLE fastapi_read WITH LOGIN;
```

# it's `date` not `datetime`
Get it right!

# `curl`s, `curl`s, `curl`s

```
curl localhost:8000/metrics/?from_date=2023-05-01\&to_date=2023-05-05
curl localhost:8000/metrics/latest
```
