# BROKEN! NOTICE!

My dear followers: a few days ago I found invalid (amounts of) data being committed to Postgres.
I seem to have a fix however, as part of a too-much-needed refactor, I require a few days time
to validate stability in other important areas, namely consistency during non-ideal network
conditions. 

To elaborate, as it stands `scrpr` is not ready to me the automated service it was before the
[AWS EC2 Pricing Page](https://aws.amazon.com/ec2/pricing/on-demand/) data-analytics refactor.
I am questioning testing as a valid approach to Selenium script development (I am sure someone with
experience would like the laugh!), and so have determined my efforts to use this project to learn more about
`pytest` in general are best used elsewhere. `scrpr`'s purpose is to collect and store valid
data, so I
feel no shame in making [learning SQLAlchemy](https://docs.sqlalchemy.org/en/20/tutorial/) my
new priority, to acknowledge what FastAPI has abstracted away from me. I will also look forward
to investigating [Alembic](https://alembic.sqlalchemy.org/en/latest/) to enable seemless
migration experience akin to what Django has to offer.


- ducee


# scrape cost data for ec2

Use a headless Chrome browser to gather EC2 price info: <https://aws.amazon.com/ec2/pricing/on-demand/>

Postres was chosen because `psycopg2` is fully thread-safe.

# usage

## data collector

```
python3 -m scrpr -t 2 --follow
```

## api

```
python3 -m venv venv-api
. venv-api/bin/activate
pip install -r api/requirements.txt
uvicorn scrpr.api.main:app --reload
```

# data

Stores data in a Postgres schema, but also in flat CSV files.

Each run of the program creates about 13M (181K compressed) of csv data.

## example data

`$ head -10 csv-data/ec2/2023-01-18/Linux/us-east-1.csv`
```
date,instance_type,operating_system,region,cost_per_hr,cpu_ct,ram_size_gb,storage_type,network_throughput
2023-01-18,a1.medium,Linux,us-east-1,$0.0255,1,2 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,a1.large,Linux,us-east-1,$0.051,2,4 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,a1.xlarge,Linux,us-east-1,$0.102,4,8 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,a1.2xlarge,Linux,us-east-1,$0.204,8,16 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,a1.4xlarge,Linux,us-east-1,$0.408,16,32 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,a1.metal,Linux,us-east-1,$0.408,16,32 GiB,EBS Only,Up to 10 Gigabit
2023-01-18,t4g.nano,Linux,us-east-1,$0.0042,2,0.5 GiB,EBS Only,Up to 5 Gigabit
2023-01-18,t4g.micro,Linux,us-east-1,$0.0084,2,1 GiB,EBS Only,Up to 5 Gigabit
2023-01-18,t4g.small,Linux,us-east-1,$0.0168,2,2 GiB,EBS Only,Up to 5 Gigabit
```


## Problems

1. OOM
  * Large values of `-t` lead to WM crashes. Program is unaware of system
    resources available.
  * No confidence running unsupervised (remember to get a `DISPLAY`)
1. Reporting
  * Too much data; not enough knowledge
  * Table structure is inefficient (but relatively simple)
    - Database schema was designed around the program, not the other way around?
1. Mandatory CSV
  * `--export-to-csv` should be an option

