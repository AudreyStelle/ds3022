import json
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# public github activity for scott chacon
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/users/schacon/events/public"
PER_PAGE = 100
PAGES = 10


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of github activity and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

# local duckdb file, created if missing
conn = duckdb.connect("actions.duckdb")  

@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    # just a placeholder for now. remove the print below this is built out
    print("something")


# name the flow something you can identify as yourself
@flow(name="nem2p-flow")
def main_flow():
    ingest_data()

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)