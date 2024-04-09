import time
from typing import List, Optional

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago

from include.etl import (
    insert_many,
    task_scrape_cards,
    task_scrape_details,
    task_store_data,
)

HEADLESS, REMOTE_HOST = True, None
zipcodes = [75, 92, 93, 94]


@dag(
    dag_id="etl",
    schedule="@monthly",
    start_date=days_ago(0),
    catchup=False,
    tags=["etl"],
)
def etl():

    @task(task_id="get_extraction_dates")
    def get_extraction_dates():
        try:
            from_date = pendulum.parse(Variable.get(key="TO_DATE"), tz="Europe/Paris")
        except KeyError:
            from_date = pendulum.now("Europe/Paris").add(hours=-1)
        to_date = pendulum.now("Europe/Paris")
        return {"FROM_DATE": from_date, "TO_DATE": to_date}

    @task(task_id="etl-cards", retries=3)
    def scrape_cards(
        rent_sale: str,
        zipcodes: List[int],
        dates: dict,
        headless: bool = True,
        remote_host: Optional[str] = None,
    ):
        return task_scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)

    @task(task_id="etl-details", retries=3)
    def scrape_details(input: dict):
        return task_scrape_details(input)

    @task(task_id="etl-store-data")
    def store_data(input: dict):
        hook = MongoHook(mongo_conn_id="mongo_default")
        with hook.get_conn() as client:
            _ = insert_many(input.get("cards"), client, input.get("rent_sale"), "cards")
            _ = insert_many(
                input.get("details"), client, input.get("rent_sale"), "details"
            )
        return task_store_data(input)

    @task_group(group_id="etl-extract-store-data")
    def extract_store_data(
        rent_sale: str,
        zipcodes: List[int],
        dates: dict,
        headless: bool = True,
        remote_host: Optional[str] = None,
    ):
        return store_data(
            scrape_details(
                scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)
            )
        )

    @task(task_id="set_extraction_dates")
    def set_extraction_dates(dates: dict):
        Variable.set(key="FROM_DATE", value=dates["FROM_DATE"])
        Variable.set(key="TO_DATE", value=dates["TO_DATE"])

    dates = get_extraction_dates()
    extract_store_data.partial(zipcodes=zipcodes, dates=dates).expand(
        # rent_sale=["location", "achat"]
        rent_sale=["location"]
    ) >> set_extraction_dates(dates)


etl()
