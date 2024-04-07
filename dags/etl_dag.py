from typing import List, Optional

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.utils.dates import days_ago

from include import utils_dag

HEADLESS, REMOTE_HOST = True, None
zipcodes = [75, 92, 93, 94]


@dag(
    dag_id="etl",
    schedule="@hourly",
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
            from_date = pendulum.now("Europe/Paris").add(hours=-10)
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
        return utils_dag.task_scrape_cards(
            rent_sale, zipcodes, dates, headless, remote_host
        )

    @task(task_id="etl-details", retries=3)
    def scrape_details(input: dict):
        return utils_dag.task_scrape_details(input)

    @task(task_id="etl-store-data")
    def store_data(input: dict):
        return utils_dag.task_store_data(input)

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
        pass
        Variable.set(key="FROM_DATE", value=dates["FROM_DATE"])
        Variable.set(key="TO_DATE", value=dates["TO_DATE"])

    dates = get_extraction_dates()
    # scrape_cards(zipcodes=zipcodes, rent_sale="rent", dates=dates)
    extract_store_data.partial(zipcodes=zipcodes, dates=dates).expand(
        rent_sale=["rent", "sale"]
    ) >> set_extraction_dates(dates)


etl()
