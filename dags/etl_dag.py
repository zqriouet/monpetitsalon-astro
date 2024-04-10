from typing import List, Optional

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
from monpetitsalon.query import Query

from include.agents import CardsNavigationAgent, DetailsNavigationAgent
from include.etl import extract_cards, extract_details, insert_many

HEADLESS, REMOTE_HOST = True, None
zipcodes = [75, 92, 93, 94]

params = {
    "max_retries": Param(10, type="integer", minimum=0, maximum=20),
    "scrape_over_last_hours": Param(1, type="integer", minimum=0, maximum=240),
    "sleep_between_cards": Param(1, type="integer", minimum=0, maximum=10),
    "sleep_between_details": Param(2, type="integer", minimum=0, maximum=10),
}


@dag(
    dag_id="etl",
    schedule="@hourly",
    start_date=days_ago(0),
    catchup=False,
    tags=["etl"],
    params=params,
)
def etl():

    @task(task_id="get-extraction-dates")
    def get_extraction_dates():
        try:
            from_date = pendulum.parse(Variable.get(key="TO_DATE"), tz="Europe/Paris")
        except KeyError:
            from_date = pendulum.now("Europe/Paris").add(
                days=-params.get("scrape_over_last_hours")
            )
        to_date = pendulum.now("Europe/Paris")
        return {"FROM_DATE": from_date, "TO_DATE": to_date}

    def extract_store_data(
        rent_sale: str,
        zipcodes: List[int],
        dates: dict,
        headless: bool = True,
        remote_host: Optional[str] = None,
    ):

        @task_group(group_id=f"extract-store-data-{rent_sale}")
        def task_group_wrapper():

            @task(
                task_id=f"etl-scraping-cards-{rent_sale}",
                retries=params.get("max_retries"),
            )
            def task_scrape_cards(
                rent_sale: str,
                zipcodes: List[int],
                dates: dict,
                headless: bool = True,
                remote_host: str | None = None,
                max_it: int = 2400,
            ):
                query = Query(rent_sale, zipcodes, dates.get("FROM_DATE"))
                cards_agent = CardsNavigationAgent(query, None, max_it)
                cards, _ = extract_cards(
                    cards_agent,
                    [],
                    params.get("sleep_between_cards"),
                    headless,
                    remote_host,
                )
                return {
                    "cards": cards,
                    "rent_sale": rent_sale,
                    "zipcodes": zipcodes,
                    "dates": dates,
                    "headless": headless,
                    "remote_host": remote_host,
                }

            @task(
                task_id=f"etl-scraping-details-{rent_sale}",
                retries=params.get("max_retries"),
            )
            def task_scrape_details(input: dict):
                query = Query(
                    input.get("rent_sale"),
                    input.get("zipcodes"),
                    input.get("dates").get("FROM_DATE"),
                )
                details_agent = DetailsNavigationAgent(
                    query, None, max_it=2 * len(input.get("cards"))
                )
                details_agent.set_page_ct(len(input.get("cards")) + 1)
                details, _ = extract_details(
                    details_agent,
                    [],
                    params.get("sleep_between_details"),
                    input.get("headless"),
                    input.get("remote_host"),
                )
                output = input | {"details": details}
                return output

            @task(task_id=f"etl-insert-mongodb-{rent_sale}")
            def task_insert_mongodb(input: dict):
                hook = MongoHook(mongo_conn_id="mongo_default")
                with hook.get_conn() as client:
                    _ = insert_many(input.get("cards"), client, rent_sale, "cards")
                    _ = insert_many(input.get("details"), client, rent_sale, "details")

            task_insert_mongodb(
                task_scrape_details(
                    task_scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)
                )
            )

        return task_group_wrapper

    @task(task_id="set-extraction-dates")
    def set_extraction_dates(dates: dict):
        Variable.set(key="FROM_DATE", value=dates["FROM_DATE"])
        Variable.set(key="TO_DATE", value=dates["TO_DATE"])

    dates = get_extraction_dates()
    (
        extract_store_data("location", zipcodes, dates)()
        >> extract_store_data("achat", zipcodes, dates)()
        >> set_extraction_dates(dates)
    )


etl()


"""


@dag(
    dag_id="etl",
    schedule="@monthly",
    start_date=days_ago(0),
    catchup=False,
    tags=["etl"],
)
def etl():

    @task(task_id="get-extraction-dates")
    def get_extraction_dates():
        try:
            from_date = pendulum.parse(Variable.get(key="TO_DATE"), tz="Europe/Paris")
        except KeyError:
            from_date = pendulum.now("Europe/Paris").add(minutes=-10)
        to_date = pendulum.now("Europe/Paris")
        return {"FROM_DATE": from_date, "TO_DATE": to_date}

    @task_group(group_id="extract-store-data")
    def extract_store_data(
        rent_sale: str,
        zipcodes: List[int],
        dates: dict,
        headless: bool = True,
        remote_host: Optional[str] = None,
    ):
        @task(task_id=f"etl-scraping-cards-{rent_sale}")
        def task_scrape_cards(
            rent_sale: str,
            zipcodes: List[int],
            dates: dict,
            headless: bool = True,
            remote_host: str | None = None,
            max_it: int = 2400,
        ):
            query = Query(rent_sale, zipcodes, dates.get("FROM_DATE"))
            cards_agent = CardsNavigationAgent(query, None, max_it)
            cards, _ = extract_cards(cards_agent, [], 1, headless, remote_host)
            return {
                "cards": cards,
                "rent_sale": rent_sale,
                "zipcodes": zipcodes,
                "dates": dates,
                "headless": headless,
                "remote_host": remote_host,
            }

        @task(task_id=f"etl-scraping-details-{rent_sale}")
        def task_scrape_details(input: dict):
            query = Query(
                input.get("rent_sale"),
                input.get("zipcodes"),
                input.get("dates").get("FROM_DATE"),
            )
            details_agent = DetailsNavigationAgent(
                query, None, max_it=2 * len(input.get("cards"))
            )
            details_agent.set_page_ct(len(input.get("cards")) + 1)
            details, _ = extract_details(
                details_agent, [], 1, input.get("headless"), input.get("remote_host")
            )
            output = input | {"details": details}
            return output

        @task(task_id=f"etl-insert-mongodb-{rent_sale}")
        def task_insert_mongodb(input: dict):
            hook = MongoHook(mongo_conn_id="mongo_default")
            with hook.get_conn() as client:
                _ = insert_many(input.get("cards"), client, rent_sale, "cards")
                _ = insert_many(input.get("details"), client, rent_sale, "details")

        task_insert_mongodb(
            task_scrape_details(
                task_scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)
            )
        )

    @task(task_id="set-extraction-dates")
    def set_extraction_dates(dates: dict):
        Variable.set(key="FROM_DATE", value=dates["FROM_DATE"])
        Variable.set(key="TO_DATE", value=dates["TO_DATE"])

    dates = get_extraction_dates()
    (
        extract_store_data("location", zipcodes, dates)
        >> extract_store_data("achat", zipcodes, dates)
        >> set_extraction_dates(dates)
    )

etl()


"""
