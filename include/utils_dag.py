import json
import os
from typing import List

import pendulum
from monpetitsalon.agents import CardsNavigationAgent, DetailsNavigationAgent
from monpetitsalon.etl import extract_cards, extract_details
from monpetitsalon.query import Query

DATA_PATH = os.getenv("DATA_PATH", "")


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


def task_store_data(input: dict):
    cards = input.get("cards")
    details = input.get("details")
    rent_sale = input.get("rent_sale")
    dates = input.get("dates")
    path = os.path.join(
        DATA_PATH,
        dates.get("TO_DATE").format("YYYY-MM-DDTHH:mm:ss"),
        rent_sale,
    )
    if not os.path.exists(path):
        os.makedirs(path)
    for data, label in zip([cards, details], ["cards", "details"]):
        file_path = os.path.join(path, f"{label}.json")
        with open(file_path, "w") as f:
            json.dump(data, f)

    return input


def tasks_extract_store_data(
    rent_sale: str,
    zipcodes: List[int],
    dates: dict,
    headless: bool = True,
    remote_host: str | None = None,
):
    return task_store_data(
        task_scrape_details(
            task_scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)
        )
    )


if __name__ == "__main__":
    dates = {
        "FROM_DATE": pendulum.now("Europe/Paris").add(hours=-10),
        "TO_DATE": pendulum.now("Europe/Paris"),
    }
    zipcodes = [75, 92, 93, 94]
    rent_sale = "rent"
    output = tasks_extract_store_data(rent_sale, zipcodes, dates, True, None)
