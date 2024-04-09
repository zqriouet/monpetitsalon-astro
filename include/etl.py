import json
import logging
import os
import time
from typing import List

import pendulum
from monpetitsalon.driver import get_driver
from monpetitsalon.query import Query
from monpetitsalon.scrapers import CardsPageScraper
from monpetitsalon.utils import wait_for_element
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchWindowException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

from include.agents import CardsNavigationAgent, DetailsNavigationAgent

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)
DATA_PATH = os.getenv("DATA_PATH", "./data")


def indices(ct: int):
    return (ct - 1) // 24 + 1, (ct - 1) % 24


def insert_many(data, client, db, collection):
    if db.lower() in ["achat", "location", "achats", "locations"]:
        db = db.lower().replace("s", "") + "s"
    else:
        raise ValueError(
            f'Incorrect database name : {db}\nInsert into databases "achats" or "locations"'
        )
    if collection.lower() in ["card", "detail", "cards", "details"]:
        collection = collection.lower().replace("s", "") + "s"
    else:
        raise ValueError(
            f'Incorrect database name : {collection}\nInsert into collections "cards" or "details"'
        )
    data = list(
        map(
            lambda listing: listing
            | {"extraction_date": pendulum.now(tz="Europe/Paris")},
            data,
        )
    )
    inserted_data = client[db][collection].insert_many(data)
    return inserted_data


def extract_single_page_data(agent, driver):
    agent.wait_loading(driver)
    soup = agent.scrape(driver)
    # agent.save_data()
    new_items = [agent.parse_raw_item(item) for item in soup]
    agent.add_iteration()
    return new_items


def extract_data(agent, driver, items=[], sleep=1):
    terminated = False
    logging.info(f"Start {agent.query.rent_sale}s extraction ...")
    logging.info(f"URL : {agent.query.url}")
    while not terminated:
        time.sleep(sleep)
        new_items = extract_single_page_data(agent, driver)
        items.extend(new_items)
        page_ct = agent.get_page_ct(driver)
        agent.set_page_ct(page_ct)
        logging.info(f"Page extracted : {page_ct: <5}{agent.get_page_id(driver)}")
        # logging.info(f"Page extracted : {agent.get_page_id(driver) or page_ct}")
        try:
            last_listing_modification_date = agent.get_last_listing_modification_date(
                driver
            )
            logging.info(
                "Last listing date : %s",
                last_listing_modification_date.format("YYYY-MM-DD HH:mm:ss"),
            )
            logging.info(
                "From date : %s", agent.query.from_date.format("YYYY-MM-DD HH:mm:ss")
            )
        except AttributeError:
            pass
        terminated = agent.terminated(driver, agent.query.from_date)
        if not terminated:
            agent.go_to_next_page(driver)
    return items


def extract_cards(cards_agent, cards=[], sleep=1, headless=False, remote_host=None):
    try:
        retries = 1
        while retries <= 5:
            try:
                # driver = next(get_driver(headless, remote_host))
                logging.info("Load driver ...")
                driver = get_driver(headless, remote_host)
                driver.get(f"{cards_agent.query.url}&page={cards_agent.page_ct + 1}")
                logging.info("Driver loaded !")
                break
            except TimeoutException:
                logging.info("timeout exception")
                retries += 1
        cards_agent.reject_cookies(driver)
        cards = extract_data(cards_agent, driver, cards, sleep)
        logging.info("Extraction finished !")
        logging.info(f"{len(cards)} cards extracted")
        return (cards, driver)
    except (
        NoSuchWindowException,
        ElementClickInterceptedException,
        WebDriverException,
    ) as e:
        logging.info(f"{e}\ndriver reloaded")
        return extract_cards(cards_agent, cards, sleep, headless, remote_host)


def extract_details(
    details_agent, details=[], sleep=1, headless=False, remote_host=None
):
    try:
        page_i, item_i = indices(max(1, details_agent.page_ct - 1))
        retries = 1
        while retries <= 5:
            try:
                # driver = next(get_driver(headless, remote_host))
                logging.info("Load driver ...")
                driver = get_driver(headless, remote_host)
                driver.get(f"{details_agent.query.url}&page={page_i}")
                logging.info("Driver loaded !")
                break
            except TimeoutException:
                logging.info("timeout exception")
                retries += 1
        details_agent.reject_cookies(driver)
        wait_for_element(driver, CardsPageScraper.css_selector, 10)
        driver.find_elements(by=By.CSS_SELECTOR, value=CardsPageScraper.css_selector)[
            item_i
        ].click()
        details = extract_data(details_agent, driver, details, sleep)
        logging.info("Extraction finished !")
        logging.info(f"{len(details)} details extracted")
        return (details, driver)
    except (
        NoSuchWindowException,
        ElementClickInterceptedException,
        WebDriverException,
    ) as e:
        logging.info(f"{e}\ndriver reloaded")
        return extract_details(details_agent, details, sleep, headless, remote_host)


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
    logging.info("Start ETL ...")
    return task_store_data(
        task_scrape_details(
            task_scrape_cards(rent_sale, zipcodes, dates, headless, remote_host)
        )
    )


if __name__ == "__main__":
    # headless = False
    # rent_sale, zipcodes, from_date = (
    #     "location",
    #     [75, 92, 93, 94],
    #     pendulum.now("Europe/Paris").add(hours=-6),
    # )
    # query = Query(rent_sale, zipcodes, from_date)

    # max_pages = 5
    # cards_agent = CardsNavigationAgent(query, None, max_it=max_pages)
    # cards, driver = extract_cards(cards_agent)

    # n_cards = len(cards)
    # details_agent = DetailsNavigationAgent(query, None, max_it=n_cards * 2)
    # details_agent.set_page_ct(n_cards + 1)
    # details, driver = extract_details(details_agent)

    # print(len(cards))
    # print(len(set([c.get("listing_id") for c in cards])))
    # print(len(details))
    # print(len(set([d.get("listing_id") for d in details])))

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
        # filename=LOG_PATH
    )

    dates = {
        "FROM_DATE": pendulum.now("Europe/Paris").add(hours=-1),
        "TO_DATE": pendulum.now("Europe/Paris"),
    }
    zipcodes = [75, 92, 93, 94]
    rent_sale = "location"
    output = tasks_extract_store_data(rent_sale, zipcodes, dates, True, None)
