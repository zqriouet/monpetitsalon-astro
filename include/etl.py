import time

import pendulum
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchWindowException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

from monpetitsalon.agents import CardsNavigationAgent, DetailsNavigationAgent
from monpetitsalon.driver import get_driver
from monpetitsalon.parsers import CardItemParser, DetailItemParser
from monpetitsalon.query import Query
from monpetitsalon.scrapers import CardsPageScraper, DetailsPageScraper
from monpetitsalon.utils import wait_for_element


def indices(ct: int):
    return (ct - 1) // 24 + 1, (ct - 1) % 24


def extract_single_page_data(agent, driver):
    agent.wait_loading(driver)
    soup = agent.scrape(driver)
    agent.save_data()
    new_items = [agent.parse_raw_item(item) for item in soup]
    agent.add_iteration()
    return new_items


def extract_data(agent, driver, items=[], sleep=1):
    terminated = False
    while not terminated:
        time.sleep(sleep)
        new_items = extract_single_page_data(agent, driver)
        items.extend(new_items)
        page_ct = agent.get_page_ct(driver)
        agent.set_page_ct(page_ct)
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
                driver = get_driver(headless, remote_host)
                driver.get(f"{cards_agent.query.url}&page={cards_agent.page_ct + 1}")
                break
            except TimeoutException:
                print("timeout exception")
                retries += 1
        cards_agent.reject_cookies(driver)
        cards = extract_data(cards_agent, driver, cards, sleep)
        return (cards, driver)
    except (
        NoSuchWindowException,
        ElementClickInterceptedException,
        WebDriverException,
    ) as e:
        print(f"{e}\ndriver reloaded")
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
                driver = get_driver(headless, remote_host)
                driver.get(f"{details_agent.query.url}&page={page_i}")
                break
            except TimeoutException:
                print("timeout exception")
                retries += 1
        details_agent.reject_cookies(driver)
        wait_for_element(driver, CardsPageScraper.css_selector, 10)
        driver.find_elements(by=By.CSS_SELECTOR, value=CardsPageScraper.css_selector)[
            item_i
        ].click()
        details = extract_data(details_agent, driver, details, sleep)
        return (details, driver)
    except (
        NoSuchWindowException,
        ElementClickInterceptedException,
        WebDriverException,
    ) as e:
        print(f"{e}\ndriver reloaded")
        return extract_details(details_agent, details, sleep, headless, remote_host)


if __name__ == "__main__":
    headless = False
    rent_sale, zipcodes, from_date = (
        "location",
        [75, 92, 93, 94],
        pendulum.now("Europe/Paris").add(hours=-6),
    )
    query = Query(rent_sale, zipcodes, from_date)

    max_pages = 5
    cards_agent = CardsNavigationAgent(query, None, max_it=max_pages)
    cards, driver = extract_cards(cards_agent)

    n_cards = len(cards)
    details_agent = DetailsNavigationAgent(query, None, max_it=n_cards * 2)
    details_agent.set_page_ct(n_cards + 1)
    details, driver = extract_details(details_agent)

    print(len(cards))
    print(len(set([c.get("listing_id") for c in cards])))
    print(len(details))
    print(len(set([d.get("listing_id") for d in details])))
