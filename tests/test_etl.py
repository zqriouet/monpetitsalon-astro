import pendulum
import pytest

from monpetitsalon.agents import CardsNavigationAgent, DetailsNavigationAgent
from monpetitsalon.etl import extract_cards, extract_details
from monpetitsalon.query import Query

headless = True


@pytest.fixture
def query():
    rent_sale, zipcodes, from_date = (
        "location",
        [75, 92, 93, 94],
        pendulum.now("Europe/Paris").add(days=-10),
    )
    return Query(rent_sale, zipcodes, from_date)


def test_extract_cards(query):
    max_pages = 2
    cards_agent = CardsNavigationAgent(query, None, max_it=max_pages)
    cards, driver = extract_cards(cards_agent, headless=headless)
    driver.quit()
    assert len(cards) == max_pages * 24


# @pytest.mark.flaky(reruns=3, reruns_delay=5)
def test_extract_details(query):
    max_it = 5
    details_agent = DetailsNavigationAgent(query, None, max_it=max_it * 2)
    details_agent.set_page_ct(max_it + 1)
    details, driver = extract_details(details_agent, headless=headless)
    driver.quit()
    assert len(details) == max_it
