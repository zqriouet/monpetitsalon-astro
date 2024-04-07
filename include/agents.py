import logging
import os
import time
from urllib.parse import parse_qs, urlparse

# from monpetitsalon.driver import get_driver
from monpetitsalon.parsers import BaseItemParser, CardItemParser, DetailItemParser
from monpetitsalon.query import Query
from monpetitsalon.scrapers import BasePageScraper, CardsPageScraper, DetailsPageScraper
from monpetitsalon.utils import wait_for_element
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.by import By


class BaseNavigationAgent:

    ITEMS_CSS_SELECTOR = ""
    NEXT_PAGE_BUTTON_SELECTOR = ""

    def __init__(
        self,
        scraper: BasePageScraper,
        parser: BaseItemParser,
        query: Query,
        storage_address: str,
        max_it: int = -1,
    ):
        self.scraper = scraper
        self.parser = parser
        self.query = query
        self.storage_address = storage_address
        self.max_iterations = max_it
        self.iterations = 0
        self.page_ct = 0
        self.page_id = None

    def scrape(self, driver):
        return self.scraper.scrape(driver)

    def parse_raw_item(self, soup):
        return self.parser.parse_raw_item(soup)

    @classmethod
    def wait_loading(cls, driver, timeout=10, sleep=1):
        try:
            wait_for_element(driver, cls.ITEMS_CSS_SELECTOR, timeout)
        except TimeoutException:
            driver.refresh()
            time.sleep(sleep)

    @classmethod
    def reject_cookies(cls, driver, timeout=10):
        try:
            close_cookies = wait_for_element(
                driver, "span.didomi-continue-without-agreeing", timeout
            )
            close_cookies.click()
        except TimeoutException:
            pass

    @classmethod
    def escape_popup_error(cls, driver, timeout=10):
        try:
            popup_error = wait_for_element(
                driver, "button.btnCloseModal, div.errorContainer a", timeout
            )
            popup_error.click()
        except TimeoutException:
            pass

    @classmethod
    def next_button(cls, driver, timeout=1):
        try:
            next_btn = wait_for_element(driver, cls.NEXT_PAGE_BUTTON_SELECTOR, timeout)
            if next_btn.get_attribute("disabled") != "true":
                return next_btn
        except StaleElementReferenceException:
            time.sleep(3)
            return cls.next_button(driver, timeout)
        except TimeoutException:
            pass

    def terminated(self, driver, from_date):
        if self.max_iterations > 0:
            if self.iterations >= self.max_iterations:
                return True
        return self.next_button(driver) is None

    @classmethod
    def go_to_next_page(cls, driver):
        try:
            next_btn = cls.next_button(driver)
            next_btn.click()
        except (ElementClickInterceptedException, StaleElementReferenceException):
            cls.escape_popup_error(driver)
            next_btn = cls.next_button(driver)
            next_btn.click()

    def set_page_id(self, page_id):
        self.page_id = page_id

    def get_page_id(self, driver):
        pass

    def set_page_ct(self, page_ct):
        self.page_ct = page_ct

    def get_page_ct(self, driver):
        pass

    # def save_data(self, file_path: str | None = None):
    #     if file_path is None:
    #         print("save data")

    def check_storage_path(self):
        pass

    def add_iteration(self):
        self.iterations += 1


class CardsNavigationAgent(BaseNavigationAgent):

    ITEMS_CSS_SELECTOR = CardsPageScraper.css_selector
    NEXT_PAGE_BUTTON_SELECTOR = "a.pagination__go-forward-button"

    def __init__(self, query: Query, storage_address: str, max_it: int = 2):
        super().__init__(
            scraper=CardsPageScraper,
            parser=CardItemParser,
            query=query,
            storage_address=storage_address,
            max_it=max_it,
        )

    def get_page_ct(self, driver):
        try:
            parsed_url = urlparse(driver.current_url)
            params = parse_qs(parsed_url.query)
            try:
                page_ct = int(params.get("page")[0])
            except TypeError:
                page_ct = 1
            self.set_page_ct(page_ct)
            return page_ct
        except (ElementClickInterceptedException, StaleElementReferenceException):
            self.escape_popup_error(driver)
            self.get_page_ct(driver)

    def terminated(self, driver, from_date):
        if super().terminated(driver, from_date):
            return True
        items = self.scrape(driver)
        last_listing_modification_date = self.parser.parse_modification_date(items[-1])
        logging.info(
            "last listing modification date : %s", last_listing_modification_date
        )
        return last_listing_modification_date < from_date


class DetailsNavigationAgent(BaseNavigationAgent):

    ITEMS_CSS_SELECTOR = DetailsPageScraper.css_selector
    NEXT_PAGE_BUTTON_SELECTOR = "a#selectPreviousDetailedSheet"

    def __init__(self, query: Query, storage_address: str, max_it: int = 5):
        super().__init__(
            scraper=DetailsPageScraper,
            parser=DetailItemParser,
            query=query,
            storage_address=storage_address,
            max_it=max_it,
        )

    def get_page_id(self, driver):
        try:
            page_id = (
                driver.find_element(
                    by=By.CSS_SELECTOR, value=DetailItemParser.css_selector
                )
                .get_attribute("id")
                .replace("section-detailedSheet_", "")
            )
            self.set_page_id(page_id)
            return page_id
        except (ElementClickInterceptedException, StaleElementReferenceException):
            self.escape_popup_error(driver)
            return self.get_page_id(driver)

    def get_page_ct(self, driver):
        try:
            page_ct = driver.find_element(
                by=By.CSS_SELECTOR, value="span.currentDetailedSheetNumber"
            ).text.replace(" ", "")
            self.set_page_ct(int(page_ct))
            return self.page_ct
        except Exception as e:
            print(f"problem with page_ct : {e}")
