import time
from datetime import datetime
import csv
from pathlib import Path
from math import floor
from typing import List, Tuple
import zipfile
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from copy import copy
import threading
import argparse
from argparse import BooleanOptionalAction
from multiprocessing import cpu_count
# 99% sure catching SIGINT hangs Selenium
# update: we have to wait for Selenium to close all of its webdrivers if you
# don't want all those stray processes. If you hit ^C in the middle of the
# program while it is throwing exceptions (in my case), it will cease closing those
# last firefox processes. Doesn't explain the behavior I observed when catching
# SIGINT, but something is better than nothing since I can't reproduce the
# error to begin with.
# import signal
import os
import shutil
from uuid import uuid1
from contextlib import contextmanager
import dotenv
import json

from selenium.webdriver.common.by import By
# from selenium.webdriver.firefox.service import Service
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common import actions, action_chains
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import psycopg2
from psycopg2.errors import UniqueViolation
from psycopg2 import sql

from ec2.instance import Instance, PGInstance


"""
Scrapes pricing information for EC2 instance types for available regions.
"""
logger = logging.getLogger(__name__)
ERRORS = []
ALREADY_EXISTED_COUNT = 0


###############################################################################
# classes and functions
###############################################################################
class ScrprException(Exception):
    global ERRORS

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        ERRORS.append((args,))
    pass


class DatabaseConfig:
    """
    NOTE local configuration for connecting to Postgres (.pgpass, pg_ident.conf,
    environment variables, etc) will supply connection DSL values. 

    NOTE django uses psycopg3. : https://github.com/django/django/blob/main/django/db/backends/postgresql/client.py
    """
    def __init__(self, host=None, port=None, dbname=None, user=None):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self._password = None
    
    def load(self, env_file='.env'):
        config = dotenv.dotenv_values(env_file)
        if config:
            self.host = config.get("db_host", "localhost")
            self.port = int(config.get("db_port", 5432))
            self.dbname = config.get("db_dbname", "scrpr")
            self.user = config.get("db_user", "scrpr")
            self.password = config.get("db_password", None)
            return True
        else:
            logger.warning("No configuration found at '{}'".format(env_file))
            return False
        

    def __repr__(self) -> str:
        if self.password:
            _pw = self.password
            self.password = '*******'
            repr = self.get_dsl()
            self.password = _pw
            return repr
        return self.get_dsl()

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, pw):
        self._password = pw
    
    def get_dsl(self):
        dsl = "host={} port={} dbname={} user={}".format(
            self.host, self.port, self.dbname, self.user
        )
        if self.password:
            dsl += " password={}".format(self.password)
        return dsl


@dataclass
class DataCollectorConfig:
    """
    Required options to instantiate an DataCollector.

    human_date: (YYYY-MM-DD) required
    db_config: None to supress writing to a database.
    csv_data_dir: None to supress writing to a database.
    url: no touch. nein.
    headless: False to start windowed browsers
    window_w: width of browser resolution (still applies if headless=False)
    window_h: height of browser resolution (still applies if headless=False)
    """
    human_date: str
    db_config: DatabaseConfig = None
    csv_data_dir: str = None
    url: str = 'https://aws.amazon.com/ec2/pricing/on-demand/'
    headless: bool = True  # @@@ unsettable
    window_w: int = 1920  # @@@ unsettable
    window_h: int = 1080  # @@@ unsettable


seconds_to_timer = lambda x: f"{floor(x/60)}m:{x%60:.1000f}s ({x} seconds)"  # noqa: E731


class ThreadDivvier:
    """
    Converts machine time processing data into developer time debugging exceptions.
    """
    def __init__(self, config: DataCollectorConfig, thread_count=cpu_count()) -> None:
        """
        Initialize one DataCollector for use in a thread, creating thread_count DataCollector instances with identical configuration.
        Each thread manages its own Selenium.WebDriver.
        """
        logger.debug("init ThreadDivvier with {} threads".format(thread_count))
        self.thread_count = thread_count
        self.drivers: List[DataCollector] = []
        self.config = config
        self.done = False
        self.init_scrapers()
    
    def init_scraper(self, thread_id: int, config: DataCollectorConfig):
        self.drivers.append(DataCollector(_id=thread_id, config=config))
        logger.debug(f"initialized {thread_id=}")

    def init_scrapers(self):
        """
        Initialize scrapers one at a time.
        As scrapers become initialized, give them to the ThreadDivvier's drivers pool.
        """
        next_id = 0
        # start init for each driver
        logger.info("initializing {} drivers".format(self.thread_count))
        for _ in range(self.thread_count):
            logger.debug("initializing new driver with id '{}'".format(next_id))
            t = threading.Thread(
                name=next_id,
                target=self.init_scraper,
                args=(next_id, self.config),
                daemon=False,
            )
            next_id += 1
            t.start()

        # wait until each driver's init is finished
        # drivers are only added to self.init_drivers *after* they are done being initialized
        # i.e. when __init__ has finished
        while len(self.drivers) != self.thread_count:
            pass
        logger.debug("Finished initializing {} scrapers".format(len(self.drivers)))
        return

    def run_threads(self, arg_queue: List[tuple]):
        """
        Takes a list of tuples (operating_system, region) and gathers corresponding EC2 instance pricing data.
        Items in the arg_queue are removed as work is completed, until there are no items left in the queue.
        """
        logger.info(f"running {self.thread_count} threads")
        _arg_queue = copy(arg_queue)  # feels wrong to pop from a queue that might could be acessed outside of func
        logger.debug("Test to make sure drivers are not locked...")
        for d in self.drivers:
            assert not d.lock.locked()
        logger.debug("ok.")
        while _arg_queue:
            for d in self.drivers:
                if not d.lock.locked():
                    d.lock.acquire()
                    try:
                        args = _arg_queue.pop()
                    except IndexError:
                        logger.warning(f"driver {d._id} tried to pop from an empty queue")
                        continue
                    logger.info(f"{len(_arg_queue)} left in queue")
                    # t = threading.Thread(name='-'.join(args), target=d.scrape_and_store, args=args, daemon=False)
                    t = threading.Thread(target=d.scrape_and_store, args=args, daemon=False)
                    t.start()
                    logger.debug(f"worker id {d._id} running new thread {'-'.join(args)}")
        # determine when all threads have finished
        # wait to return until all threads have finished their work
        threads_finished = []
        logger.info("waiting for last threads to finish juuuust a sec")
        while len(threads_finished) != self.thread_count:
            # @@@ bug when number of threads is greater than the arg queue
            for d in self.drivers:
                # @@ if an exception is thrown in a thread it may not properly release it's lock.
                # This will cause the script to hang after it is done running, at least.
                if not d.lock.locked() and d._id not in threads_finished:
                    threads_finished.append(d._id)
                    d.driver.close()
        return


class DataCollector:
    """
    Base class for selenium drivers to interface with a ThreadDivvier.
    """

    def __init__(self, _id, config: DataCollectorConfig, _test_driver=None):
        logger.debug("init DataCollector")
        self.lock = threading.Lock()
        self.lock.acquire()
        self._id = _id
        ###################################################
        # config
        ###################################################
        self.url = config.url
        self.human_date = config.human_date
        self.csv_data_dir = config.csv_data_dir
        self.db_config = config.db_config

        if _test_driver is None:
            self.driver = self.get_driver(
                headless=config.headless,
                window_w=config.window_w,
                window_h=config.window_h
            )
            self.prep_driver()
        else:
            self.driver = _test_driver
        logger.debug("Finished init of thread {} without errors".format(self._id))

    def prep_driver(self):
        delay = 0.5
        try:
            # @@@ is this needed?
            self.waiter = WebDriverWait(self.driver, 10)
            self.driver.get(self.url)
            logger.debug("Initializing worker with id {}".format(self._id))
            logger.debug(f"{self._id} begin nav_to")
            time.sleep(delay)
            sidebar = self.driver.find_element(By.CLASS_NAME, 'lb-sidebar-content')
            logger.debug("sidebar content located")
            for elem in sidebar.find_elements(By.XPATH, 'div/a'):
                if elem.text.strip() == self.NavSectionEC2.ON_DEMAND_PRICING:
                    elem.click()
                    break
            logger.debug("navving...")
            time.sleep(delay)
            logger.debug("wait untill iFrame located...")
            self.waiter.until(ec.visibility_of_element_located((By.ID, "iFrameResizer0")))
            self.iframe = self.driver.find_element('id', "iFrameResizer0")
            logger.debug("done. waiting 3 seconds before releasing lock...")
            time.sleep(delay)
            self.lock.release()
        except Exception as e:
            logger.critical("While initializing worker '{}', an exception occurred which requires closing the Selenium WebDriver: {}".format(self._id, e), exc_info=True)
            self.driver.close()

    def get_driver(self, browser_binary='/usr/bin/google-chrome', automation_driver='/usr/local/bin/chromedriver', headless=True, window_w=1920, window_h=1080):
        # options = webdriver.FirefoxOptions()
        # options.binary_location = '/usr/bin/firefox-esr'
        # options.add_argument('-headless')
        # driverService = Service('/usr/local/bin/geckodriver')
        # self.driver = webdriver.Firefox(service=driverService, options=options)
        options = webdriver.ChromeOptions()
        options.binary_location = browser_binary
        options.add_argument("window-size={},{}".format(window_w, window_h))
        if headless:
            options.add_argument('-headless')
        driverService = Service(automation_driver)
        return webdriver.Chrome(service=driverService, options=options)

    def scroll(self, amt):
        action = action_chains.ActionChains(self.driver)
        action.scroll_by_amount(0, amt)
        action.perform()
        time.sleep(1)


class EC2DataCollector(DataCollector):
    """
    Instantiates a Selenium WebDriver for Chrome.
    """
    class NavSectionEC2:
        """3+ lines of code"""
        ON_DEMAND_PRICING = "On-Demand Pricing"

    def __init__(self, _id, config: DataCollectorConfig, _test_driver=None):
        super().__init__(_id, config, _test_driver)

    def get_available_regions_and_os(self) -> List[tuple]:
        """
        Return a tuple of all the possible combinations of operating systems and regions for which to iterate over
        NOTE: contains try/catch for driver
        NOTE: switches to iframe
        """
        delay = 0.5
        results = {}
        # @@ what if an OS isn't available in a region?
        logger.debug("get regions and os")
        try:
            # @@@ why?
            self.driver.refresh()
            logger.debug("waiting for page to refresh...")
            time.sleep(5)
            # @@@ self.iframe?
            iframe = self.driver.find_element('id', "iFrameResizer0")
            self.driver.switch_to.frame(iframe)

            #########
            # OS
            #########
            logger.info("getting available operating systems...")

            available_operating_systems = []
            os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
            os_selection_box.click()
            for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
                available_operating_systems.append(elem.text)
                results[elem.text] = []
            # collapse
            logger.debug("got operating systems, collapsing menu...")
            time.sleep(delay)
            os_selection_box.click()

            #########
            # Regions
            #########
            logger.info("getting available regions...")
            time.sleep(delay)
            available_regions = []
            logger.debug("finding region selection box......")
            region_selection_box = self.driver.find_element('id', 'awsui-select-1-textbox')
            logger.debug("found region selection box")
            time.sleep(delay)
            region_selection_box.click()
            logger.debug("clicked region selection box")
            time.sleep(delay)
            for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
                available_regions.append(elem.text)
            logger.debug("got regions, collapsing menu...")
            time.sleep(delay)
            region_selection_box.click()
        finally:
            self.driver.switch_to.default_content()
        logger.debug("found {} operating systems and {} regions".format(len(available_operating_systems), len(available_regions)))
        return (available_operating_systems, available_regions)
    

    def get_available_operating_systems(self) -> List[str]:
        """
        Get all operating systems for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug("{} get available os".format(self._id))
        try:
            self.driver.switch_to.frame(self.iframe)
            available_operating_systems = []
            self.waiter.until(ec.visibility_of_element_located((By.ID, "awsui-select-2")))
            os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
            # expand menu
            os_selection_box.click()
            for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
                available_operating_systems.append(elem.text)
            # collapse
            os_selection_box.click()
            self.wait_for_menus_to_close()
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(e)
            raise e
        return available_operating_systems

    def select_operating_system(self, _os: str):
        """
        Select an operating system for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        delay = 0
        logger.debug("{} select os {}".format(self._id, _os))
        try:
            self.driver.switch_to.frame(self.iframe)
            logger.debug("Waiting for visibility of selection box...")
            self.waiter.until(ec.visibility_of_element_located((By.ID, "awsui-select-2")))
            os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
            logger.debug("found.")
            logger.debug("selecting operating system selection box...")
            time.sleep(delay)
            os_selection_box.click()
            time.sleep(delay)
            for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
                if elem.text == _os:
                    logger.debug("{} Found operating system '{}'".format(self._id, elem.text))
                    time.sleep(delay)
                    elem.click()
                    time.sleep(delay)
                    self.wait_for_menus_to_close()
                    self.driver.switch_to.default_content()
                    logger.debug("{} Selected operating system '{}'".format(self._id, _os))
                    return
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(e)
            raise e
        raise ScrprException("{} No such operating system: '{}'".format(self._id, _os))

    def get_available_regions(self) -> List[str]:
        """
        Get all aws regions for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug("{} get regions".format(self._id))
        try:
            self.driver.switch_to.frame(self.iframe)
            available_regions = []
            region_selection_box = self.driver.find_element('id', 'awsui-select-1-textbox')
            region_selection_box.click()
            for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
                available_regions.append(elem.text)
            region_selection_box.click()
            self.wait_for_menus_to_close()
            self.driver.switch_to.default_content()
        except Exception as e:
            logger.error(e)
            self.driver.close()
            raise e
        return available_regions

    def select_region(self, region: str):
        """
        Set the table of data to display for a given aws region
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug("{} select region '{}'".format(self._id, region))
        delay = 0
        try:
            self.driver.switch_to.frame(self.iframe)
            ################
            # Location Type
            ################
            location_type_select_box = self.driver.find_element(By.ID, 'awsui-select-0')
            location_type_select_box.click()
            time.sleep(delay)
            # self.wait_for_menus_to_close()

            # what you have to click firt in order to show the dropdown
            filter_selection = 'AWS Region'
            all_filter_options = self.driver.find_element(By.ID, 'awsui-select-0-dropdown')
            all_filter_options.find_element(By.XPATH, f'//*[ text() = "{filter_selection}"]').click()
            self.wait_for_menus_to_close()
            time.sleep(delay)

            ################
            # Region
            ################
            region_select_box = self.driver.find_element(By.ID, 'awsui-select-1-textbox')
            region_select_box.click()
            time.sleep(delay)
            search_box = self.driver.find_element(By.ID, 'awsui-input-0')
            search_box.send_keys(region)
            time.sleep(delay)
            for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
                if elem.text == region:
                    logger.debug("{} Found region '{}'".format(self._id, elem.text))
                    time.sleep(delay)
                    elem.click()
                    time.sleep(delay)
                    self.wait_for_menus_to_close()
                    self.driver.switch_to.default_content()
                    logger.debug("{} Selected region '{}'".format(self._id, region))
                    time.sleep(delay)
                    return True
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(e)
            raise e
        raise ScrprException("No such region: '{}'".format(region))

    def collect_ec2_data(self, _os: str, region: str) -> List[Instance]:
        """
        Select an operating system and region to fill the pricing page table with data, scrape it, and save it to a csv file.
        CSV files are saved in a parent directory of self.csv_data_dir, by date then by operating system. e.g. '<self.csv_data_dir>/2023-01-18/Linux'
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe

        Fails for small browser sizes
        """
        delay = 0.5
        logger.debug(f"{self._id} scrape all: {region=} {_os=}")
        time.sleep(delay)

        try:
            logger.debug("{} selecting operating system...".format(self._id))
            time.sleep(delay)
            self.select_operating_system(_os)
            logger.debug("{} operating system selected.".format(self._id))
            time.sleep(delay)
            logger.debug("{} selecting region...".format(self._id))
            time.sleep(delay)
            self.select_region(region)
            logger.debug("{} region selected, scraping...".format(self._id))
            time.sleep(delay)
        except ScrprException:
            # if a region or an operating system can not be found, release the
            # lock to allow driver to be reused.
            logger.error("{} failed to select an operating system '{}' for region {}, this data will not be recorded if it exists!".format(self._id, _os, region), exc_info=True)
            return None
        try:
            self.driver.switch_to.frame(self.iframe)
            rtn: List[PGInstance] = []

            logger.debug("{} resetting view port position...".format(self._id))
            self.scroll(-10000)

            logger.debug("{} scrolling table into view...".format(self._id))
            table = self.driver.find_element(By.XPATH, '//awsui-table[@data-test="pricing_table"]')
            header = table.find_element(By.CLASS_NAME, 'awsui-table-sticky-active')  # the row you can click to sort the table
            self.scroll(header.location['y'])
            self.scroll(header.size['height'] * 4)
            time.sleep(delay)

            # On EC2 pricing page:
            # page numbers (1, 2, 3, (literal)..., last, >)
            # changes as buttons are pressed
            pages = self.driver.find_element(By.CLASS_NAME, 'awsui-table-pagination-content').find_elements(By.TAG_NAME, "li")
            pages[1].click()  # make sure we're starting on page 1
            arrow_button = pages[-1].find_element(By.TAG_NAME, 'button')  # >
            num_pages = pages[-2].find_element(By.TAG_NAME, 'button').text  # last page number


            #################
            # scrape
            #################
            logger.info("{} scraping {} pages for os: {} region {}...".format(self._id, num_pages, _os, region))
            for i in range(int(num_pages)):
                logger.debug(f"scraping page {i}")
                # only cycle to next page after it has been scraped
                if i > 0:
                    logger.debug("{} clicking 'next' button".format(self._id))
                    arrow_button.click()
                selection = self.driver.find_element(By.TAG_NAME, 'tbody')
                # rows in HTML table
                tr_tags = selection.find_elements(By.TAG_NAME, 'tr')
                logger.debug("{} num rows: {}".format(self._id, len(tr_tags)))
                for tr in tr_tags:
                    instance_props = []
                    # columns in row
                    for td in tr.find_elements(By.TAG_NAME, "td"):
                        # 0 t3a.xlarge # 1 $0.1699 # 2 4 # 3 16 GiB # 4 EBS Only # 5 Up to 5 Gigabit
                        instance_props.append(td.text)
                    rtn.append(PGInstance(self.human_date, region, _os, *instance_props))
                # @@
                time.sleep(0.1)
            logger.debug("{} resetting viewport before returning...".format(self._id))
            self.scroll(-10000)
            self.driver.switch_to.default_content()
            return rtn
            
        except Exception as e:
            logger.critical("worker {}: While scraping os '{}' for region '{}'. an exception occurred which requires closing the Selenium WebDriver: {}".format(self._id, _os, region, e), exc_info=True)
            self.driver.close()
            logger.debug("(from exception in worker {}) releasing lock...".format(self._id))
            try:
                self.lock.release()
            except RuntimeError:
                # if self.scrape() wasn't called in a thread there will be no lock to release
                pass
            return None

    def store_postgres(self, instances: List[PGInstance]) -> Tuple[int, int]:
        """
        returns (written_count, error_count)
        """
        error_count = 0
        already_existed_count = 0
        stored_count = 0
        with self.get_db() as conn:
            for instance in instances:
                try:
                    instance.store(conn)
                    stored_count += 1
                except UniqueViolation:
                    already_existed_count += 1
                    error_count += 1
                    conn.commit()
                except Exception as e:
                    logger.error("An unhandled exception occured while attempting to write data to database: {}".format(e))
                    error_count += 1
                    conn.commit()
        if already_existed_count > 0:
            logger.warning("{} records already existed in database and were not stored".format(already_existed_count))
        return (stored_count, error_count)

    def store_csv(self, instances: List[Instance]) -> Tuple[int, int]:
        """
        not sure I like the way this function works
        """
        stored_count = 0
        error_count = 0
        # all to avoid requiring 'region' as a parameter. let's see if this
        # pays of...
        _regions = []
        for i in instances:
            _regions.append(i.region)
        if len(_regions) > 1:
            logger.critical("BUG: more than 1 region in csv dataset: Marking as bad!")
            d = Path(f"{self.csv_data_dir}-bad/{self.human_date}/{_os}")
            d.mkdir(exist_ok=True, parents=True)
            f = d / f"{'_'.join(_regions)}.csv"
            error_count = len(instances)
        else:
            d = Path(f"{self.csv_data_dir}/{self.human_date}/{_os}")
            d.mkdir(exist_ok=True, parents=True)
            # @@ feels dirty...
            f = d / f"{_regions[0]}.csv"

        logger.info("{} storing data to csv file {}".format(self._id, f.relative_to(self.csv_data_dir)))

        if f.exists():
            # @@@ what do if compressed data exists.??
            logger.warning("Overwriting existing data at '{}'".format(f.absolute()))
            # I think the hassle of rotating the file around to ensure a
            # backup is produced is too much headache later on.
            f.unlink()
        try:
            with f.open('w') as cf:
                fieldnames = Instance.get_fields()
                writer = csv.DictWriter(cf, fieldnames=fieldnames)
                writer.writeheader()
                for inst in instances:
                    writer.writerow(inst.as_dict())
                    stored_count += 1
        except Exception as e:
            raise ScrprException("failed to save csv data fo file '{}': {}".format(f.absolute(), e))
        logger.debug("saved data to file: '{}'".format(f))
        return (stored_count, error_count)

    def scrape_and_store(self, _os: str, region: str) -> bool:
        """
        Select an operating system and region to fill the pricing page table with data, scrape it, and save it.
        CSV files are saved in a parent directory of self.csv_data_dir, by date then by operating system. e.g. '<self.csv_data_dir>/2023-01-18/Linux'
        This function is meant to be run in a ThreadDivvier singleton.
        NOTE: contains try/catch for self.driver
        """

        logger.debug(f"{self._id} scrape and store: {region=} {_os=}")
        ################# # @Scrape #################
        try:
            instances: List[PGInstance] = self.collect_ec2_data(_os=_os, region=region)
        except ScrprException:
            logger.error("failed to acquire data")
            self.lock.release()
            return False

        try:
            ################# # Postgres #################
            if self.db_config is not None:
                stored_count, error_count = self.store_postgres(instances)
                logger.info("{} stored {}/{} records with {} errors.".format(self._id, stored_count, len(instances), error_count))
            else:
                logger.info("{} skip saving to database".format(self._id))

            ################# # CSV #################
            if self.csv_data_dir is not None:
                try:
                    self.store_csv(instances)
                except ScrprException as e:
                    logger.error("failed to save to database")
            else:
                logger.info("{} skip saving to csv file".format(self._id))

            self.lock.release()
            return True

        except Exception as e:
            logger.critical("worker {}: While storing data for os '{}' for region '{}', an exception occurred which may result in dataloss: {}".format(self._id, _os, region, e), exc_info=True)
            self.lock.release()
            return False

    def get_result_count_test(self) -> int:
        """
        get the 'Viewing ### of ### Available Instances' count to check our results
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug("{} get results count".format(self._id))
        try:
            self.driver.switch_to.frame(self.iframe)
            t = self.driver.find_element(By.CLASS_NAME, 'awsui-table-header').find_element(By.TAG_NAME, "h2").text
            # ¯\_(ツ)_/¯
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error("{} error getting number of results: {}".format(self._id, e), exc_info=True)
            raise e
        return int(t.split("of")[1].split(" available")[0].strip())

    def wait_for_menus_to_close(self):
        """
        Blocks until any of the clickable dialog boxes in this script have returned to a collapsed state before returning.
        NOTE: contains try/catch for self.driver
        NOTE: might not do anything
        """
        try:
            logger.debug("{} ensuring menus are closed".format(self._id))
            # vCPU selection box
            self.waiter.until(ec.visibility_of_element_located((By.XPATH, "//awsui-select[@data-test='vCPU_single']")))
            # Instance Type selection box
            self.waiter.until(ec.visibility_of_element_located((By.XPATH, "//awsui-select[@data-test='plc:InstanceFamily_single']")))
            # Operating System selection box
            self.waiter.until(ec.visibility_of_element_located((By.XPATH, "//awsui-select[@data-test='plc:OperatingSystem_single']")))
            # instance search bar
            self.waiter.until(ec.visibility_of_element_located((By.ID, "awsui-input-1")))
            # instance search paginator
            self.waiter.until(ec.visibility_of_element_located((By.TAG_NAME, "awsui-table-pagination")))
        except Exception as e:
            self.driver.close()
            logger.error("{} Error waiting for menus to close: {}".format(self._id, e), exc_info=True)
            raise e
    
    @contextmanager
    def get_db(self):
        """
        Uses DataCollectorConfig.db (db_config) to yield a connection to a Postgres database
        """
        conn = psycopg2.connect(
            self.db_config.get_dsl()
        )
        yield conn
        # logger.debug("committing transactions")
        # conn.commit()
        logger.debug("{} closing database connection".format(self._id))
        conn.close()


def compress_data(data_dir: str, human_date: str):
    """Save data to a zip archive immediately under data_dir instead of saving directory tree"""
    errors = False
    csv_data_tree = Path(data_dir) / human_date  # e.g. csv-data/ec2/2023-01-29
    logger.debug("compressing data in '{}'".format(csv_data_tree.absolute()))
    old_cwd = os.getcwd()
    os.chdir(data_dir)
    bkup_zip_file = Path(f"{human_date}.bkup-{uuid1()}.zip")
    if Path(f"{human_date}.zip").exists():
        logger.warning("Removing existing zip file '{}.zip'".format(human_date))

        Path(f"{human_date}.zip").rename(bkup_zip_file.name)
    with zipfile.ZipFile(f"{human_date}.zip", 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for os_name_dir in Path(human_date).iterdir():
            if os_name_dir.is_dir():
                _csv_ct = 0
                for csv_file in os_name_dir.iterdir():
                    _csv_ct += 1
                    try:
                        logger.debug("adding {}/{}/{}".format(human_date, os_name_dir.name, csv_file.name))
                        zf.write(f"{human_date}/{os_name_dir.name}/{csv_file.name}")
                    except Exception as e:
                        errors = True
                        logger.error("Error writing compressed data: {}".format(e), exc_info=True)
            logger.debug("Compressed {} files of {} os data".format(_csv_ct, os_name_dir.name))
    if not errors:
        if bkup_zip_file.exists():
            logger.debug("removing temporary backup '{}'".format(bkup_zip_file.absolute()))
            bkup_zip_file.unlink()
        logger.debug("Removing uncompressed data in '{}'".format(csv_data_tree.absolute()))
        shutil.rmtree(csv_data_tree)
    os.chdir(old_cwd)
    return

def do_args():
    default_data_dir = os.path.join(os.path.expanduser('~') , '.local' , 'share' , 'scrpr' , 'csv-data', 'ec2')
    default_log_dir = os.path.join(os.path.expanduser('~') , '.local' , 'share' , 'scrpr' , 'logs')
    default_args = {  # fyi
        "follow": False,
        "log_file": "/home/scrpr/.local/share/scrpr/logs/scrpr.log",
        "thread_count": 24,
        "overdrive_madness": False,
        "no_compress": False,
        "regions": None,
        "operating_systems": None,
        "get_operating_systems": False,
        "get_regions": False,
        "data_dir": "/home/scrpr/.local/share/scrpr/csv-data/ec2",
        "metric_data_file": "/home/scrpr/.local/share/scrpr/logs/metric-data.txt",
        "store_csv": True,
        "store_db": True,
        "v": 0
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("--follow",
        required=False,
        action='store_true',
        help="print logs to stdout in addition to the log file")
    parser.add_argument("--log-file",
        required=False,
        default=os.path.join(default_log_dir, 'scrpr.log'),
        help="log output destination file. '--log-file=console' will instead print to your system's stderr stream.")
    parser.add_argument("-t", "--thread-count",
        default=cpu_count(),
        action='store',
        type=int,
        help="number of threads (Selenium drivers) to use")
    parser.add_argument("--overdrive-madness",
        action='store_true',
        help=f"allow going over the recommended number of threads (number of threads for your machine, {cpu_count()})")
    parser.add_argument("--no-compress",
        required=False,
        action='store_true',
        help="script outputs a directory tree of csv data instead of the <os_name>_<date>.zip archive")
    parser.add_argument("--regions",
        type=str,
        required=False,
        default=None,
        help="comma-separated list of regions to scrape (omit=scrape all)",
        metavar='region-1,region-2,...')
    parser.add_argument("--operating-systems",
        type=str,
        required=False,
        default=None,
        help="comma-separated list of oprating systems to scrape (omit=scrape all)",
        metavar="Linux,Windows,'Windows with SQL Web'")
    parser.add_argument("--get-operating-systems",
        action='store_true',
        required=False,
        help="print a list of the available operating systems and exit")
    parser.add_argument("--get-regions",
        action='store_true',
        required=False,
        help="print a list of the available regions and exit")
    parser.add_argument("-d", "--csv-data-dir",
        required=False,
        default=default_data_dir,
        help="base directory for output directory tree or zip file")
    parser.add_argument("--metric-data-file",
        required=False,
        default=os.path.join(default_log_dir, 'metric-data.txt'),
        help="base directory for output directory tree")
    parser.add_argument("--store-csv",
        required=False,
        action=BooleanOptionalAction,
        default=True,
        help="toggle saving data to csv files")
    parser.add_argument("--store-db",
        type=bool,
        required=False,
        action=BooleanOptionalAction,
        default=True,
        help="toggle saving data to a database")
    parser.add_argument("-v",
        required=False,
        action='count',
        default=0,
        help='increase logging output')

    return parser.parse_args()


def get_table_size(db_config: DatabaseConfig, table='ec2_instance_pricing'):
    # SELECT pg_size_pretty(pg_total_relation_size('public.ec2_instance_pricing'));  # kB, mB
    s = sql.SQL("SELECT pg_total_relation_size('public.{}')".format(table))
    conn = psycopg2.connect(db_config.get_dsl())
    curr = conn.cursor()
    curr.execute(s)
    r = curr.fetchone()[0]
    conn.close()
    logger.debug("'{}' table is {:.2f} Mb ({} bytes)".format(
        table, r / 1024 / 1024, r 
    ))
    return r


if __name__ == '__main__':
    t_main = time.time()
    URL = "https://aws.amazon.com/ec2/pricing/on-demand/"
    human_date: str = datetime.fromtimestamp(floor(time.time())).strftime("%Y-%m-%d")

    args = do_args()

    ##########################################################################
    # logging
    ##########################################################################
    logger = logging.getLogger()
    logging.getLogger('selenium.*').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    if args.v == 0:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    else:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(threadName)s (%(thread)s) : %(funcName)s : %(message)s')
    if args.follow:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    fh = logging.FileHandler(str(args.log_file))
    fh = RotatingFileHandler(str(args.log_file),
        maxBytes=5_000_000,  # 5MB
        backupCount=5
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.warning("This program is still under development, log output may be ... less than scrupulous.")
    logger.info("Starting program with PID {}".format(os.getpid()))

    if not Path(args.csv_data_dir).exists():
        logger.error(f"no such directory '{args.csv_data_dir}'")
        exit(1)

    if args.store_db:
        db_config = DatabaseConfig()
        db_config.load()
        conn = psycopg2.connect(db_config.get_dsl())
        conn.close()
        logger.debug("db connection ok")
    else:
        db_config = None

    if args.store_csv:
        csv_config = args.csv_data_dir
    else:
        csv_config = None

    config = DataCollectorConfig(
        human_date=human_date,
        csv_data_dir=csv_config,
        db_config=db_config,
    )

    for arg, val in vars(args).items():
        logger.debug("{}={}".format(arg, val))
    for k, v in config.__dict__.items():
        logger.debug("{}={}".format(k, v))
    logger.debug("{}".format(str(db_config)))

    s_start = get_table_size(db_config)

    ##########################################################################
    # regions and operating systems
    ##########################################################################
    # get list of operating_system-region pair tuples
    os_region_collector = EC2DataCollector('os_region_collector', config=config)
    tgt_oses, tgt_regions = os_region_collector.get_available_regions_and_os()
    os_region_collector.driver.close()
    logger.debug("tgt_oses    = {}".format(tgt_oses))
    logger.debug("tgt_regions = {}".format(tgt_regions))

    if args.get_operating_systems:
        print("Available Operating Systems:")
        for _os in tgt_oses: print(f"\t'{_os}'")
    if args.get_regions:
        print("Available Regions:")
        for r in tgt_regions: print(f"\t{r}")
    if args.get_operating_systems or args.get_regions:
        exit(0)

    ##########################################################################
    # main
    ##########################################################################
    if args.regions is not None:
        logger.debug("Validating provided regions...")
        for r in args.regions.split(','):
            if r == '':
                continue
            if r not in tgt_regions:
                [print(f"{n}:\t{R}") for n, R in enumerate(tgt_regions)]
                print(f"supplied region '{r}' not in list")
                exit(1)
        tgt_regions = args.regions.split(',')

    if args.operating_systems is not None:
        logger.debug("Validating provided operating systems...")
        for o in args.operating_systems.split(','):
            if o == '':
                continue
            if o not in tgt_oses:
                [print(f"{n}:\t{O}") for n, O in enumerate(tgt_oses)]
                print(f"supplied operating system '{o}' not in list")
                exit(1)
        tgt_oses = args.operating_systems.split(',')

    num_threads = cpu_count()
    if args.thread_count < 1:
        num_threads = 1
    elif args.thread_count > cpu_count() and not args.overdrive_madness:
        logger.warning("Using {} threads instead of requested {}".format(cpu_count(), args.thread_count))
        num_threads = cpu_count()
    else:
        num_threads = args.thread_count

    thread_tgts = []
    for o in tgt_oses:
        for r in tgt_regions:
            thread_tgts.append((o, r))
    logger.debug("thread targets ({}) = {}".format(len(thread_tgts), thread_tgts))
    thread_thing = ThreadDivvier(config=config, thread_count=num_threads)
    t_prog_init = time.time() - t_main
    logger.info("Initialized in {}".format(seconds_to_timer(time.time() - t_main)))
    # blocks until all threads have finished running, and thread_tgts is exhausted
    thread_thing.run_threads(thread_tgts)

    ##########################################################################
    # after data has been collected
    ##########################################################################
    # @@@
    if (not args.no_compress) and (args.store_csv):
        compress_data(data_dir=args.csv_data_dir, human_date=human_date)
        s_csv = Path(f"{args.csv_data_dir}/{human_date}.zip").stat().st_size
    else:
        if Path(f"{args.csv_data_dir}/{human_date}").exists():
            s_csv = 0
            for _os in Path(f"{args.csv_data_dir}/{human_date}").iterdir():
                if not _os.is_dir():
                    continue
                for csvf in _os.iterdir():
                    s_csv += csvf.stat().st_size
    if args.store_db:
        s_db = get_table_size(db_config) - s_start

    t_prog_tot = time.time() - t_main
    metric_data = {
        "date": human_date,
        "threads": num_threads,
        "oses": len(tgt_oses),
        "regions": len(tgt_regions),
        "t_init": f"{t_prog_init:.2f}",
        "t_run": f"{t_prog_tot:.2f}",
        "s_csv_run": f"{s_csv / 1024 / 1024:.3f}",
        "s_db_run": f"{s_db / 1024 / 1024:.3f}",
        "reported_errors": len(ERRORS),
    }
    logger.debug("run metrics:")
    for k, v in metric_data.items():
        logger.debug(f"{k}: {v}")
    logging.debug("Saving run's metric data to '{}'".format(args.metric_data_file))
    with open(args.metric_data_file, 'a') as md:
        writer = csv.DictWriter(md, fieldnames=list(metric_data.keys()), delimiter='\t').writerow(metric_data)
        # md.write(f"{num_threads}\t{len(tgt_oses)}\t{len(tgt_regions)}\t{t_prog_init:.2f}\t{t_prog_tot:.2f}\t{(t_size / 1024 / 1024):.3f}M\n")
    logger.info("Program finished in {}".format(seconds_to_timer(time.time() - t_main)))
    logger.info('---------------------------------------------------')
    logger.info("errors ({}):".format(len(ERRORS)))
    for e in ERRORS:
        logger.error(e)
    logger.info('---------------------------------------------------')

    ##########################################################################
    # +3 SLOC
    ##########################################################################
