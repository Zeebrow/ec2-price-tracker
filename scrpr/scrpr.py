import time
import csv
import datetime
from pathlib import Path
from math import floor
from typing import List, Tuple, Dict, Any, Optional
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.remote.webdriver import WebDriver
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
from collections import OrderedDict
import psutil
import json
import re
import warnings

from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait  # pyright:ignore
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common import action_chains
from selenium import webdriver
from selenium.webdriver.remote.command import Command
import psycopg2
from psycopg2.errors import UniqueViolation
from psycopg2 import sql

from .instance import Instance, PGInstance
from .api.sql_app import crud, database

logger = logging.getLogger(__name__)


"""
Scrapes pricing information for EC2 instance types for available regions.
"""
ERRORS = []
SCRPR_HOME = os.path.join(os.path.expanduser('~'), '.local', 'share', 'scrpr')
# this is overridable with the flag --csv-data-dir
DEFAULT_CSV_DATA_DIR = os.path.join(SCRPR_HOME, "csv-data")
DEFAULT_METRICS_DATA_FILE = os.path.join(SCRPR_HOME, "metric-data.txt")
DEFAULT_LOG_FILE = os.path.join(SCRPR_HOME, "logs", "scrpr.log")
_THREAD_RUN_TIMES = []
ROWS_COLLECTED = 0
ROWS_STORED = 0

####################### memory stuf
TOTAL_MEM_SYS = psutil.virtual_memory().total
FREE_MEM_SYS = psutil.virtual_memory().free
MAIN_PROCESS = psutil.Process()
INITIAL_MEM = MAIN_PROCESS.memory_info()


#######################

###############################################################################
# classes and functions
###############################################################################


def set_api_status(status: str):
    try:
        fastapi_db = database.SessionLocal()
        logger.debug(f"state transition: {crud.get_system_status(fastapi_db)} -> {status}")
        crud.set_system_status(status, fastapi_db)
    except Exception:
        logger.warning("unable to set api status (desired status is '{}')".format(status))


class ScrprException(Exception):
    """Not sure if my global ERRORS contraption is a great idea..."""

    global ERRORS

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        ERRORS.append(*args)


class ScrprCritical(Exception):
    """
    Use to indicate that a DataCollector's WebDriver needs to be closed or
    restarted in order to continue.
    """

    global ERRORS

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        ERRORS.append(*args)


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
            self.port = int(config.get("db_port", 5432))  # pyright:ignore
            self.dbname = config.get("db_dbname", "scrpr")
            self.user = config.get("db_user", "scrpr")
            self.password = config.get("db_password", None)
            return True
        else:
            raise FileNotFoundError("No configuration found at '{}'".format(env_file))

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
    headless: bool = True
    window_w: int = 1920
    window_h: int = 1080


@dataclass
class EC2DataCollectorConfig(DataCollectorConfig):
    url: str = 'https://aws.amazon.com/ec2/pricing/on-demand/'
    db_config: Optional[DatabaseConfig] = None  # @@@ what happens with this is None?
    csv_data_dir: Optional[str] = None


seconds_to_timer = lambda x: f"{floor(x/60)}m:{x%60:.1000f}s ({x} seconds)"  # noqa: E731


class ThreadDivvier:
    """
    Converts machine time processing data into developer time debugging exceptions.
    """
    def __init__(self, thread_count) -> None:
        """
        Initialize one DataCollector for use in a thread, creating thread_count DataCollector instances with identical configuration.
        Each thread manages its own Selenium.WebDriver.
        """
        logger.debug("init ThreadDivvier with {} threads".format(thread_count))
        self.thread_count = thread_count
        self.drivers: List[DataCollector] = []

    def init_scraper(self, thread_id: int, config: DataCollectorConfig) -> None:
        try:
            # this feels like a common problem that I don't know how to search for.
            # pyright and I do not like assigning an instance of type
            # DataCollectorConfig which is only supposed to accept
            # EC2DataCollectorConfig, a subclass.
            d = EC2DataCollector(_id=thread_id, config=config)

            # after = psutil.Process().memory_info().rss
            # logger.critical(f"\033[44m{thread_id} {after=}\033[0m")
            # print("change = {}".format(after.rss - before.rss))

            self.drivers.append(d)
            logger.debug(f"initialized {thread_id=}")
        except ScrprCritical as e:
            logger.critical(e, exc_info=True)

    def init_scrapers_of(self, config: DataCollectorConfig) -> None:
        """
        Initialize scrapers one at a time.
        As scrapers become initialized, give them to the ThreadDivvier's drivers pool.
        """
        next_id = 0  # name shown in log messages
        # start init for each driver
        logger.debug("initializing {} drivers".format(self.thread_count))
        threads = []
        # logger.critical(f"\033[44mBefore threads init: {psutil.Process().memory_full_info()=}\033[0m")
        for _ in range(self.thread_count):
            logger.debug("initializing new driver with id '{}'".format(next_id))
            # before = psutil.Process().memory_info().rss
            # logger.critical(f"\033[44mthread {next_id} {before=}\033[0m")
            t = threading.Thread(
                name=str(next_id),
                target=self.init_scraper,
                args=(next_id, config),
                daemon=False,
            )
            next_id += 1

            t.start()
            # always the same as the one below
            # logger.critical(f"\033[44m{psutil.Process(t.native_id).memory_full_info().rss=}\033[0m")
            threads.append(t)
            # time.sleep(0.1)
            # logger.critical(f"\033[44m{psutil.Process(t.native_id).memory_full_info().rss=}\033[0m")

        # wait until each driver's init is finished
        # drivers are only added to self.init_drivers *after* they are done being initialized
        # i.e. when __init__ has finished
        while len(self.drivers) != self.thread_count:
            pass
        # logger.critical(f"\033[44mAfter threads init: {psutil.Process().memory_full_info()=}\033[0m")
        for t in threads:
            t.join()
        logger.debug("Finished initializing {} scrapers".format(len(self.drivers)))

        return

    # @memory_profiler.profile
    def run_threads(self, arg_queue: List[tuple]):
        """
        Takes a list of tuples (operating_system, region) and gathers corresponding EC2 instance pricing data.
        Items in the arg_queue are removed as work is completed, until there are no items left in the queue.
        """
        logger.debug(f"running {self.thread_count} threads")
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
                    logger.debug(f"{len(_arg_queue)} left in queue")
                    # t = threading.Thread(name='-'.join(args), target=d.scrape_and_store, args=args, daemon=False)
                    t = threading.Thread(target=d.scrape_and_store, args=args, daemon=False)
                    t.start()
                    logger.debug(f"worker id {d._id} running new thread {'-'.join(args)}")
        # determine when all threads have finished
        # wait to return until all threads have finished their work
        threads_finished = []
        logger.debug("waiting for last threads to finish juuuust a sec")
        while len(threads_finished) != self.thread_count:
            # @@ bug when number of threads is greater than the arg queue
            for d in self.drivers:
                # @@ if an exception is thrown in a thread it may not properly release it's lock.
                # This will cause the script to hang after it is done running, at least.
                if not d.lock.locked() and d._id not in threads_finished:
                    threads_finished.append(d._id)
                    d.driver.quit()
        return


class DataCollector:
    """
    Base class for selenium drivers to interface with a ThreadDivvier.
    """
    data_type_scraped = None

    def __init__(self, _id, config: DataCollectorConfig, _test_driver=None):
        logger.debug("init DataCollector")
        self.lock = threading.Lock()
        self.lock.acquire()
        self._id = _id
        ###################################################
        # config
        ###################################################
        self.human_date = config.human_date

        if _test_driver is None:
            self.get_driver(
                headless=config.headless,
                window_w=config.window_w,
                window_h=config.window_h
            )
        else:
            self.driver = _test_driver
        logger.debug("Finished init of thread {} without errors".format(self._id))
        self.lock.release()

    def get_driver(self, browser_binary='/usr/bin/google-chrome', automation_driver='/usr/local/bin/chromedriver', headless=True, window_w=1920, window_h=1080):
        options = webdriver.ChromeOptions()
        options.binary_location = browser_binary
        options.add_argument("window-size={},{}".format(window_w, window_h))
        if headless:
            options.add_argument('-headless')
        driverService = Service(automation_driver)
        self.driver = webdriver.Chrome(service=driverService, options=options)

    def scroll(self, amt):
        """scroll down amt in pixels"""
        action = action_chains.ActionChains(self.driver)
        action.scroll_by_amount(0, amt)
        action.perform()
        time.sleep(1)


class EC2TableBase:
    def __init__(self, driver, iframe) -> None:
        self.driver = driver
        self.iframe = iframe
        self.data_selection_root = self._get_data_selection_root()

    def _get_data_selection_root(self):
        """ Each element we care about scraping has a data-selection-root attribute.  """
        self.driver.switch_to.frame(self.iframe)
        rtn = self.driver.find_element(By.XPATH, "//*[@data-selection-root]")
        self.driver.switch_to.default_content()
        return rtn


class EC2Table(EC2TableBase):

    class Header(EC2TableBase):
        """ Represents the clickable header of the EC2 pricing table.  """
        def __init__(self, driver, iframe) -> None:
            super().__init__(driver, iframe)
            self.element = self.get_element()

        def get_element(self):
            """requires switching to iframe"""
            self.driver.switch_to.frame(self.iframe)
            # @@ can selenium determine if the iframe is selected?
            # print(f"{self.driver.current_window_handle=}")
            rtn = self.data_selection_root.find_element(By.XPATH, './/table/thead/tr')
            self.driver.switch_to.default_content()
            # print(f"{self.driver.current_window_handle=}")
            return rtn

    class Rows(EC2TableBase):
        """
        Reporesents where all the juicy data lives. The big kahuna, if you will.
        """
        def __init__(self, driver, iframe) -> None:
            super().__init__(driver, iframe)

        def get_rows(self):
            """requires switching to iframe"""
            return [tr for tr in self.data_selection_root.find_elements(By.XPATH, './/table/tbody/tr')]

        def get_total_row_count(self):
            """requires switching to iframe"""
            row_count_re = re.compile(r'([0-9]{1,3}) available instances$')
            return [int(b.groups()[0]) if (b := row_count_re.search(self.data_selection_root.find_element(By.XPATH, './/h2/span').text)) is not None else -1][0]

        def get_rows_count_on_page(self):
            """requires switching to iframe"""
            return len([tr for tr in self.data_selection_root.find_elements(By.XPATH, './/table/tbody/tr')])

    def __init__(self, driver, iframe):
        super().__init__(driver, iframe)
        self.header = self.Header(driver, iframe)
        self.rows = self.Rows(driver, iframe)

    def get_current_page(self) -> int:
        """ requires switching to iframe """
        return int(self.data_selection_root.find_element(By.XPATH, ".//li/button[@aria-current='true']").text)

    def get_total_pages(self) -> int:
        """ requires switching to iframe """
        return int(self.data_selection_root.find_element(By.XPATH, './/ul/li[last()-1]').text)

    def navto_first_page(self) -> None:
        """ requires switching to iframe """
        self.data_selection_root.find_element(By.XPATH, './/ul/li[2]').click()

    def navto_next_page(self) -> None:
        """ requires switching to iframe """
        self.data_selection_root.find_element(By.XPATH, './/ul/li[last()]').click()

class EC2Dropdown:
    def __init__(self, driver: WebDriver, iframe: WebElement):
        self.driver: WebDriver = driver
        self.iframe: WebElement = iframe
        self.analytics_element: Optional[WebElement] = None
        self.button: Optional[WebElement] = None
        self.options: List[str] = []

    def current_value(self):
        return self.button.text

    def select(self, selection: str) -> None:
        logger.debug("select {} '{}'".format(self.__class__.__name__, selection))
        try:
            self.driver.switch_to.frame(self.iframe)
            self.button.click()
            lis = self.analytics_element.find_elements(By.XPATH, './/ul[@role="listbox"]/li')
            for li in lis:
                if selection in li.text:
                    li.click()
                    break
            self.driver.switch_to.default_content()
            return

        except Exception as e:  # pragma: no cover
            self.driver.quit()
            logger.error(e)
            # @@ do we need to raise?
            raise

class EC2LocationType(EC2Dropdown):
    def __init__(self, driver: WebDriver, iframe: WebElement):
        super().__init__(driver, iframe)
        raise NotImplementedError

class EC2Region(EC2Dropdown):
    def __init__(self, driver: WebDriver, iframe: WebElement):
        super().__init__(driver, iframe)

class EC2OperatingSystem(EC2Dropdown):
    def __init__(self, driver: WebDriver, iframe: WebElement):
        super().__init__(driver, iframe)

class EC2InstanceType(EC2Dropdown):
    def __init__(self, driver: WebDriver, iframe: WebElement):
        super().__init__(driver, iframe)

class EC2CpuCount(EC2Dropdown):
    def __init__(self, driver: WebDriver, iframe: WebElement):
        super().__init__(driver, iframe)
        raise NotImplementedError


class EC2DataCollector(DataCollector):
    """ Instantiates a Selenium WebDriver for Chrome.  """
    data_type_scraped = 'ec2'
    url = 'https://aws.amazon.com/ec2/pricing/on-demand/'

    def __init__(self, _id, config: EC2DataCollectorConfig, _test_driver=None):
        super().__init__(_id, config, _test_driver)
        self.lock.acquire()
        self.csv_data_dir = config.csv_data_dir
        self.db_config = config.db_config
        self.url = config.url
        if _test_driver is None:
            self.prep_driver()
        self._in_iframe = False
        self.region_dropdown = None
        self.instance_type_dropdown = None
        self.operating_system_dropdown = None
        self.location_type_dropdown = None
        self.cpu_dropdown = None
        # @@ this is slow and gets called twice
        # once when building the list of arguments for threads,
        # and again when initializing each driver in the threads.
        self.get_dropdown_menus()
        self.table = EC2Table(self.driver, self.iframe)
        self.lock.release()

    def prep_driver(self):
        delay = 0.5
        try:
            self.waiter = WebDriverWait(self.driver, 10)
            self.driver.get(self.url)
            logger.debug("Initializing worker with id {}".format(self._id))
            logger.debug(f"{self._id} begin nav_to")
            time.sleep(delay)
            logger.debug("wait untill iFrame located...")
            self.waiter.until(ec.visibility_of_element_located((By.ID, "iFrameResizer0")))
            self.iframe = self.driver.find_element('id', "iFrameResizer0")
            logger.debug("done. waiting 3 seconds before releasing lock...")
            time.sleep(delay)
        except Exception as e:  # pragma: no cover
            logger.critical("While initializing worker '{}', an exception occurred which requires closing the Selenium WebDriver: {}".format(self._id, e), exc_info=True)
            self.driver.quit()
            raise ScrprCritical("While initializing worker '{}', an exception occurred which requires closing the Selenium WebDriver: {}")

    def _get_data_analytics_divs(self) -> List[WebElement]:
        # voodoo?
        self.waiter.until(ec.visibility_of_element_located((By.XPATH, "//*[@data-analytics-field-label]")))
        data_analytics_divs = self.driver.find_elements(By.XPATH, "//*[@data-analytics-field-label]")
        logger.debug("{} data analytics elements found.".format(len(data_analytics_divs)))
        if len(data_analytics_divs) == 0:
            logger.error("could not find data analytics div! No data could be extracted.")
            raise SystemExit(1)
        return data_analytics_divs

    def get_dropdown_menus(self) -> None:
        """
        Create self.Region, self.InstanceType, and self.OperatingSystem classes
        from the WebElements available on the EC2 pricing page.

        we do this here instead of in the classes
        themselves to avoid having to
        iterate over the DOM's data analytics divs for each class.

        switches to iframe
        """
        self.driver.switch_to.frame(self.iframe)

        # sift through and find the key terms we can filter on
        data_analytics_divs = self._get_data_analytics_divs()

        for dad in data_analytics_divs:
            # find all clickable drop-down menus by using very, very conveniently attributed divs
            # amazon watching us watch them...
            query_category = dad.get_attribute('data-analytics-field-label')

            # parse the xpath expression contained within
            if (xpath_query := re.search(r'(?P<tag_name>[a-z]*)="(?P<label_for>.*)(?P<_label>-label)"', query_category)) is None:
                logger.error("Could not derive xpath query from data analyitics field label '{}'".format(query_category))
                # TODO: something more useful?
                raise SystemExit(1)
            else:
                if xpath_query.groupdict()['tag_name'] != 'id':
                    logger.warning("Possibly ambiguous identifier used to locate dropdown menus ({})".format(xpath_query.groupdict()['tag_name']))

            # construct required xpath queries from data analytics div data
            # Note Location Type must always be 'Region' and is ignored
            #
            # There will always be Operating System, Instance Type, and vCPU once
            # a Location Type is specified.
            category_xpath_query = "//label[@{}='{}{}']".format(
                xpath_query.groupdict()['tag_name'],
                xpath_query.groupdict()['label_for'],
                xpath_query.groupdict()['_label'],
            )
            button_xpath_query = "//button[@{}='{}']".format(
                xpath_query.groupdict()['tag_name'],
                xpath_query.groupdict()['label_for'],
            )

            # extract the category name to determine what kind of dropdown
            category_text_elem = self.driver.find_element(By.XPATH, category_xpath_query)
            button_click_elem = self.driver.find_element(By.XPATH, button_xpath_query)

            # click the button to show menu
            button_click_elem.click()
            lis = dad.find_elements(By.XPATH, './/ul[@role="listbox"]/li')

            # extract the options
            # we do it here so that we only have to iterate over all of these
            # elements once, instead of for each filter instanciated.
            options = []
            options_list = {}
            for li in lis:
                if (option := li.text) is not None:
                    if category_text_elem.text == 'Region':
                        if '\n' in option:
                            t = option.split('\n')[-1]
                            if re.search(r'^[a-z]{2}-(gov-)?[a-z]*-[1-9]$', t):
                                options.append(t)
                    else:
                        # @@@ test
                        options_list[option] = li
                        options.append(option)

            # after options have been collected, assign them to their rightful dropdown
            # I sense a software design pattern going *woosh*
            match category_text_elem.text:
                case 'Region':
                    self.region_dropdown = EC2Region(self.driver, self.iframe)
                    self.region_dropdown.button = button_click_elem
                    self.region_dropdown.analytics_element = dad
                    self.region_dropdown.options = options
                case 'Operating system':
                    self.operating_system_dropdown = EC2OperatingSystem(self.driver, self.iframe)
                    self.operating_system_dropdown.button = button_click_elem
                    self.operating_system_dropdown.analytics_element = dad
                    self.operating_system_dropdown.options = options
                case 'Instance type':
                    self.instance_type_dropdown = EC2InstanceType(self.driver, self.iframe)
                    self.instance_type_dropdown.button = button_click_elem
                    self.instance_type_dropdown.analytics_element = dad
                    self.instance_type_dropdown.options = options
                case 'vCPU':  # not something we filter based on
                    pass
                case 'Location Type':  # always "AWS Region"
                    pass
                case _:
                    logger.debug(f"unsupported filter category: {category_text_elem.text}")

            # once finished, click button again to hide menu
            button_click_elem.click()
            logger.info("please clap")
        self.driver.switch_to.default_content()
        return None

    def get_available_operating_systems(self) -> List[str]:
        """
        Get all operating systems for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        warnings.warn(
            "get_available_operating_systems is deprecated: use self.operating_system_dropdown.options instead.",
            DeprecationWarning,
            stacklevel=1
        )
        logger.debug("{} get available os".format(self._id))
        # return self.dropdowns['Operating system']['options']
        return self.operating_system_dropdown.options

    def select_operating_system(self, _os: str) -> str:
        """
        Select an operating system for filtering data
        Returns the operating system name if successful, tries to return None otherwise.
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        warnings.warn(
            "select_operating_system is deprecated: use operating_system_dropdown.select('os') instead.",
            DeprecationWarning,
            stacklevel=1
        )
        self.operating_system_dropdown.select(_os)
        return _os

    def get_available_regions(self) -> List[str]:
        """
        Get all aws regions for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        warnings.warn(
            "get_available_regions is deprecated: use self.region_dropdown.options instead.",
            DeprecationWarning,
            stacklevel=1
        )
        logger.debug("{} get available regions".format(self._id))
        return self.region_dropdown.options

    def select_region(self, region: str):
        """
        Set the table of data to display for a given aws region
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        warnings.warn(
            "get_available_regions is deprecated: use self.region_dropdown.options instead.",
            DeprecationWarning,
            stacklevel=1
        )
        self.region_dropdown.select(region)
        return region

    def collect_ec2_data(self, _os: str, region: str) -> List[Instance]:
        """
        Select an operating system and region to fill the pricing page table with data, scrape it, and save it to a csv file.
        CSV files are saved in a parent directory of self.csv_data_dir, by date then by operating system. e.g. '<self.csv_data_dir>/2023-01-18/Linux'
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe

        NOTE: Fails for small browser sizes

        _os: Operating System name offered by AWS
        region: AWS region name

        raises: ScrprCritical
        """
        global ROWS_COLLECTED
        delay = 0.5
        logger.debug(f"{self._id} scrape all: {region=} {_os=}")
        time.sleep(delay)

        ########################
        # set table filters appropriately
        ########################
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
        except ScrprException:  # pragma: no cover
            logger.error("{} failed to select an operating system '{}' for region {}, this data will not be recorded if it exists!".format(self._id, _os, region), exc_info=True)
            return []

        try:
            rtn: List[PGInstance] = []

            logger.debug("{} resetting view port position...".format(self._id))
            self.scroll(-10000)

            self.driver.switch_to.frame(self.iframe)

            # voodoo
            logger.debug("{} scrolling table into view...".format(self._id))
            self.scroll(self.table.header.element.location['y'])
            self.scroll(self.table.header.element.size['height'] * 4)
            time.sleep(delay)

            num_pages = self.table.get_total_pages()
            validation_rows_scraped_per_page = {}
            logger.debug(f"before nav to first page: {self.table.get_current_page()}")
            # for some reason, pages like to start at 4 instead of 1, so we call this to make sure
            self.table.navto_first_page()
            logger.debug(f"after nav to first page: {self.table.get_current_page()}")

            #################
            # scrape
            #################

            logger.info("{} scraping {} rows on {} pages for os: {} region {}...".format(self._id, self.table.rows.get_total_row_count(), num_pages, _os, region))
            row_ct = 0
            logger.debug(f"total row count: {self.table.rows.get_total_row_count()}")

            for i in range(num_pages):
                # logger.debug(f"scraping page {self.table['get_current_page']()}")  # trace
                # navto next page only after the initial page is marshalled
                if i > 0:
                    # logger.debug("{} clicking 'next' button".format(self._id))  # trace
                    self.table.navto_next_page()

                validation_current_page = self.table.get_current_page()
                logger.debug(f"current page: {validation_current_page}")
                logger.debug(f"rows on page: {self.table.rows.get_rows_count_on_page()}")
                validation_rows_scraped_per_page[validation_current_page] = 0
                # logger.debug("current page: {}".format(validation_current_page))  # trace

                # exctract data from rows displayed
                for row in self.table.rows.get_rows():
                    row_ct += 1
                    validation_rows_scraped_per_page[validation_current_page] += 1
                    instance_props = []
                    for col in row.find_elements(By.XPATH, './/td'):
                        instance_props.append(col.text)
                    rtn.append(PGInstance(self.human_date, region, _os, *instance_props))  # i hate my code
                time.sleep(0.1)

            #################
            # validation
            #################
            actual_rows_count = sum([int(v) for _, v in validation_rows_scraped_per_page.items()])
            logger.debug(f"row count from summing page items: {actual_rows_count=}")
            logger.debug(f"row count from iterating: {row_ct=}")
            # requires risking introducing more errors, nah
            # actual_pages_count = max(validation_rows_scraped_per_page.keys())
            if actual_rows_count != len(rtn):
                logger.error("scraped {} rows, but returning {}!".format(
                    actual_rows_count, len(rtn)
                ))
            logger.debug("{} resetting viewport before returning...".format(self._id))
            self.scroll(-10000)
            self.driver.switch_to.default_content()
            ROWS_COLLECTED += len(rtn)
            return rtn

        except Exception as e:  # pragma: no cover
            logger.critical("worker {}: While scraping os '{}' for region '{}'. an exception occurred which requires closing the Selenium WebDriver: {}".format(self._id, _os, region, e), exc_info=True)
            # raise ScrprCritical("worker {}: While scraping os '{}' for region '{}'. an exception occurred which requires closing the Selenium WebDriver: {}".format(self._id, _os, region, e))

    def store_postgres(self, instances: List[PGInstance], table='ec2_instance_pricing') -> Tuple[int, int]:
        """
        Takes a list of PGInstances and commits them to the target table, using
        the EC2DataCollector instance's configured database parameters.
        returns (written_count, error_count)
        """
        error_count = 0
        already_existed_count = 0
        stored_count = 0
        with self.get_db() as conn:
            for instance in instances:
                try:
                    stored_new_data = instance.store(conn, table=table)
                    if stored_new_data:
                        stored_count += 1
                    else:
                        already_existed_count += 1
                        error_count += 1
                except Exception as e:  # pragma: no cover
                    logger.error("An unhandled exception occured while attempting to write data to database: {}".format(e))
                    error_count += 1
                    conn.commit()
        # squash a bunch of warnings into one
        if already_existed_count:
            logger.warning("{} records already existed in database and were not stored".format(already_existed_count))
        return (stored_count, error_count)

    def save_csv(self, region, _os, instances: List[Instance]) -> Tuple[int, int]:
        """
        Save a single csv file named 'region.csv' under the directory

        `<csv_data_dir>/ec2/<date>/<operating_system>/<region>.csv`

        for a directory tree like:
        ```text
        ~/.local/share/scrpr/csv-data/ec2/
        ├── 2023-02-16
        │   ├── Linux
        │   │   ├── ap-southeast-4.csv
        │   │   ├── eu-west-2.csv
        │   │   :
        │   │   └── us-east-1.csv
        │   ├── Windows
        │   ...
        │
        └── 2023-02-17
            ├── Linux
            ├── Windows
            ...
        ```

        ---

        ### output format

        | date | instance_type | operating_system | region | cost_per_hr | cpu_ct | ram_size_gb | storage_type | network_throughput |
        | :---: | :----------: | :--------------: | :----: | :---------: | :----: | :---------: | :----------: | :----------------: |
        | 2023-02-17 | t4g.nano | Linux | ca-central-1 | $0.0046 | 2 | 0.5 GiB | EBS Only | Up to 5 Gigabit |
        | 2023-02-17 | t4g.micro | Linux | ca-central-1 | $0.0092 | 2 | 1 GiB | EBS Only | Up to 5 Gigabit |

        * `end of markdown experiment`

        """

        stored_count = 0
        csv_data_tree = Path(f"{self.csv_data_dir}") / self.data_type_scraped / self.human_date
        d = csv_data_tree / _os
        d.mkdir(exist_ok=True, parents=True)
        f = d / f"{region}.csv"

        logger.debug("{} storing data to csv file {}".format(self._id, f.relative_to(self.csv_data_dir)))

        if f.exists():
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
        except Exception as e:  # pragma: no cover
            logger.error("Error saving data: {}".format(e), exc_info=True)
            return 0, len(instances)
        logger.debug("saved data to file: '{}'".format(f))

        return stored_count, 0

    def scrape_and_store(self, _os: str, region: str) -> bool:
        """
        Select an operating system and region to fill the pricing page table with data, scrape it, and save it.
        CSV files are saved in a parent directory of self.csv_data_dir, by date then by operating system. e.g. '<self.csv_data_dir>/2023-01-18/Linux'
        This function is meant to be run in a ThreadDivvier singleton.
        NOTE: contains try/catch for self.driver
        """
        global ROWS_STORED
        t_thread_start = int(time.time())
        logger.debug(f"{self._id} scrape and store: {region=} {_os=}")
        ################# # Scrape #################
        instances: List[PGInstance] = self.collect_ec2_data(_os=_os, region=region)

        try:
            ################# # Postgres #################
            if self.db_config is not None:
                stored_count, error_count = self.store_postgres(instances)
                ROWS_STORED += stored_count
                if error_count > 0:
                    logger.warning("{} stored {}/{} records for {}: {} with {} errors.".format(self._id, stored_count, len(instances), region, _os, error_count))
                else:
                    logger.debug("{} stored {}/{} records for {}: {} with {} errors.".format(self._id, stored_count, len(instances), region, _os, error_count))
            else:
                logger.debug("{} skip saving to database".format(self._id))

            ################# # CSV #################
            if self.csv_data_dir is not None:
                stored_count, error_count = self.save_csv(region, _os, instances)
                logger.debug("{} saved {}/{} csv rows for {}: {} with {} errors.".format(self._id, stored_count, len(instances), region, _os, error_count))
            else:
                logger.debug("{} skip saving to csv file".format(self._id))

            t_run = int(time.time() - t_thread_start)
            _THREAD_RUN_TIMES.append({
                'id': '-'.join([self.human_date, region, _os]),
                'num_instances': len(instances),
                't_run': t_run
            })
            # logger.critical("\033[42m(good)\033[0m added thread metric '{}' \033[104m{}/{}s = {}\033[0m".format(
            #     '-'.join([self.human_date, region, _os]), len(instances), t_run, len(instances) / t_run
            # ))
            self.lock.release()
            return True

        except Exception as e:  # pragma: no cover
            logger.critical("worker {}: While storing data for os '{}' for region '{}', an exception occurred which may result in dataloss: {}".format(self._id, _os, region, e), exc_info=True)
            _THREAD_RUN_TIMES.append({
                'id': '-'.join(["ERROR", self.human_date, region, _os]),
                'num_instances': len(instances),
                't_run': int(time.time() - t_thread_start)
            })
            # logger.critical("\033[41madded thread metric {} \033[104m{}\033[0m".format(('-'.join(["ERROR", self.human_date, region, _os]), len(instances), int(time.time() - t_thread_start)), int(time.time() - t_thread_start)))
            self.lock.release()
            raise ScrprException("worker {}: While storing data for os '{}' for region '{}', an exception occurred which may result in dataloss: {}".format(self._id, _os, region, e))

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
        except Exception as e:  # pragma: no cover
            self.driver.quit()
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
            dropdown_buttons = self.driver.find_elements(By.XPATH, '//*[@data-open]')
            for db in dropdown_buttons:
                if db.get_attribute('data-open') == "true":
                    logger.warning("Drop-down menu is still visible ")
            return

        except Exception as e:  # pragma: no cover
            self.driver.quit()
            logger.error("{} Error waiting for menus to close: {}".format(self._id, e), exc_info=True)
            raise e

    @contextmanager
    def get_db(self):
        """
        Uses DataCollectorConfig.db (db_config) to yield a connection to a Postgres database
        """
        if self.db_config is None:
            logger.debug("No db_config specified.")
            return
        logger.debug("connecting to database with config: {}".format(self.db_config))
        conn = psycopg2.connect(
            self.db_config.get_dsl()
        )
        yield conn
        # logger.debug("committing transactions")
        # conn.commit()
        logger.debug("{} closing database connection".format(self._id))
        conn.close()


def compress_data(csv_data_dir: str, data_type_scraped: str, human_date: str, rm_tree=True):
    """Save data to a zip archive immediately under data_dir instead of saving directory tree"""
    errors = False
    csv_data_tree = Path(f"{csv_data_dir}") / data_type_scraped / human_date
    assert csv_data_tree.exists()
    logger.debug("compressing data in '{}'".format(csv_data_tree.absolute()))
    old_cwd = os.getcwd()  # @@@ jank ???
    os.chdir(Path(csv_data_dir) / data_type_scraped)
    bkup_zip_file = Path(f"{human_date}.bkup-{uuid1()}.zip")
    if Path(f"{human_date}.zip").exists():
        logger.warning("Removing existing zip file '{}.zip'".format(human_date))
        Path(f"{human_date}.zip").rename(bkup_zip_file.name)

    with zipfile.ZipFile(f"{human_date}.zip", 'a', compression=zipfile.ZIP_DEFLATED) as zf:

        for os_name_dir in Path(human_date).iterdir():
            if os_name_dir.is_dir():
                _csv_ct = 0
                for csv_file in os_name_dir.iterdir():
                    _csv_ct += 1
                    try:
                        logger.debug("adding {}/{}/{}".format(human_date, os_name_dir.name, csv_file.name))
                        zf.write(f"{human_date}/{os_name_dir.name}/{csv_file.name}")
                    except Exception as e:  # pragma: no cover
                        errors = True
                        logger.error("Error writing compressed data: {}".format(e), exc_info=True)
                logger.debug("Compressed {} files of {} os data".format(_csv_ct, os_name_dir.name))
    if not errors:
        if bkup_zip_file.exists():
            logger.debug("removing temporary backup '{}'".format(bkup_zip_file.absolute()))
            bkup_zip_file.unlink()
        if rm_tree:
            logger.debug("Removing uncompressed data in '{}'".format(csv_data_tree.absolute()))
            shutil.rmtree(csv_data_tree)
    os.chdir(old_cwd)
    return


def do_args(sys_args):
    """
    fyi:
    ```json
    default_args = {
        "follow": False,
        "log_file": "<XDG_SHARE_DIR>/scrpr/logs/scrpr.log"
        "thread_count": 24,
        "overdrive_madness": False,
        "compress": False,
        "regions": None,
        "operating_systems": None,
        "get_operating_systems": False,
        "get_regions": False,
        "csv_data_dir": "<XDG_SHARE_DIR>/scrpr/csv-data",
        "store_csv": True,
        "store_db": True,
        "v": 0
    }
    ```
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--follow",
        required=False,
        action='store_true',
        help="print logs to stdout in addition to the log file")
    parser.add_argument("--log-file",
        required=False,
        default=DEFAULT_LOG_FILE,
        help="log output destination file. '--log-file=console' will instead print to your system's stderr stream.")
    parser.add_argument("-t", "--thread-count",
        default=cpu_count(),
        action='store',
        type=int,
        help="number of threads (Selenium drivers) to use")
    parser.add_argument("--overdrive-madness",
        action='store_true',
        help=f"allow going over the recommended number of threads (number of threads for your machine, {cpu_count()})")
    parser.add_argument("--compress",
        required=False,
        action=BooleanOptionalAction,
        default=True,
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
        default=DEFAULT_CSV_DATA_DIR,
        help="override the base directory for storing csv-data. Has no effect if --no-store-csv is also set.")
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

    parser.add_argument("--check-size",
        required=False,
        action='store_true',
        help='return the current size of the database and csv directory, then exit.')

    # parser.add_argument("-h", "--help",
    #     required=False,
    #     action='store_true',
    #     help='print help and exit.')

    args = parser.parse_args(sys_args)

    # return MainConfig(args)
    return args


def get_data_dir_size(data_dir=DEFAULT_CSV_DATA_DIR) -> int:
    """
    Returns the size in bytes of a directory.
    Roughly equivalent to du -bs data_dir/* (does not include size of data_dir)

    Returns -1 on error.
    """
    s = 0
    try:
        for base in os.walk(data_dir):
            # add files' sizes
            for d2 in base[2]:
                # print(Path(os.path.join(base[0], d2)).absolute(), Path(os.path.join(base[0], d2)).stat().st_size)
                s += Path(os.path.join(base[0], d2)).stat().st_size
            # traverse subdirectories
            if base[1]:
                for d1 in base[1]:
                    # print(Path(os.path.join(base[0], d1)).absolute(), Path(os.path.join(base[0], d1)).stat().st_size)
                    s += Path(os.path.join(base[0], d1)).stat().st_size
                    s += get_data_dir_size(os.path.join(base[0], d1))
            # nothing left to do
            else:
                return s
            return s
        return s
    except Exception as e:
        logger.warning("Error getting size of data directory '{}': {}".format(data_dir, e))
        return -1


def get_table_size(db_config: DatabaseConfig, table='ec2_instance_pricing') -> int:
    """
    Return the current on-disk size of a database table.
    Returns -1 on error.
    """
    # SELECT pg_size_pretty(pg_total_relation_size('public.ec2_instance_pricing'));  # kB, mB
    try:
        s = sql.SQL("SELECT pg_total_relation_size('public.{}')".format(table))
        conn = psycopg2.connect(db_config.get_dsl())
        curr = conn.cursor()
        curr.execute(s)
        r = curr.fetchone()
        if r is None:
            logger.error("Couldn't get table size data. Check your db_config.")
            raise ScrprException("Couldn't get table size data. Check your db_config.")
        size_bytes = r[0]
        conn.close()
        logger.debug("'{}' table is {:.2f} Mb ({} bytes)".format(
            table, size_bytes / 1024 / 1024, size_bytes
        ))
        return size_bytes
    except Exception as e:
        logger.error("Could not retrieve size of table '{}': {}".format(table, e))
        return -1


@dataclass
class MainConfig:
    """
    Required arguments to run main()
    These are provided as a Namespace by do_args()
    """
    follow: bool
    thread_count: int
    overdrive_madness: bool
    compress: bool
    regions: List[str]
    operating_systems: List[str]
    get_operating_systems: bool
    get_regions: bool
    store_csv: bool
    store_db: bool
    v: int
    check_size: bool
    log_file: str = DEFAULT_LOG_FILE
    csv_data_dir: str = DEFAULT_CSV_DATA_DIR

    def load(self):
        raise NotImplementedError

    def __repr__(self):
        return f"""\
            {__file__} {self.__dict__}
        """


@dataclass
class MetricData:
    """
    Run metadata stored after every successful run
    """
    date: datetime.datetime
    threads: int = -2
    oses: int = -1
    regions: int = -1
    t_init: float = -2
    t_run: float = -2
    s_csv: float = -2
    s_db: float = -2
    reported_errors: int = -2
    _command_line: dict = None

    def __init__(self, date):
        self.date = date

    @property
    def command_line(self):
        return self._command_line

    @command_line.setter
    def command_line(self, cl):
        if isinstance(cl, MainConfig):
            self._command_line = cl.__dict__
        elif isinstance(cl, dict):
            try:
                self._command_line = MainConfig(**cl).__dict__
            except Exception:
                logger.error("MetricData validation error")
                self._command_line = MainConfig(
                    follow=False,
                    thread_count=-1,
                    overdrive_madness=False,
                    compress=False,
                    regions=["MainConfig validation error"],
                    operating_systems=["MainConfig validation error"],
                    get_operating_systems=False,
                    get_regions=False,
                    store_csv=False,
                    store_db=False,
                    v=-1,
                    check_size=False,
                    log_file="MainConfig validation error",
                    csv_data_dir="MainConfig validation error",
                ).__dict__

    def as_dict(self):
        return OrderedDict({
            "date": self.date,
            "threads": self.threads,
            "oses": self.oses,
            "regions": self.regions,
            "t_init": self.t_init,
            "t_run": self.t_run,
            "s_csv": self.s_csv,
            "s_db": self.s_db,
            "reported_errors": self.reported_errors,
            "command_line": self.command_line,
        })

    def store(self, db_config: DatabaseConfig) -> bool:
        """
        Save a run's collected metrics to a Postgres table 'metric_data'.
        """
        if db_config is None:
            logger.warn("not storing data")
            return False
        try:
            conn = psycopg2.connect(db_config.get_dsl())
            curr = conn.cursor()
            curr.execute("""\
                INSERT INTO metric_data (date, threads, oses, regions, t_init, t_run, s_csv, s_db, reported_errors, command_line)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (self.date, self.threads, self.oses, self.regions, self.t_init, self.t_run, self.s_csv, self.s_db, self.reported_errors, json.dumps(self.command_line))
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.warning("Error saving metric data to database: {}".format(e))
            return False

    def store_csv(self, filename=DEFAULT_METRICS_DATA_FILE) -> bool:
        """
        Save metrics data in csv format to METRICS_DATA_FILE
        Returns True on success and False otherwise.
        """
        try:
            with open(filename, 'a') as md:
                csv.DictWriter(md, fieldnames=list(self.as_dict().keys()), delimiter='\t').writerow(self.as_dict())
            return True
        except Exception as e:
            logger.warning("Error saving metric data to file '{}': {}".format(filename, e))
            return False


def init_logging(verbosity: int, follow: bool, log_file: str | Path | None):
    logger = logging.getLogger()
    logging.getLogger('selenium.*').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    if log_file is None:
        log_file = DEFAULT_LOG_FILE
    if verbosity == 0:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    else:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(threadName)s (%(thread)s) : %(funcName)s : %(message)s')
    if follow:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    fh = RotatingFileHandler(
        str(log_file),
        maxBytes=5_000_000,  # 5MB
        backupCount=5
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


def get_date():
    """
    Get an unaware datestamp for ID purposes..
    """
    # @@@ tzinfo
    return datetime.datetime.now()


def api_status_wrapper(f):
    # atexit.register(exit_func)
    def wrapper(*args, **kwargs):
        set_api_status("starting")
        f(*args, **kwargs)
        set_api_status("idle")
    return wrapper


@api_status_wrapper
def main(args: MainConfig):  # noqa: C901
    global ROWS_STORED, ROWS_COLLECTED
    # main_process = psutil.Process()  # noqa: F841

    t_main = time.time()
    # set the collection date relative to time main() was called
    # this applies to all data stored in database
    datestamp = get_date()
    # human-readable datestamp
    human_date: str = datestamp.strftime("%Y-%m-%d")

    metric_data = MetricData(human_date)
    metric_data.command_line = args.__dict__

    ##########################################################################
    # logging
    ##########################################################################
    logger = init_logging(
        verbosity=args.v,
        follow=args.follow,
        log_file=args.log_file
    )
    # logger.critical(f"\033[44mmain mem: {main_process.memory_info().rss}\033[0m")

    logger.warning("This program is still under development, log output may be ... less than scrupulous.")
    logger.info("Starting program with PID {}".format(os.getpid()))
    logger.debug("datestamp : {}".format(datestamp))
    logger.debug("human_date: {}".format(human_date))

    # @@@ Always try and load database config
    db_config = DatabaseConfig()
    db_config.load()

    if args.check_size:
        s_db = -1
        try:
            s_db = get_table_size(db_config)
            print("database:")
            print("{:.2f} MB".format(s_db / 1024 / 1024))
        except Exception as e:
            print(e)
        print()
        try:
            s_csv = get_data_dir_size()
            print("csv data:")
            print("{:.2f} MB".format(s_csv / 1024 / 1024))
        except Exception as e:
            print(e)
        raise SystemExit(0)

    # argparsing
    if args.store_db:
        metric_data.s_db = -1
        conn = psycopg2.connect(db_config.get_dsl())
        conn.close()
        logger.debug("db connection ok")
    else:
        db_config = None

    # argparsing
    if args.store_csv:
        csv_data_dir = args.csv_data_dir
    else:
        csv_data_dir = DEFAULT_CSV_DATA_DIR

    config = EC2DataCollectorConfig(
        headless=True,
        human_date=human_date,
        csv_data_dir=csv_data_dir,
        db_config=db_config,
    )

    logger.debug("-----------------program args---------------------")
    for arg, val in vars(args).items():
        logger.debug("{}={} ({})".format(arg, val, type(val)))
    logger.debug("-----------------program config-------------------")
    for k, v in config.__dict__.items():
        logger.debug("{}={} ({})".format(k, v, type(v)))
    logger.debug("-----------------db config------------------------")
    logger.debug("{}".format(str(db_config)))
    logger.debug("--------------------------------------------------")

    # argparsing
    if args.store_db:
        s_db_start = get_table_size(db_config)
    if args.store_csv:
        s_csv_start = get_data_dir_size(csv_data_dir)

    ##########################################################################
    # regions and operating systems
    ##########################################################################
    set_api_status("collecting available regions and operating systems")
    # logger.critical(f"\033[44mmem before region/os collection: {main_process.memory_info().rss}\033[0m")
    os_region_collector = EC2DataCollector('os_region_collector', config=config)
    tgt_oses = os_region_collector.operating_system_dropdown.options
    tgt_regions = os_region_collector.region_dropdown.options
    # logger.critical(f"\033[44mmem after region/os collection: {main_process.memory_info().rss}\033[0m")
    # argparsing
    if args.get_operating_systems:
        logger.debug("tgt_oses = {}".format(tgt_oses))
        print("Available Operating Systems:")
        for _os in tgt_oses:
            print(f"\t'{_os}'")
    # argparsing
    if args.get_regions:
        logger.debug("tgt_regions = {}".format(tgt_regions))
        print("Available Regions:")
        for r in tgt_regions:
            print(f"\t{r}")
    os_region_collector.driver.quit()
    # logger.critical(f"\033[44mmem after region/os collection close: {main_process.memory_info().rss}\033[0m")
    # argparsing
    if args.get_operating_systems or args.get_regions:
        exit(0)

    ##########################################################################
    # main
    ##########################################################################
    set_api_status("running")
    # argparsing
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
    metric_data.regions = len(tgt_regions)

    # argparsing
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
    metric_data.oses = len(tgt_oses)

    num_threads = cpu_count()
    # argparsing
    if args.thread_count < 1:
        num_threads = 1
    elif args.thread_count > cpu_count() and not args.overdrive_madness:
        logger.warning("Using {} threads instead of requested {}".format(cpu_count(), args.thread_count))
        num_threads = cpu_count()
    else:
        num_threads = args.thread_count
    metric_data.threads = num_threads

    thread_tgts = []
    for o in tgt_oses:
        for r in tgt_regions:
            thread_tgts.append((o, r))
    logger.debug("thread targets ({}) = {}".format(len(thread_tgts), thread_tgts))

    metric_data.t_init = 0
    # logger.critical(f"\033[44mmem before ThreadDivvier init: {main_process.memory_info().rss}\033[0m")
    thread_thing = ThreadDivvier(thread_count=num_threads)
    # logger.critical(f"\033[44mmem before init_scrapers: {main_process.memory_info().rss}\033[0m")
    thread_thing.init_scrapers_of(config=config)
    # memory_profiler.memory_usage((thread_thing.init_scrapers_of, (config,), {}))

    # logger.critical(f"\033[44mmem after init_scrapers: {main_process.memory_info().rss}\033[0m")
    t_prog_init = time.time() - t_main
    metric_data.t_init = t_prog_init
    logger.debug("Initialized in {}".format(seconds_to_timer(time.time() - t_main)))

    # blocks until all threads have finished running, and thread_tgts is exhausted
    thread_thing.run_threads(thread_tgts)
    # logger.critical(f"\033[44mmem after scrapers ran: {main_process.memory_info().rss}\033[0m")

    ##########################################################################
    # after data has been collected
    ##########################################################################
    set_api_status("cleaning up")
    # argparsing
    if args.compress and csv_data_dir:
        compress_data(csv_data_dir, 'ec2', human_date, rm_tree=True)

    # argparsing
    if args.store_csv:
        metric_data.s_csv = get_data_dir_size(csv_data_dir) - s_csv_start

    # argparsing
    if args.store_db:
        s_db = get_table_size(db_config) - s_db_start
        metric_data.s_db = s_db

    metric_data.t_run = time.time() - t_main
    metric_data.reported_errors = len(ERRORS)

    logger.debug("run metrics:")
    for k, v in metric_data.as_dict().items():
        logger.debug(f"{k}: {v}")

    # ?? is it safe to assume db_config will always be available?? I mean, what if that assumption is made, and one day, its *not*?
    if db_config:
        logger.debug("db total size:\t{:.2f}M".format(get_table_size(db_config) / 1024 / 1024))
    logger.debug("csv total size:\t{:.2f}M".format(get_data_dir_size(csv_data_dir) / 1024 / 1024))
    logger.debug("Saving run's metric data to '{}'".format(DEFAULT_METRICS_DATA_FILE))
    metric_data.store(db_config)
    logger.info("Program finished with {} errors in {}".format(len(ERRORS), seconds_to_timer(metric_data.t_run)))
    logger.debug('---------------------------------------------------')
    logger.debug("errors ({}):".format(len(ERRORS)))
    for e in ERRORS:
        logger.error(e)
    logger.debug('---------------------------------------------------')
    print(f"{ROWS_COLLECTED=}")
    print(f"{ROWS_STORED=}")

    conn = psycopg2.connect(db_config.get_dsl())
    curr = conn.cursor()
    metric_store_errors = 0
    # Store run time of each thread
    # TODO: use floats
    # "id"                                          "t_run" "num_instances" "datetime_utc"
    # "2023-03-07-eu-south-2-Windows with SQL Web"	74	    21	            "2023-03-07 05:49:17.782281"
    for thread_run_time in _THREAD_RUN_TIMES:
        try:
            curr.execute("INSERT INTO ec2_thread_times VALUES (%s, %s, %s, %s)",
                (thread_run_time['id'], thread_run_time['num_instances'],
                    thread_run_time['t_run'], datestamp)
            )
            conn.commit()
        except UniqueViolation:
            metric_store_errors += 1
            conn.commit()
            continue
        except Exception:
            logger.error("misconfigured thread_run_times!")
    if metric_store_errors > 1:
        logger.error("failed to store")
    conn.close()
    with open('1999-01-01' + "-thread-metrics-" + ".txt", 'a') as f:
        for thread_run_time in _THREAD_RUN_TIMES:
            csv.DictWriter(f, fieldnames=['id', 'num_instances', 't_run'], delimiter='\t').writerow(thread_run_time)


# if __name__ == '__main__':
#     import sys
#     cli_args = do_args(sys.argv[1:])
#     args = MainConfig(**cli_args.__dict__)
#     raise SystemExit(main(args))
