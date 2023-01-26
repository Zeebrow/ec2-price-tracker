import time
from datetime import datetime
import csv
from pathlib import Path
from math import floor
from typing import List, Dict
import zipfile
import logging
from dataclasses import dataclass
from copy import copy
import threading
import argparse
from multiprocessing import cpu_count
# 99% sure catching SIGINT hangs Selenium 
#import signal
import psutil

from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium import webdriver

#from urllib3 import exceptions as urllib3_exceptions

"""
Scrapes pricing information for EC2 instance types for available regions.
"""
logger = logging.getLogger(__name__)

###############################################################################
# classes and functions
###############################################################################
@dataclass
class EC2ScraperConfig:
    """
    Required options to instantiate an EC2Scraper.
    """
    human_date: str
    log_file: str = 'scrape_ec2.log' # None to supress log output
    csv_data_dir: Path = Path('csv-data/ec2')
    page_load_delay: int = 5 #seconds
    url: str = "https://aws.amazon.com/ec2/pricing/on-demand/"
    t_prog_start: float = time.time()

def time_me(f):
    def _timer(*args, **kwargs):
        t_i = time.time()
        f(*args, **kwargs)
        logger.debug(f"function {f.__name__} ET: {time.time() - t_i}s")
    return _timer

seconds_to_timer = lambda x: f"{floor(x/60)}m:{x%60:.1000f}s ({x} seconds)"

class Instance:
    def __init__(self, datestamp: str, region: str, operating_system:str, instance_type: str , cost_per_hr:str , cpu_ct:str , ram_size:str , storage_type:str , network_throughput: str) -> None:
        self.region = region
        self.operating_system = operating_system
        self.instance_type = instance_type
        self.cost_per_hr = cost_per_hr
        self.cpu_ct = cpu_ct
        self.ram_size = ram_size
        self.storage_type = storage_type
        self.network_throughput = network_throughput
        self.datestamp = datestamp # YYYY-MM-DD

    def __repr__(self) -> str:
        return f"{self.instance_type} {self.operating_system} {self.region}"

    @classmethod
    def get_csv_fields(self):
        return [
            "date",
            "instance_type",
            "operating_system",
            "region",
            "cost_per_hr",
            "cpu_ct",
            "ram_size_gb",
            "storage_type",
            "network_throughput"
        ]
    def as_dict(self):
        return {
            "date": self.datestamp,
            "instance_type": self.instance_type,
            "operating_system": self.operating_system,
            "region": self.region,
            "cost_per_hr": self.cost_per_hr,
            "cpu_ct": self.cpu_ct,
            "ram_size_gb": self.ram_size,
            "storage_type": self.storage_type,
            "network_throughput": self.network_throughput,
        }

class ThreadDivvier:
    """
    Converts machine time processing data into developer time debugging exceptions.
    """
    def __init__(self, config: EC2ScraperConfig, thread_count=cpu_count()) -> None:
        """
        Initialize one EC2Scraper for use in a thread, creating thread_count EC2Scraper instances with identical configuration.
        Each thread manages its own Selenium.WebDriver.
        """
        logger.debug("init ThreadDivvier with {} threads".format(thread_count))
        self.thread_count = thread_count
        self.drivers: List[EC2Scraper] = []
        self.config = config
        self.done = False
        self.init_scrapers()

    def init_scraper(self, thread_id: int, config: EC2ScraperConfig):
        self.drivers.append(EC2Scraper(_id=thread_id, config=config))
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
        logger.info("done initializing {} drivers".format(self.thread_count))
        return

    def run_threads(self, arg_queue: List[tuple]):
        """
        Takes a list of tuples (operating_system, region) and gathers corresponding EC2 instance pricing data.
        Items in the arg_queue are removed as work is completed, until there are no items left in the queue.
        """
        logger.info(f"running {self.thread_count} threads")
        _arg_queue = copy(arg_queue) #
        while _arg_queue:
            for d in self.drivers:
                if not d.lock.locked():
                    try:
                        args = _arg_queue.pop()
                    except IndexError:
                        logger.warning(f"driver {d._id} tried to pop from an empty queue")
                        continue
                    logger.debug(f"{len(_arg_queue)} left in queue")
                    t = threading.Thread(name='-'.join(args), target=d.scrape_and_store, args=args, daemon=False)
                    t.start()
                    logger.debug(f"worker id {d._id} running new thread {'-'.join(args)}")
        # determine when all threads have finished
        threads_finished = []
        logger.info("waiting for last threads to finish juuuust a sec")
        while len(threads_finished) != self.thread_count:
            for d in self.drivers:
                if not d.lock.locked() and d._id not in threads_finished:
                    threads_finished.append(d._id)
                    d.driver.close()

class EC2Scraper:
    class NavSection:
        """3+ lines of code"""
        ON_DEMAND_PRICING = "On-Demand Pricing"

    def __init__(self, _id, config: EC2ScraperConfig):
        logger.debug("init EC2 Scraper")
        self.lock = threading.Lock()
        self.lock.acquire()
        self._id = _id
        ###################################################
        # config
        ###################################################
        self.log_file = config.log_file
        self.url = config.url
        self.page_load_delay = config.page_load_delay
        self.t_prog_start = config.t_prog_start
        self.log_file = config.log_file
        self.csv_data_dir = config.csv_data_dir
        self.url = config.url
        self.human_date = config.human_date

        options = webdriver.FirefoxOptions()
        options.binary_location = '/usr/bin/firefox-esr'
        options.headless = True
        driverService = Service('/usr/local/bin/geckodriver')
        self.driver = webdriver.Firefox(service=driverService, options=options)
        try: 
            self.waiter = WebDriverWait(self.driver, 10)
            self.driver.get(self.url)
            logger.debug("Initializing worker with id {}".format(self._id))
            # self.nav_to(self.NavSection.ON_DEMAND_PRICING)
            logger.debug(f"{self._id} begin nav_to")
            sidebar = self.driver.find_element(By.CLASS_NAME, 'lb-sidebar-content')
            for elem in sidebar.find_elements(By.XPATH, 'div/a'):
                if elem.text.strip() == self.NavSection.ON_DEMAND_PRICING:
                    elem.click()
                    break
            self.waiter.until(ec.visibility_of_element_located((By.ID, "iFrameResizer0")))
            self.iframe = self.driver.find_element('id', "iFrameResizer0")
            self.lock.release()
        except Exception as e:
            logger.critical(e)
            self.driver.close()

    @classmethod
    def nav_to_cm(self, driver: webdriver.Firefox, section: NavSection):
        """
        Select a section to mimic scrolling
        NOTE: contains try/catch for driver
        """
        logger.debug("begin nav_to")
        try:
            sidebar = driver.find_element(By.CLASS_NAME, 'lb-sidebar-content')
            for elem in sidebar.find_elements(By.XPATH, 'div/a'):
                if elem.text.strip() == section:
                    elem.click()
                    time.sleep(1)
                    return
        except Exception as e:
            driver.close()
            logger.error(e)
            raise e
        raise Exception(f"no such section '{section}'")

    def nav_to(self, section: NavSection):
        """Select a section to mimic scrolling"""
        pass

    def get_available_operating_systems(self) -> List[str]:
        """
        Get all operating systems for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug(f"{self._id} get available os")
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
        logger.debug(f"{self._id} select os {_os}")
        try:
            self.driver.switch_to.frame(self.iframe)
            self.waiter.until(ec.visibility_of_element_located((By.ID, "awsui-select-2")))
            os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
            os_selection_box.click()
            for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
                if elem.text == _os:
                    elem.click()
                    self.wait_for_menus_to_close()
                    self.driver.switch_to.default_content()
                    return
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(e)
            raise e
        raise Exception(f"{self._id} No such operating system: '{_os}'")

    @classmethod
    def get_available_regions_and_os(self, config: EC2ScraperConfig) -> List[tuple]:
        """
        Return a tuple of all the possible combinations of operating systems and regions for which to iterate over
        NOTE: contains try/catch for driver
        NOTE: switches to iframe
        """
        results = {}
        #@@ what if a region becomes not available for a particular OS?
        logger.debug(f"get regions and os")
        options = webdriver.FirefoxOptions()
        options.binary_location = '/usr/bin/firefox-esr'
        options.headless = True
        driverService = Service('/usr/local/bin/geckodriver')
        driver = webdriver.Firefox(service=driverService, options=options)
        try:
            driver.get(config.url)
            time.sleep(config.page_load_delay)
            iframe = driver.find_element('id', "iFrameResizer0")
            EC2Scraper.nav_to_cm(driver, EC2Scraper.NavSection.ON_DEMAND_PRICING)
            driver.switch_to.frame(iframe)

            #########
            # OS
            #########
            available_operating_systems = []
            os_selection_box = driver.find_element(By.ID, 'awsui-select-2')
            os_selection_box.click()
            for elem in driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
                available_operating_systems.append(elem.text)
                results[elem.text] = []
            # collapse
            os_selection_box.click()

            #########
            # Regions
            #########
            available_regions = []
            region_selection_box = driver.find_element('id', 'awsui-select-1-textbox')
            region_selection_box.click()
            for elem in driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
                available_regions.append(elem.text)
            region_selection_box.click()
        finally:
            driver.close()
        logger.debug(f"found {len(available_operating_systems)} operating systems and {len(available_regions)} regions")
        return (available_operating_systems, available_regions)

    def get_available_regions(self) -> List[str]:
        """
        Get all aws regions for filtering data
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug(f"{self._id} get regions")
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
            self.driver.close()
            logger.error(f"{self._id}: {e}")
            raise e
        return available_regions

    def select_aws_region_dropdown(self, region: str):
        """
        Set the table of data to display for a given aws region
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug(f"{self._id} select region '{region}'")
        try:
            self.driver.switch_to.frame(self.iframe)
            ################
            # Location Type
            ################
            location_type_select_box = self.driver.find_element(By.ID, 'awsui-select-0')
            location_type_select_box.click()
            self.wait_for_menus_to_close()

            # what you have to click firt in order to show the dropdown 
            filter_selection = 'AWS Region'
            all_filter_options = self.driver.find_element(By.ID, 'awsui-select-0-dropdown')
            all_filter_options.find_element(By.XPATH, f'//*[ text() = "{filter_selection}"]').click()
            self.wait_for_menus_to_close()
            
            ################
            # Region
            ################
            region_select_box = self.driver.find_element(By.ID, 'awsui-select-1-textbox')
            region_select_box.click()
            search_box = self.driver.find_element(By.ID, 'awsui-input-0')
            search_box.send_keys(region)
            for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
                if elem.text == region:
                    elem.click()
                    self.wait_for_menus_to_close()
                    self.driver.switch_to.default_content()
                    return
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(f"{self._id}: {e}")
            raise e
        raise Exception(f"No such region: '{region}'")

    def scrape_and_store(self, _os: str, region:str) -> List[Instance]:
        """
        Select an operating system and region to fill the pricing page table with data, scrape it, and save it to a csv file.
        CSV files are saved in a parent directory of self.csv_data_dir, by date then by operating system. e.g. '<self.csv_data_dir>/2023-01-18/Linux'
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.info(f"{self._id} scrape all: {region=} {_os=}")
        self.lock.acquire()
        self.select_operating_system(_os)
        self.select_aws_region_dropdown(region)
        try:
            self.driver.switch_to.frame(self.iframe)
            rtn: List[Instance] = []
            # On EC2 pricing page:
            # page numbers (1, 2, 3, (literal)..., last, >)
            pages = self.driver.find_element(By.CLASS_NAME, 'awsui-table-pagination-content').find_elements(By.TAG_NAME, "li")
            pages[1].click() # make sure we're starting on page 1
            arrow_button = pages[-1] # >
            num_pages = pages[-2].find_element(By.TAG_NAME, 'button').text # last page number

            #################
            # scrape
            #################
            logger.debug(f"{self._id} scraping {num_pages} pages...")
            for i in range(int(num_pages)):
                # only cycle to next page after it has been scraped
                if i > 0:
                    arrow_button.click()
                selection = self.driver.find_element(By.TAG_NAME, 'tbody')
                # rows in HTML table
                tr_tags = selection.find_elements(By.TAG_NAME, 'tr')
                for tr in tr_tags:
                    instance_props = []
                    # columns in row
                    for td in tr.find_elements(By.TAG_NAME, "td"):
                        # 0 t3a.xlarge # 1 $0.1699 # 2 4 # 3 16 GiB # 4 EBS Only # 5 Up to 5 Gigabit
                        instance_props.append(td.text)
                    rtn.append(Instance(human_date, region, _os, *instance_props))
                # @@@
                time.sleep(0.2)

            #################
            # Store
            #################
            d = Path(self.csv_data_dir / self.human_date / _os)
            d.mkdir(exist_ok=True, parents=True)
            f = d / f"{region}.csv"
            if f.exists():
                logger.warning(f"Overwriting existing data at '{f.relative_to('.')}'")
                f.unlink()
            with f.open('w') as cf:
                fieldnames = Instance.get_csv_fields()
                writer = csv.DictWriter(cf, fieldnames=fieldnames)
                writer.writeheader()
                for inst in rtn:
                    writer.writerow(inst.as_dict())
            logger.debug(f"saved data to file: '{f}'")
            self.driver.switch_to.default_content()
            self.lock.release()
        except Exception as e:
            self.driver.quit()
            self.driver.close()
            logger.error(f"{self._id} {e}")
            raise e
        return rtn

    def get_result_count_test(self) -> int:
        """
        get the 'Viewing ### of ### Available Instances' count to check our results
        NOTE: contains try/catch for self.driver
        NOTE: switches to iframe
        """
        logger.debug(f"{self._id} get results count")
        try:
            self.driver.switch_to.frame(self.iframe)
            t =  self.driver.find_element(By.CLASS_NAME, 'awsui-table-header').find_element(By.TAG_NAME, "h2").text
            # ¯\_(ツ)_/¯
            self.driver.switch_to.default_content()
        except Exception as e:
            self.driver.close()
            logger.error(e)
            raise e
        return int(t.split("of")[1].split(" available")[0].strip())

    def wait_for_menus_to_close(self):
        """
        Blocks until any of the clickable dialog boxes in this script have returned to a collapsed state before returning.
        NOTE: contains try/catch for self.driver
        """
        try:
            logger.debug(f"{self._id} ensuring menus are closed")
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
            logger.error(f"{self._id} {e}")
            raise e

def compress_data(data_dir: str|Path):
    _os = Path(data_dir).relative_to('.')
    date_str = _os.name # don't rely on generating a fresh date from timestamp
    for i in _os.iterdir():
        if i.is_dir():
            # i.name is the operating system name for which regional data has been gathered
            friendly_os_name = '-'.join(i.name.split(' '))
            tgt_zipfile = _os / (friendly_os_name + f"_{date_str}.bz2.zip") # e.g. csv-data/ec2/2023-01-04/Linux_2023-01-04.bz2.zip
            with zipfile.ZipFile(str(tgt_zipfile), 'w', compression=zipfile.ZIP_BZIP2) as zf:
                _csv_ct = 0
                for csv_file in i.iterdir():
                    _csv_ct += 1
                    zf.write(csv_file.relative_to('.'))
            logger.debug(f"{i.stat()=}")
            logger.info(f"Compressed {_csv_ct} files of {friendly_os_name} os data to: {str(tgt_zipfile)} ({tgt_zipfile.stat().st_size} bytes)")

if __name__ == '__main__':
    t_main = time.time()
    human_date: str = datetime.fromtimestamp(floor(time.time())).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--thread-count", default=cpu_count(), action='store', type=int, help='number of threads (Selenium drivers) to use')
    parser.add_argument("--overdrive-madness", action='store_true', help='allow going over the maximum number of threads')
    parser.add_argument("--compress", required=False, action='store_true', help='program compresses resulting data, e.g. <os_name>_<date>.bz2.zip')
    args = parser.parse_args()

    if args.thread_count < 1:
        NUM_THREADS = 1
    elif args.thread_count > cpu_count() and not args.overdrive_madness:
        logger.warning("Using {} threads instead of requested {}".format(cpu_count(), args.thread_count))
        NUM_THREADS = cpu_count()
    else:
        NUM_THREADS = args.thread_count

    LOG_FILE = 'scrape_ec2.log'
    OPERATING_SYSTEMS = None # 'None' to scrape all
    REGIONS = None # 'None' to scrape all regions
    CSV_DATA_DIR = Path('csv-data/ec2')
    PAGE_LOAD_DELAY = 5 #seconds
    URL = "https://aws.amazon.com/ec2/pricing/on-demand/"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logging.getLogger('selenium.*').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s : %(funcName)s : %(levelname)s : %(name)s : %(message)s')
    if LOG_FILE:
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        logger.disabled = True
    logger.info("Starting program")

    config = EC2ScraperConfig(
        human_date=human_date,
        log_file=LOG_FILE,
        csv_data_dir=CSV_DATA_DIR,
        page_load_delay=PAGE_LOAD_DELAY
    )
    logger.debug(f"{config.log_file=}")
    logger.debug(f"{config.csv_data_dir=}")
    logger.debug(f"{config.page_load_delay=}")
    logger.debug(f"{config.url=}")

    # get list of operating_system-region pair tuples
    tgt_oses, tgt_regions = EC2Scraper.get_available_regions_and_os(config)
    thread_tgts = []
    for o in tgt_oses:
        for r in tgt_regions:
            thread_tgts.append((o,r))
    thread_thing = ThreadDivvier(config=config, thread_count=NUM_THREADS)
    t_prog_init = time.time() - t_main
    print(f"Initialized in {seconds_to_timer(time.time() - t_main)}")
    # blocks until all threads have finished running, and thread_tgts is exhausted
    thread_thing.run_threads(thread_tgts)

    if args.compress:
        compress_data(config.csv_data_dir / config.human_date)
    t_prog_tot = time.time() - t_main

    p = Path('{}/{}'.format(config.csv_data_dir, human_date))
    t_size = 0
    for _os in p.iterdir():
        if not _os.is_dir():
            continue
        for csvf in _os.iterdir():
            t_size += csvf.stat().st_size
    with open('metric-data.txt', 'a') as md:
        md.write(f"{NUM_THREADS}\t{len(tgt_oses)}\t{len(tgt_regions)}\t{t_prog_init:.2f}\t{t_prog_tot:.2f}\t{(t_size / 1024 / 1024):.3f}M\n")
    
    print(f"Program finished in {seconds_to_timer(time.time() - t_main)}")
    logger.info(f"Program finished in {seconds_to_timer(time.time() - t_main)}")
