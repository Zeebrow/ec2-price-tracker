# https://www.simplilearn.com/tutorials/python-tutorial/selenium-with-python
import time
from datetime import datetime
import csv
from pathlib import Path
from math import floor, ceil
from typing import List, Any
import shutil
import zipfile
import os
import logging
from dataclasses import dataclass
import argparse

from pyautogui import hotkey
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.service import Service
from selenium import webdriver

"""
Scrapes pricing information for EC2 instance types for available regions.
Data for all regions (28) of a single operating system uses between 588K - 712K of disk space.
"""
logger = logging.getLogger(__name__)

###############################################################################
# globals, constants
###############################################################################
# DATABASE = 'ec2-costs.db'
# DWH_DATABASE = 'ec2-costs-dwh.db'
# MAIN_TABLE_NAME = 'ec2_costs'

@dataclass
class Config:
    log_file: str = 'scrape_ec2.log' # None to supress log output
    csv_data_dir: Path = Path('csv-data/ec2')
    timestamp: int = floor(time.time())
    page_load_delay: int = 5 #seconds
    wait_between_commands: int = 1 #seconds
    url: str = "https://aws.amazon.com/ec2/pricing/on-demand/"
    timezone: str = 'EST' # for uploading to s3
    upload_bucket_region: str = 'us-east-1'
    upload_bucket_name: str = 'quickhost-pricing-data'
    aws_profile: str = 'quickhost-ci-admin'
    t_prog_start: float = time.time()
    human_date: str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d") # date -I

###############################################################################
# set up storage
###############################################################################
# db = EC2CostsDatabase(db_name=DATABASE, main_table_name=MAIN_TABLE_NAME)
# dwh = EC2CostsDWH(ts=TIMESTAMP, dwh_db_name=DWH_DATABASE, dwh_main_table_name=MAIN_TABLE_NAME)
# db.nuke_tables()
# db.create_tables()
# dwh.create_tables()
# dwh_tgt_table = dwh.new_dwh_table()

###############################################################################
# classes and functions
###############################################################################
class MyScreen:
    """neato way to show what is going on in program"""
    PADDING_AMT = 1
    def __init__(self, w=None, h=None) -> None:
        self.w, self.h = os.get_terminal_size()
        self._static_w = None
        self._static_h = None
        if w is not None:
            self._static_w = w
            self.w = int(w)
        if h is not None:
            self._static_h = h
            self.h = int(h)
        self.lines = []
        for il in range(self.h):
            self.lines.append([])
            self.set_line(il, f"{il})...")
        self.draw_screen()

    def _get_term_size(self):
        self.w, self.h = os.get_terminal_size()
        if self._static_h:
            self.h = self._static_h
        if self._static_w:
            self.w = self._static_w

    def draw_line(self,lineno:int, msg: str):
        self.set_line(lineno=lineno, msg=msg)
        self.draw_screen()
    def set_line(self,lineno:int, msg: str):
        if len(msg) > self.w - MyScreen.PADDING_AMT:
            logger.warning("message exceeds termsize and could ruin your day")
            logger.debug(f"message len before trun: {len(msg)}")
            msg = msg[:self.w - MyScreen.PADDING_AMT]
            logger.debug(f"message len after trun: {len(msg)}")
            msg += '\033[0m'
            logger.debug(f"message len after color add: {len(msg)}")
        self.lines[lineno] = msg 
    def clear(self):
        [print() for l in self.lines]
    def draw_screen(self):
        for l in self.lines:
            print(''.join(l)+ " "*(self.w - len(l) - MyScreen.PADDING_AMT) + '\n', end='')
        for l in self.lines:
            print("\x1b[1A", end='')


class Instance:
    def __init__(self, region: str, operating_system:str, instance_type: str , cost_per_hr:str , cpu_ct:str , ram_size:str , storage_type:str , network_throughput: str) -> None:
        self.region = region
        self.operating_system = operating_system
        self.instance_type = instance_type
        self.cost_per_hr = cost_per_hr
        self.cpu_ct = cpu_ct
        self.ram_size = ram_size
        self.storage_type = storage_type
        self.network_throughput = network_throughput
        self.split_instance_type()
    def __repr__(self) -> str:
        return f"{self.instance_type} {self.operating_system} {self.region}"
    def split_instance_type(self):
        x, self.instance_size = self.instance_type.split(".")
        self.instance_family = x[0]
        self.instance_generation = x[1]
        self.instance_attributes = None
        if len(x) > 2:
            self.instance_attributes = x[2:]

    @classmethod
    def get_instance_details_fields(self):
        return [
            "instance_type",
            "instance_size",
            "instance_family",
            "instance_generation",
            "instance_attributes",
            "operating_system",
            "region",
            "cost_per_hr",
            "cpu_ct",
            "ram_size_gb",
            "storage_type",
            "network_throughput",
        ]
    @classmethod
    def get_csv_fields(self):
        return [
            "instance_type",
            "operating_system",
            "region",
            "cost_per_hr",
            "cpu_ct",
            "ram_size_gb",
            "storage_type",
            "network_throughput"
        ]

    def instance_details_dict(self):
        return {
            "instance_type": self.instance_type,
            "instance_size": self.instance_size,
            "instance_family": self.instance_family,
            "instance_generation": self.instance_generation,
            "instance_attributes": self.instance_attributes,
            "operating_system": self.operating_system,
            "region": self.region,
            "cost_per_hr": self.cost_per_hr,
            "cpu_ct": self.cpu_ct,
            "ram_size_gb": self.ram_size,
            "storage_type": self.storage_type,
            "network_throughput": self.network_throughput,
        }
    def as_dict(self):
        return {
            "instance_type": self.instance_type,
            "operating_system": self.operating_system,
            "region": self.region,
            "cost_per_hr": self.cost_per_hr,
            "cpu_ct": self.cpu_ct,
            "ram_size_gb": self.ram_size,
            "storage_type": self.storage_type,
            "network_throughput": self.network_throughput,
        }


class EC2Scraper:

    class NavSection:
        ON_DEMAND_PRICING = "On-Demand Pricing"
        DATA_TRANSFER = "Data Transfer"
        DATA_TRANSFER_WITHIN_THE_SAME_AWS_REGION = "Data Transfer within the same AWS Region"
        EBS_OPTIMIZED_INSTANCES = "EBS-Optimized Instances"
        ELASTIC_IP_ADDRESSES = "Elastic IP Addresses"
        CARRIER_IP_ADDRESSES = "Carrier IP Addresses"
        ELASTIC_LOAD_BALANCING = "Elastic Load Balancing"
        ON_DEMAND_CAPACITY_RESERVATIONS = "On-Demand Capacity Reservations"
        T2_T3_T4G_UNLIMITED_MODE_PRICING = "T2/T3/T4g Unlimited Mode Pricing"
        AMAZON_CLOUDWATCH = "Amazon CloudWatch"
        AMAZON_ELASTIC_BLOCK_STORE = "Amazon Elastic Block Store"
        AMAZON_EC2_AUTO_SCALING = "Amazon EC2 Auto Scaling"
        AWS_GOVCLOUD_REGION = "AWS GovCloud Region"

    def __init__(self, config: Config):
        ###################################################
        # config
        ###################################################
        self.log_file = config.log_file
        self.url = config.url
        self.page_load_delay = config.page_load_delay
        self.t_prog_start = config.t_prog_start
        self.log_file = config.log_file
        self.csv_data_dir = config.csv_data_dir
        self.timestamp = config.timestamp
        self.page_load_delay = config.page_load_delay
        self.wait_between_commands = config.wait_between_commands
        self.url = config.url
        self.human_date = config.human_date
        self.upload_bucket_region = config.upload_bucket_region
        self.upload_bucket_name = config.upload_bucket_name
        self.aws_profile = config.aws_profile

        print("Please wait for the browser to zoom out before moving it or changing focus away from it.")
        options = webdriver.FirefoxOptions()
        options.binary_location = '/usr/bin/firefox-esr'
        driverService = Service('/usr/local/bin/geckodriver')
        self.driver = webdriver.Firefox(service=driverService, options=options)
        self.driver.get(self.url)
        logger.debug(f"beginning in {str(self.page_load_delay)} seconds...")
        time.sleep(self.page_load_delay)
        logger.info("starting")
        self.iframe = self.driver.find_element('id', "iFrameResizer0")

        logger.debug(f"{self.t_prog_start=}")
        logger.debug(f"{self.log_file=}")
        logger.debug(f"{self.csv_data_dir=}")
        logger.debug(f"{self.timestamp=}")
        logger.debug(f"{self.page_load_delay=}")
        logger.debug(f"{self.wait_between_commands=}")
        logger.debug(f"{self.url=}")
        logger.debug(f"{self.human_date=}")
        logger.debug(f"{self.upload_bucket_region=}")
        logger.debug(f"{self.upload_bucket_name=}")
        logger.debug(f"{self.aws_profile=}")

    def zoom_browser(self):
        hotkey('ctrl', '-')
        hotkey('ctrl', '-')
        hotkey('ctrl', '-')
        hotkey('ctrl', '-')
        hotkey('ctrl', '-')
        hotkey('ctrl', '-')
        time.sleep(3)

    def nav_to(self, section: NavSection):
        """select a section to mimic scrolling"""
        sidebar = self.driver.find_element(By.CLASS_NAME, 'lb-sidebar-content')
        for elem in sidebar.find_elements(By.XPATH, 'div/a'):
            if elem.text.strip() == section:
                elem.click()
                time.sleep(self.wait_between_commands)
                return
        raise Exception(f"no such section '{section}'")

    def get_available_operating_systems(self) -> List[str]:
        """get all operating systems for filtering data"""
        self.driver.switch_to.frame(self.iframe)
        available_operating_systems = []
        os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
        os_selection_box.click()
        time.sleep(self.wait_between_commands)
        for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
            available_operating_systems.append(elem.text)
        # collapse
        os_selection_box.click()
        time.sleep(self.wait_between_commands)
        self.driver.switch_to.default_content()
        return available_operating_systems
        
    def select_operating_system(self, os: str):
        """select an operating system for filtering data"""
        self.driver.switch_to.frame(self.iframe)
        os_selection_box = self.driver.find_element(By.ID, 'awsui-select-2')
        os_selection_box.click()
        time.sleep(self.wait_between_commands)
        for elem in self.driver.find_element(By.ID, 'awsui-select-2-dropdown').find_elements(By.CLASS_NAME, 'awsui-select-option-label'):
            if elem.text == os:
                elem.click()
                time.sleep(self.wait_between_commands)
                self.driver.switch_to.default_content()
                return
        self.driver.switch_to.default_content()
        raise Exception(f"No such operating system: '{os}'")

    def get_available_regions(self) -> List[str]:
        """get all aws regions for filtering data"""
        self.driver.switch_to.frame(self.iframe)
        available_regions = []
        region_selection_box = self.driver.find_element('id', 'awsui-select-1-textbox')
        region_selection_box.click()
        time.sleep(self.wait_between_commands)
        for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
            available_regions.append(elem.text)
        region_selection_box.click()
        time.sleep(self.wait_between_commands)
        self.driver.switch_to.default_content()
        return available_regions

    def select_aws_region_dropdown(self, region: str):
        """Set the table of data to display for a given aws region"""
        self.driver.switch_to.frame(self.iframe)
        ################
        # Location Type
        ################
        location_type_select_box = self.driver.find_element(By.ID, 'awsui-select-0')
        location_type_select_box.click()
        time.sleep(self.wait_between_commands)

        # what you have to click firt in order to show the dropdown 
        filter_selection = 'AWS Region'
        all_filter_options = self.driver.find_element(By.ID, 'awsui-select-0-dropdown')
        all_filter_options.find_element(By.XPATH, f'//*[ text() = "{filter_selection}"]').click()
        time.sleep(self.wait_between_commands)
        
        ################
        # Region
        ################
        region_select_box = self.driver.find_element(By.ID, 'awsui-select-1-textbox')
        region_select_box.click()
        time.sleep(self.wait_between_commands)
        search_box = self.driver.find_element(By.ID, 'awsui-input-0')
        search_box.send_keys(region)
        time.sleep(self.wait_between_commands)
        for elem in self.driver.find_elements(By.CLASS_NAME, 'awsui-select-option-label-tag'):
            if elem.text == region:
                elem.click()
                time.sleep(self.wait_between_commands)
                self.driver.switch_to.default_content()
                return
        self.driver.switch_to.default_content()
        raise Exception(f"No such region: '{region}'")

    def scrape_all_(self, region:str, os: str, screen: MyScreen, screen_status_line: int) -> List[Instance]:
        """
        get all pages in iframe
        when finished searching, get number of pages.
        then iterate through them with the > arrow until num_pages 
        """
        self.driver.switch_to.frame(self.iframe)
        rtn = []
        # page numbers (1, 2, 3, (literal)..., last, >)
        pages = self.driver.find_element(By.CLASS_NAME, 'awsui-table-pagination-content').find_elements(By.TAG_NAME, "li")
        pages[1].click() # make sure were on page 1 when this function is run more than once
        time.sleep(self.wait_between_commands)
        arrow_button = pages[-1] # >
        num_pages = pages[-2].find_element(By.TAG_NAME, 'button').text # last page number

        for i in range(int(num_pages)):
            screen.draw_line(screen_status_line, f"scraping page {i+1}/{num_pages}...")
            # only cycle to next page after it has been scraped
            if i > 0:
                arrow_button.click()
                time.sleep(0.1)
            selection = self.driver.find_element(By.TAG_NAME, 'tbody')
            # rows in HTML table
            tr_tags = selection.find_elements(By.TAG_NAME, 'tr')
            for tr in tr_tags:
                instance_props = []
                # columns in row
                for td in tr.find_elements(By.TAG_NAME, "td"):
                    # 0 t3a.xlarge
                    # 1 $0.1699
                    # 2 4
                    # 3 16 GiB
                    # 4 EBS Only
                    # 5 Up to 5 Gigabit
                    instance_props.append(td.text)
                instance = Instance(region, os, *instance_props)
                # instance.store(db.conn, MAIN_TABLE_NAME)
                # instance.store(conn=dwh.conn, main_table_name=MAIN_TABLE_NAME, dwh_timestamp_suffix=TIMESTAMP)
                rtn.append(instance)
            time.sleep(0.2)
        self.driver.switch_to.default_content()
        return rtn

    def get_result_count_test(self) -> int:
        """get the 'Viewing ### of ### Available Instances' count to check our results"""
        self.driver.switch_to.frame(self.iframe)
        t =  self.driver.find_element(By.CLASS_NAME, 'awsui-table-header').find_element(By.TAG_NAME, "h2").text
        # ¯\_(ツ)_/¯
        self.driver.switch_to.default_content()
        return int(t.split("of")[1].split(" available")[0].strip())

    def run(self, tgt_regions=None, tgt_operating_systems=None):
        """
        operating_systems: List[Any] = None # 'None' to scrape ['Linux', 'Windows']
        regions = None # 'None' to scrape all regions
        """
        if not tgt_regions:
            tgt_regions = self.get_available_regions()
        if not tgt_operating_systems:
            tgt_operating_systems = ['Linux', 'Windows']

        base_scrape_dir = Path(self.csv_data_dir / self.human_date)
        if base_scrape_dir.exists():
            logger.warning(f"Deleting old data in '{base_scrape_dir}'")
            shutil.rmtree(base_scrape_dir)

        screen_os_lines = {}
        for n, r in enumerate(tgt_operating_systems):
            # 2 for title and "OS" heading
            screen_os_lines[r] = n+2
        screen_regions_lines = {}
        for n, r in enumerate(tgt_regions):
            # 2 for title and "OS" heading
            # 1 for "Regions" heading
            screen_regions_lines[r] = n+2+len(tgt_operating_systems)+1

        line_count = len(tgt_operating_systems) + len(tgt_regions) + 3 + 2
        screen = MyScreen(h=(len(tgt_operating_systems) + len(tgt_regions) + 3 + 2))
        screen.draw_line(0, "Scraperdoodle")
        screen.draw_line(1, "OS:")
        for t, n in screen_os_lines.items():
            screen.draw_line(n, f"- {t}")
        screen.draw_line(2+len(tgt_operating_systems), "Regions:")
        for t, n in screen_regions_lines.items():
            screen.draw_line(n, f"- {t}")
        screen.draw_line(line_count-2, "")
        screen.draw_line(line_count-1, "Status: starting...")
        status_line = line_count - 1

        self.zoom_browser()
        self.nav_to(self.NavSection.ON_DEMAND_PRICING)
        total_instance_count = 0
        for o_num, _os in enumerate(tgt_operating_systems):
            screen.draw_line(status_line, f"setting operating system")

            self.select_operating_system(_os)
            screen.draw_line(screen_os_lines[_os], f"\033[93m- {_os}\033[0m")

            # NOTE: this is where the directory structure is determined
            tgt_csv_data_dir = self.csv_data_dir / self.human_date / _os
            tgt_csv_data_dir.mkdir(parents=True, exist_ok=True)

            ###########################
            # Write csv file per region
            ###########################
            for r_num, region in enumerate(tgt_regions):
                # NOTE: if the selected region was not visible in the drop-down menu at the time it was clicked,
                # the page seems to jump back to the top. Happened on ca-central-1. There are also fancy dividers
                # between some regions
                screen.draw_line(status_line, f"setting region to {region}")

                self.select_aws_region_dropdown(region=region)
                t_region_scrape_start = time.time()
                screen.draw_line(screen_regions_lines[region], f"\033[93m- {region}\033[0m")
                screen.draw_line(status_line, f"scraping {region}...")

                stuff = self.scrape_all_(region=region, os=_os, screen=screen, screen_status_line=status_line)
                t_region_scrape = ceil(time.time() - t_region_scrape_start)
                if len(stuff) != self.get_result_count_test():
                    screen.draw_line(screen_regions_lines[region], f"\033[94m- {region} BAD DATA: (*{len(stuff)}* instances in {t_region_scrape} sec)\033[0m")
                    logger.warning("WARNING: possibly got bad data - make sure every thing in the 'On-Demand Plans for EC2' section is visible, and rerun")
                total_instance_count += len(stuff)
                screen.draw_line(screen_regions_lines[region], f"\033[93m- {region} ({len(stuff)} instances in {t_region_scrape} sec)\033[0m")
                screen.draw_line(status_line, f"writing csv data...")

                f = tgt_csv_data_dir / f"{region}.csv"
                with f.open('w') as cf:
                    fieldnames = Instance.get_csv_fields()
                    # don't need these fields, they are included in the filepath
                    fieldnames.remove("region")
                    fieldnames.remove("operating_system")
                    writer = csv.DictWriter(cf, fieldnames=fieldnames)
                    writer.writeheader()
                    for inst in stuff:
                        row = inst.as_dict()
                        row.pop("region")
                        row.pop("operating_system")
                        writer.writerow(row)
                _et = ceil(time.time() - self.t_prog_start)
                et = f"{floor(_et/60)}:{_et%60:02}"
                screen.draw_line(screen_regions_lines[region], f"\033[92m- {region} ({len(stuff)} instances in {t_region_scrape} sec) (saved to file '{f.relative_to('.')}')\033[0m")
                screen.draw_line(status_line, f"finished scraping {region}")
                logger.info(f"{et} - processed {len(stuff)} {_os} ({o_num+1}/{len(tgt_operating_systems)}) instances for {region} ({r_num+1}/{len(tgt_regions)}) in {t_region_scrape} seconds.")

            screen.draw_line(status_line, f"finished scraping {_os}")
            screen.draw_line(screen_os_lines[_os], f"\033[92m- {_os}\033[0m")
            for t, n in screen_regions_lines.items():
                screen.draw_line(n, f"- {t}")
            ###########################
            # zip the csv files per os
            ###########################
            zip_archive = f"{_os}_{self.human_date}.bz2.zip"
            with zipfile.ZipFile(str(base_scrape_dir / zip_archive), 'w', compression=zipfile.ZIP_BZIP2) as zf:
                for csv_file in tgt_csv_data_dir.iterdir():
                    zf.write(csv_file.relative_to('.'))
            logger.info(f"Compressed data stored: {str(base_scrape_dir/zip_archive)}")

        screen.clear()
        t_prog_end = ceil(time.time() - scraper.t_prog_start)
        self.driver.close()
        logger.info(f"Program finished in {t_prog_end} seconds")
        logger.info(f"{total_instance_count} instances were scraped for {len(tgt_operating_systems)} operating systems in {len(tgt_regions)} regions.")

###############################################################################
# main-ish
###############################################################################
if __name__ == '__main__':
    LOG_FILE = 'scrape_ec2.log' # None to supress log output
    OPERATING_SYSTEMS = None # 'None' to scrape ['Linux', 'Windows']
    REGIONS = None # 'None' to scrape all regions
    CSV_DATA_DIR = Path('csv-data/ec2')
    TIMESTAMP = floor(time.time())
    PAGE_LOAD_DELAY = 5 #seconds
    WAIT_BETWEEN_COMMANDS = 1 #seconds
    URL = "https://aws.amazon.com/ec2/pricing/on-demand/"
    TIMEZONE = 'EST' # for uploading to s3
    UPLOAD_BUCKET_REGION = 'us-east-1'
    UPLOAD_BUCKET_NAME = 'quickhost-pricing-data'
    AWS_PROFILE = 'quickhost-ci-admin'

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logging.getLogger('selenium.*').setLevel(logging.ERROR)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s:%(name)s: %(message)s')
    if LOG_FILE:
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        logger.disabled = True

    config = Config(
        log_file=LOG_FILE,
        csv_data_dir = CSV_DATA_DIR, 
        timestamp = TIMESTAMP,
        page_load_delay = PAGE_LOAD_DELAY,
        wait_between_commands = WAIT_BETWEEN_COMMANDS,
        url = URL,
        timezone = TIMEZONE, 
        upload_bucket_region = UPLOAD_BUCKET_REGION, 
        upload_bucket_name = UPLOAD_BUCKET_NAME,
        aws_profile = AWS_PROFILE,
    )
    scraper = EC2Scraper(config)
    scraper.run()
