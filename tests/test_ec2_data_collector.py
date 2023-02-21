from pathlib import Path
import os

import pytest

import scrpr


def test_test_datacollector_has_no_driver_attribute(ec2_driverless_dc):
    assert ec2_driverless_dc.__getattribute__('driver') == 'nodriver'


@pytest.mark.selenium
def test_get_driver_on_driverless_datacollector_sets_driver(ec2_driverless_dc):
    ec2_driverless_dc.get_driver()
    assert ec2_driverless_dc.__getattribute__('driver') != 'nodriver'


def test_store_postgres(db, ec2_driverless_dc: scrpr.EC2DataCollector, instances):
    human_date = "1999-12-31"
    ec2_driverless_dc.human_date = human_date

    stored_count, error_count = ec2_driverless_dc.store_postgres(instances=instances, table='ec2_instance_pricing_test')

    assert error_count == 0
    assert stored_count == len(instances)

    curr = db.cursor()
    curr.execute("SELECT * FROM ec2_instance_pricing_test")
    r = curr.fetchall()
    assert stored_count + 1 == len(r)  # +1  for initial data in database

    # try and fail to store same instances again
    stored_count, error_count = ec2_driverless_dc.store_postgres(instances=instances, table='ec2_instance_pricing_test')
    assert error_count == len(instances)
    assert stored_count == 0


@pytest.mark.selenium
@pytest.mark.parametrize('region', (
    'us-east-1',
    'ap-southeast-4'
    )
)
def test_select_region(ec2_data_collector: scrpr.EC2DataCollector, region):
    assert ec2_data_collector.select_region(region) == region
    with pytest.raises(scrpr.ScrprException):
        ec2_data_collector.select_region('not-a-region')


@pytest.mark.selenium
@pytest.mark.parametrize('opersysm', (
    'Linux',
    'Windows'
    )
)
def test_select_operating_system(ec2_data_collector: scrpr.EC2DataCollector, opersysm):
    assert ec2_data_collector.select_operating_system(opersysm) == opersysm
    with pytest.raises(scrpr.ScrprException):
        ec2_data_collector.select_operating_system('Linux from scratch')


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        # (800, 600),    # 4:3
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_get_regions(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_get_regions', ec2_data_collector_config)
    regions = dc.get_available_regions()
    assert regions is not None
    assert 'us-east-1' in regions
    assert list(set(regions)).sort() == regions.sort()


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        # (800, 600),    # 4:3
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_get_operating_systems(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_get_operating_systems', ec2_data_collector_config)
    oses = dc.get_available_operating_systems()
    assert oses is not None
    assert 'Linux' in oses
    assert 'Windows' in oses
    assert list(set(oses)).sort() == oses.sort()


@pytest.mark.selenium
def test_ec2_data_collector_is_idempotent(ec2_data_collector: scrpr.EC2DataCollector):
    ec2_data_collector.get_available_regions()
    ec2_data_collector.prep_driver()
    ec2_data_collector.get_available_regions()
    ec2_data_collector.get_available_regions()
    ec2_data_collector.get_available_operating_systems()
    ec2_data_collector.prep_driver()
    ec2_data_collector.get_available_operating_systems()
    ec2_data_collector.get_available_operating_systems()
    ec2_data_collector.select_operating_system('Linux')
    ec2_data_collector.select_region('us-east-1')
    ec2_data_collector.prep_driver()
    ec2_data_collector.select_operating_system('Linux')
    ec2_data_collector.select_operating_system('Windows')
    ec2_data_collector.select_region('ap-southeast-4')
    ec2_data_collector.select_region('us-east-1')


# Need to figure out how to get instances created within the scrape_and_store
# function to be mocked TEST_TABLE_NAME instances
@pytest.mark.skip
@pytest.mark.selenium
def test_scrape_and_store(ec2_data_collector: scrpr.EC2DataCollector):
    assert not ec2_data_collector.lock.locked()  # assumed by scrape_and_store function
    ec2_data_collector.lock.acquire()
    assert ec2_data_collector.scrape_and_store(_os='Linux', region='ap-southeast-4')
    assert not ec2_data_collector.lock.locked()  # ready to accept another job


@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (2560, 1440),  # 16:9
        (1920, 1080),  # 16:9
        # (800, 600),    # 4:3 # bad
        (2560, 1080),  # 21:9
        # (2048, 1152),  # 16:9
        # (1680, 1050),  # 16:10
        # (1600, 1200),  # 4:3
        # (1280, 1024),  # 5:4
    )
)
def test_collect_ec2_data(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_collect_ec2_data', ec2_data_collector_config)
    try:
        instances = dc.collect_ec2_data(_os='Linux', region='ap-southeast-4')
    except Exception as e:
        dc.driver.save_screenshot('screenshots/err_test_collect_ec2_data.png')
        raise e

    assert instances is not None
    for i in instances:
        assert i.region == 'ap-southeast-4'
        assert i.operating_system == 'Linux'


def test_save_csv(data_dir, ec2_driverless_dc: scrpr.EC2DataCollector, indexed_instances):
    human_date = "1999-12-31"
    d = Path(data_dir).absolute()
    ec2_driverless_dc.csv_data_dir = str(d)
    ec2_driverless_dc.human_date = human_date

    # since save_csv changes the working directory in order to work (jank), make
    # sure pytest changes back to original directory if test fails
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir('.')
        old_cwd = os.getcwd()

        for i in indexed_instances:
            stored_count, error_count = ec2_driverless_dc.save_csv(i[0], i[1], i[2])
            assert error_count == 0
            assert stored_count == len(i[2])
        csv_root = Path(d / ec2_driverless_dc.data_type_scraped / human_date)
        assert csv_root.exists()
        for os_dir in csv_root.iterdir():
            assert os_dir.name in ["test os 1", "test os 2"]
            for region_csv in os_dir.iterdir():
                assert region_csv.name in ["test-region-1.csv", "test-region-2.csv"]

        assert old_cwd == os.getcwd()

    assert Path(os.path.join(data_dir, "ec2")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 1")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 2")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 1", "test-region-1.csv")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 1", "test-region-2.csv")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 2", "test-region-1.csv")).exists()
    assert Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 2", "test-region-2.csv")).exists()

    with Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 1", "test-region-1.csv")).open('r') as f:
        contents = f.read()
        assert "ts1.type1" in contents
        assert "ts1.type2" in contents
        assert "ts1.type3" in contents

    with Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 1", "test-region-2.csv")).open('r') as f:
        contents = f.read()
        assert "ts1.type1" in contents
        assert "ts1.type2" in contents

    with Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 2", "test-region-1.csv")).open('r') as f:
        contents = f.read()
        assert "ts1.type1" in contents
        assert "ts1.type2" in contents
        assert "ts1.type3" in contents

    with Path(os.path.join(data_dir, "ec2", "1999-12-31", "test os 2", "test-region-2.csv")).open('r') as f:
        contents = f.read()
        assert "ts1.type1" in contents
        assert "ts1.type2" in contents


###############################################################################
# known failures
###############################################################################

@pytest.mark.xfail
@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (800, 600),    # 4:3
    )
)
def test_get_regions_failing(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_get_regions', ec2_data_collector_config)
    regions = dc.get_available_regions()
    assert regions is not None
    assert 'us-east-1' in regions
    assert list(set(regions)).sort() == regions.sort()


@pytest.mark.xfail
@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (800, 600),    # 4:3
    )
)
def test_get_operating_systems_failing(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_get_operating_systems', ec2_data_collector_config)
    oses = dc.get_available_operating_systems()
    assert oses is not None
    assert 'Linux' in oses
    assert 'Windows' in oses
    assert list(set(oses)).sort() == oses.sort()


@pytest.mark.xfail
@pytest.mark.selenium
@pytest.mark.parametrize(('window_w', 'window_h'), (
        (800, 600),
    )
)
def test_collect_ec2_data_failing(ec2_data_collector_config: scrpr.DataCollectorConfig, window_w, window_h):
    ec2_data_collector_config.window_w = window_w
    ec2_data_collector_config.window_h = window_h
    dc = scrpr.EC2DataCollector('test_collect_ec2_data', ec2_data_collector_config)
    try:
        instances = dc.collect_ec2_data(_os='Linux', region='ap-southeast-4')
    except Exception as e:
        dc.driver.save_screenshot('screenshots/err_test_collect_ec2_data.png')
        raise e

    assert instances is not None
    for i in instances:
        assert i.region == 'ap-southeast-4'
        assert i.operating_system == 'Linux'
