import tempfile
import shutil

import pytest

import scrpr


def test_driverless_datacollector(dc_no_driver):
    assert dc_no_driver.__getattribute__("driver") == "nodriver"

@pytest.mark.skip
def test_store_csv(data_dir, dc_no_driver: scrpr.DataCollector, instances):
    dc_no_driver.csv_
    assert False



@pytest.mark.skip
def test_store_postgres():
    assert False


@pytest.mark.skip
def test_compress():
    assert False


@pytest.mark.skip
def test_scrpr_exception_increments_global_err_count():
    assert False


@pytest.mark.skip
def test_default_cli_args_():
    assert False

@pytest.mark.skip
def test_instance_is_serializable():
    assert False
