import os
from multiprocessing import cpu_count

import pytest

from scrpr import scrpr


def test_get_data_dir_size(fake_csv_dir_tree):
    # for base in os.walk(dd):
    #     print("\t--->{}".format(base))
    assert scrpr.get_data_dir_size(fake_csv_dir_tree) == 12338  # 4096 * directories + 10 * files


def test_scrpr_exception():
    # ignore other tests throwing errors, which add to scrpr.ERRORS
    # not safe to run this test in parallel, now...
    old_global_errors = scrpr.ERRORS
    scrpr.ERRORS = []
    with pytest.raises(scrpr.ScrprException):
        raise scrpr.ScrprException("test exception")
    assert "test exception" in scrpr.ERRORS

    with pytest.raises(scrpr.ScrprCritical):
        raise scrpr.ScrprCritical("test critical")
    assert "test critical" in scrpr.ERRORS

    assert len(scrpr.ERRORS) == 2
    scrpr.ERRORS = old_global_errors


def test_main_config_has_no_default_arguments():
    """idk how to test but I want"""
    with pytest.raises(TypeError):
        scrpr.MainConfig()


def test_do_args_defaults_implement_main_config():
    default_data_dir = os.path.join(os.path.expanduser('~'), '.local', 'share', 'scrpr', 'csv-data')
    default_log_dir = os.path.join(os.path.expanduser('~'), '.local', 'share', 'scrpr', 'logs')

    mc_default = scrpr.MainConfig(
        follow=False,
        log_file=os.path.join(default_log_dir, "scrpr.log"),
        thread_count=cpu_count(),
        overdrive_madness=False,
        compress=True,
        regions=None,
        operating_systems=None,
        get_operating_systems=False,
        get_regions=False,
        csv_data_dir=default_data_dir,
        store_csv=True,
        store_db=True,
        v=0,
        check_size=False,
    )
    args = scrpr.do_args('')
    mc = scrpr.MainConfig(**vars(args))
    assert mc == mc_default


def test_global_scrpr_home():
    assert scrpr.SCRPR_HOME == os.path.join(os.path.expanduser('~'), '.local', 'share', 'scrpr')
