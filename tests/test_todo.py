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


###########################################################
# refactored dropdowns + table Selenium operations
###########################################################

#        # @@@
#        # TODO put tests where they belong
#        # test table_functions_map table
#        # os: Linux
#        # Region: us-east-1 (Ohio)
#        assert table_functions_map['get_current_page']() == 1
#        print(table_functions_map['rows']['get_rows_count_on_page']())
#        table_functions_map['navto_next_page']()
#        assert table_functions_map['get_current_page']() == 2
#        # assert table_functions_map['rows']['get_rows_count_on_page']() == 20
#        table_functions_map['navto_next_page']()
#        assert table_functions_map['get_current_page']() == 3
#        # assert table_functions_map['rows']['get_rows_count_on_page']() == 20
#        table_functions_map['navto_next_page']()
#        assert table_functions_map['get_current_page']() == 4
#        assert table_functions_map['rows']['get_total_row_count']() == 586
#        assert table_functions_map['get_total_pages']() == 30
#
#        for col in ['Instance name', 'On-Demand hourly rate', 'vCPU', 'Memory', 'Storage', 'Network performance']:
#            assert col in table_functions_map['header']['content']
#
#        # [print(r.get_attribute('aria-rowindex')) for r in table_functions_map['rows']['get_rows']()]
#        # test rows are returned sequentially by using the aria-rowindex attribute (which we can take to be sequential)
#        for idx, _ in enumerate(c := [ int(row.get_attribute('aria-rowindex')) for row in table_functions_map['rows']['get_rows']() ]):  # noqa: E201,E202
#            if idx < len(c) - 2:
#                assert c[idx] == c[idx + 1] - 1
#

