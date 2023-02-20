import pytest

import scrpr


def test_metric_data_requires_dates():
    with pytest.raises(TypeError):
        scrpr.MetricData()


@pytest.mark.xfail
def test_metric_data_datestamp_validates():
    with pytest.raises(TypeError):
        scrpr.MetricData('not-a-date')


def test_metric_data_stores(db, pg_dbconfig):
    md = scrpr.MetricData('1999-12-31')
    md.store(pg_dbconfig)
    curr = db.cursor()
    curr.execute(
        "SELECT * FROM metric_data"
    )
    rows = curr.fetchall()
    assert len(rows) == 2
