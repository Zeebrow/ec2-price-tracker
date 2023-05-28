import json

import pytest

import scrpr


def test_metric_data_requires_dates():
    with pytest.raises(TypeError):
        scrpr.MetricData()


@pytest.mark.xfail
def test_metric_data_datestamp_validates():
    with pytest.raises(TypeError):
        scrpr.MetricData('not-a-date')


def test_metric_data_stores(db, pg_dbconfig, command_line):
    md = scrpr.MetricData('1999-12-31')
    md.command_line = command_line
    md.store(pg_dbconfig)
    curr = db.cursor()
    curr.execute(
        "SELECT * FROM metric_data"
    )
    assert len(curr.fetchall()) == 2


def test_command_line_deserializes_to_dict(db, pg_dbconfig, command_line):
    md = scrpr.MetricData('1999-12-31')
    md.command_line = command_line
    md.store(pg_dbconfig)
    curr = db.cursor()
    curr.execute(
        "SELECT * FROM metric_data"
    )
    for row in curr.fetchall():
        assert json.loads(row[10])


def test_metric_data_command_line_invalid_input_still_produces_storable_json__lol_more_words(db, pg_dbconfig):
    md = scrpr.MetricData('1999-01-02')
    md.command_line = {}
    print(md.command_line)
    assert isinstance(md.command_line, dict)
    md.store(pg_dbconfig)
    curr = db.cursor()
    curr.execute(
        "SELECT * FROM metric_data"
    )
    rows = curr.fetchall()
    assert len(rows) == 2
    for row in rows:
        assert json.loads(row[10])
