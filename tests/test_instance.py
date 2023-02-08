import pytest
from psycopg2.errors import UniqueViolation

from ec2.instance import PGInstance

def test_pginstance_stores(db):
    instance = PGInstance(
        "2023-01-01",
        "test-region-1",
        "Red Hat Enterprise Linux with HA and SQL Enterprise",
        "m6gd.16xlarge",
        "$16.3074",
        "128",
        "768 GiB",
        "1 x 1900 NVMe SSD",
        "Up to 12500 Megabit"
    )
    curr = db.cursor()

    instance.store(db)
    with pytest.raises(UniqueViolation):
        instance.store(db)
    db.commit()
    curr.execute("SELECT * FROM ec2_instance_pricing WHERE region = 'test-region-1'")
    r = curr.fetchone()
    assert r is not None
    curr.close()

