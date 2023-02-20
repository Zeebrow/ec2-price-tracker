from ec2.instance import PGInstance


def test_pginstance_repr():
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
    assert str(instance) == ' '.join([
        "2023-01-01",
        "m6gd.16xlarge",
        "Red Hat Enterprise Linux with HA and SQL Enterprise",
        "test-region-1",
    ])


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

    assert instance.store(db, table='ec2_instance_pricing_test')
    assert not instance.store(db, table='ec2_instance_pricing_test')  # test returns False on psycopg2.errors.UniqueViolation

    curr.execute("SELECT * FROM ec2_instance_pricing_test WHERE region = 'test-region-1'")
    r = curr.fetchone()
    assert r is not None
    curr.close()
