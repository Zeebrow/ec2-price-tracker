--postgres 14
CREATE TABLE IF NOT EXISTS instance_types (
    id                  SERIAL PRIMARY KEY,
    instance_type       varchar(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS operating_systems (
    id                  SERIAL PRIMARY KEY,
    operating_system    varchar(60) NOT NULL
);

CREATE TABLE IF NOT EXISTS regions (
    id                  SERIAL PRIMARY KEY,
    region              varchar(15) NOT NULL
);

CREATE TABLE IF NOT EXISTS storage_types (
    id                  SERIAL PRIMARY KEY,
    storage_type        varchar(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS network_throughputs (
    id                  SERIAL PRIMARY KEY,
    network_throughput  varchar(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS ec2_instance_pricing (
    id                  SERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    instance_type       varchar(20) NOT NULL,
    operating_system    varchar(60) NOT NULL,
    region              varchar(15) NOT NULL,
    cost_per_hr         float,
    cpu_ct              int,
    ram_size_gb         real,
    storage_type        varchar(20),
    network_throughput  varchar(20),
    UNIQUE (date, instance_type, operating_system, region)
);

ALTER TABLE IF EXISTS public.ec2_instance_pricing
    OWNER TO scrpr_test;
-- GRANT ALL ON ec2_instance_pricing TO scrpr_test;
-- GRANT ALL

INSERT INTO ec2_instance_pricing (date, instance_type, operating_system, region, cost_per_hr, cpu_ct, ram_size_gb, storage_type, network_throughput)
VALUES (
    '1900-01-01',
    'xyz.test',
    'operating system test',
    'region test',
    10.1234,
    129,
    767.5,
    'storage type test',
    'network thruput test'
);
