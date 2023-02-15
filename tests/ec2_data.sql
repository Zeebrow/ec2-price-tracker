--postgres 14
CREATE TABLE public.ec2_instance_pricing_test (
    pk character varying(255) NOT NULL,
    date date NOT NULL,
    instance_type character varying(20) NOT NULL,
    operating_system character varying(60) NOT NULL,
    region character varying(15) NOT NULL,
    cost_per_hr double precision,
    cpu_ct integer,
    ram_size_gb real,
    storage_type character varying(20),
    network_throughput character varying(20)
);

ALTER TABLE public.ec2_instance_pricing_test OWNER TO scrpr_test;
ALTER TABLE ONLY public.ec2_instance_pricing_test
    ADD CONSTRAINT ec2_instance_pricing_test_pkey PRIMARY KEY (pk);

-- GRANT ALL ON ec2_instance_pricing TO scrpr_test;
-- GRANT ALL

INSERT INTO ec2_instance_pricing_test (pk, date, instance_type, operating_system, region, cost_per_hr, cpu_ct, ram_size_gb, storage_type, network_throughput)
VALUES (
    '1900-01-01-xyz.test-operating system test-region test',
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
