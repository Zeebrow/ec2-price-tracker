# scrape cost data for ec2

Gather EC2 price info by scraping <https://aws.amazon.com/ec2/pricing/on-demand/>

# example data

`csv-data/ec2/2022-12-28/Linux/us-east-1.csv`
```
instance_type,cost_per_hr,cpu_ct,ram_size_gb,storage_type,network_throughput
a1.medium,$0.0255,1,2 GiB,EBS Only,Up to 10 Gigabit
a1.large,$0.051,2,4 GiB,EBS Only,Up to 10 Gigabit
a1.xlarge,$0.102,4,8 GiB,EBS Only,Up to 10 Gigabit
a1.2xlarge,$0.204,8,16 GiB,EBS Only,Up to 10 Gigabit
```

# api url

*maybe...*

