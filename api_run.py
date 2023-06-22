from scrpr import scrpr
import sys
import logging
import json
import time


print("------- begin api_run.py output -------")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.debug("start")

print("command line:")
for arg in sys.argv:
    print(arg)

args = json.loads(sys.argv[1])
main_args = scrpr.MainConfig(**args)
ma_json = main_args.__dict__
print(json.dumps(ma_json, indent=2))
for k,v in ma_json.items():
    print(f"{k}={v} ({type(v)})")
print("-------end api_run.py output -------")
raise SystemExit(scrpr.main(main_args))
