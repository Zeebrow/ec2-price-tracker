from scrpr import scrpr
import sys

cli_args = scrpr.do_args(sys.argv[1:])
args = scrpr.MainConfig(**cli_args.__dict__)
raise SystemExit(scrpr.main(args))
