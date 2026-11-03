"""USER-RUN ONLY: these commands connect to Snowflake. Never called by the local workflow."""

import argparse
import sys


def main():
    args = sys.argv[1:]
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--target", "-t", action="append")
    parser.add_argument("--profiles-dir", action="append")
    options, _ = parser.parse_known_args(args)
    if not options.target or any(target != "snowflake" for target in options.target):
        raise SystemExit("This manual entry point requires explicit --target snowflake")
    if not options.profiles_dir or not all(options.profiles_dir):
        raise SystemExit("Provide your private --profiles-dir; do not use the local-only profile")
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(args)
    if not result.success:
        if result.exception:
            print(result.exception, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
