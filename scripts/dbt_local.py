"""Run dbt in its isolated environment; refuse every nonlocal target."""

import argparse
import sys


def main() -> int:
    arguments = sys.argv[1:]
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--target", "-t", action="append")
    options, _ = parser.parse_known_args(arguments)
    if not options.target or any(target != "local" for target in options.target):
        raise SystemExit("This wrapper requires --target local")
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(arguments)
    if result.exception:
        print(result.exception, file=sys.stderr)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
