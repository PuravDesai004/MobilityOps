"""Run dbt in its isolated environment; refuse every nonlocal target."""

import sys

from dbt.cli.main import dbtRunner


def main() -> int:
    arguments = sys.argv[1:]
    if "--target" not in arguments or arguments[arguments.index("--target") + 1] != "local":
        raise SystemExit("This wrapper requires --target local")
    result = dbtRunner().invoke(arguments)
    if result.exception:
        print(result.exception, file=sys.stderr)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
