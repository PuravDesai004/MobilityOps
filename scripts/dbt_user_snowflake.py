"""USER-RUN ONLY: these commands connect to Snowflake. Never called by the local workflow."""

import sys


def main():
    args = sys.argv[1:]
    if "--target" not in args or args[args.index("--target") + 1] != "snowflake":
        raise SystemExit("This manual entry point requires explicit --target snowflake")
    if "--profiles-dir" not in args:
        raise SystemExit("Provide your private --profiles-dir; do not use the local-only profile")
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(args)
    if not result.success:
        if result.exception:
            print(result.exception, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
