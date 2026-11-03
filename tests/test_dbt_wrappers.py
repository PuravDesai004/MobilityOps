"""Target guards must inspect every spelling before invoking dbt."""

import sys
from types import ModuleType, SimpleNamespace

import pytest
from scripts import dbt_local, dbt_user_snowflake


@pytest.fixture
def dbt_calls(monkeypatch):
    calls = []
    module = ModuleType("dbt.cli.main")

    class Runner:
        def invoke(self, arguments):
            calls.append(arguments)
            return SimpleNamespace(success=True, exception=None)

    module.dbtRunner = Runner
    monkeypatch.setitem(sys.modules, "dbt.cli.main", module)
    return calls


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "--target", "local", "--target", "snowflake"],
        ["run", "--target", "local", "--target=snowflake"],
        ["run", "--target", "local", "-t", "snowflake"],
        ["run", "--target"],
        ["run"],
    ],
)
def test_local_wrapper_rejects_missing_or_conflicting_targets(monkeypatch, dbt_calls, arguments):
    monkeypatch.setattr(sys, "argv", ["dbt_local.py", *arguments])
    with pytest.raises(SystemExit):
        dbt_local.main()
    assert dbt_calls == []


@pytest.mark.parametrize("target", [["--target", "local"], ["--target=local"], ["-tlocal"]])
def test_local_wrapper_accepts_explicit_local_target(monkeypatch, dbt_calls, target):
    arguments = ["run", *target, "--target-path", "output"]
    monkeypatch.setattr(sys, "argv", ["dbt_local.py", *arguments])
    assert dbt_local.main() == 0
    assert dbt_calls == [arguments]


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "--target", "snowflake", "--target=local", "--profiles-dir", "private"],
        ["run", "--target", "snowflake", "--profiles-dir"],
        ["run", "--target", "snowflake", "--profiles-dir", ""],
        ["run", "--target"],
        ["run", "--target", "snowflake"],
    ],
)
def test_manual_wrapper_rejects_conflicting_targets_or_missing_profiles(
    monkeypatch, dbt_calls, arguments
):
    monkeypatch.setattr(sys, "argv", ["dbt_user_snowflake.py", *arguments])
    with pytest.raises(SystemExit):
        dbt_user_snowflake.main()
    assert dbt_calls == []


def test_manual_wrapper_accepts_equals_options(monkeypatch, dbt_calls):
    arguments = ["run", "--target=snowflake", "--profiles-dir=private"]
    monkeypatch.setattr(sys, "argv", ["dbt_user_snowflake.py", *arguments])
    dbt_user_snowflake.main()
    assert dbt_calls == [arguments]
