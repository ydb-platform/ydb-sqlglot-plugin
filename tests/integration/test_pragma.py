"""Execute generated PRAGMA statements against YDB."""

import pytest
import ydb
from sqlglot import parse


def render_script(source: str) -> str:
    return ";\n".join(
        statement.sql(dialect="ydb") for statement in parse(source, dialect="ydb") if statement is not None
    )


@pytest.mark.parametrize(
    "pragma",
    (
        "UseTablePrefixForEach",
        "SimpleColumns",
        "DisableSimpleColumns",
        "CoalesceJoinKeysOnQualifiedAll",
        "DisableCoalesceJoinKeysOnQualifiedAll",
        "StrictJoinKeyTypes",
        "DisableStrictJoinKeyTypes",
        "AnsiInForEmptyOrNullableItemsCollections",
        "DisableAnsiInForEmptyOrNullableItemsCollections",
        "AnsiRankForNullableKeys",
        "DisableAnsiRankForNullableKeys",
        "AnsiCurrentRow",
        "AnsiOrderByLimitInUnionAll",
        "OrderedColumns",
        "DisableOrderedColumns",
        "PositionalUnionAll",
        "UnicodeLiterals",
        "DisableUnicodeLiterals",
        "WarnUntypedStringLiterals",
        "DisableWarnUntypedStringLiterals",
        "AllowDotInAlias",
        "WarnUnnamedColumns",
        "yson.AutoConvert",
        "yson.Strict",
        "yson.DisableStrict",
    ),
)
def test_documented_global_pragma_flag_executes(ydb_pool, pragma):
    yql = render_script(f"PRAGMA {pragma}; SELECT 1 AS result")
    result = ydb_pool.execute_with_retries(yql)

    assert result[0].rows[0]["result"] == 1


@pytest.mark.parametrize(
    "source",
    (
        'PRAGMA TablePathPrefix = "home/yql"; SELECT 1 AS result',
        'PRAGMA GroupByLimit = "64"; SELECT 1 AS result',
        'PRAGMA GroupByCubeLimit = "8"; SELECT 1 AS result',
        'PRAGMA RegexUseRe2 = "true"; SELECT 1 AS result',
        'PRAGMA ClassicDivision = "true"; SELECT 1 AS result',
        'PRAGMA yson.Strict = "true"; SELECT 1 AS result',
        'PRAGMA yson.Strict = "false"; SELECT 1 AS result',
        'PRAGMA yson.DisableStrict = "true"; SELECT 1 AS result',
        'PRAGMA yson.DisableStrict = "false"; SELECT 1 AS result',
        'PRAGMA Warning("disable", "1101"); SELECT 1 AS result',
    ),
)
def test_documented_global_pragma_value_executes(ydb_pool, source):
    yql = render_script(source)
    result = ydb_pool.execute_with_retries(yql)

    assert result[0].rows[0]["result"] == 1


def test_autocommit_executes_through_scripting_api(ydb_driver, ydb_pool):
    table = "integ_pragma_autocommit"
    ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")
    ydb_pool.execute_with_retries(f"CREATE TABLE `{table}` (id Int64 NOT NULL, PRIMARY KEY (id))")

    try:
        yql = render_script(f"PRAGMA AutoCommit; UPSERT INTO `{table}` (id) VALUES (1)")
        ydb.ScriptingClient(ydb_driver).execute_yql(yql)

        result = ydb_pool.execute_with_retries(f"SELECT id FROM `{table}`")
        assert result[0].rows[0]["id"] == 1
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE `{table}`")
