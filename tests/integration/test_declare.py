"""Execute generated DECLARE scripts against YDB."""

import ydb
from sqlglot import parse


def render_script(source: str) -> str:
    return ";\n".join(
        statement.sql(dialect="ydb") for statement in parse(source, dialect="ydb") if statement is not None
    )


def test_declared_parameter_executes(ydb_pool):
    yql = render_script("DECLARE $value AS Int32; SELECT $value + 1 AS result")
    result = ydb_pool.execute_with_retries(
        yql,
        {"$value": ydb.TypedValue(41, ydb.PrimitiveType.Int32)},
    )

    assert result[0].rows[0]["result"] == 42


def test_declared_special_types_compile_when_unused(ydb_pool):
    yql = render_script("DECLARE $nothing AS Null; DECLARE $void AS Void; SELECT 1 AS result")
    result = ydb_pool.execute_with_retries(yql)

    assert result[0].rows[0]["result"] == 1
