"""Execute generated COMMIT scripts against YDB."""

import ydb
from sqlglot import parse


def render_script(source: str) -> str:
    return ";\n".join(
        statement.sql(dialect="ydb") for statement in parse(source, dialect="ydb") if statement is not None
    )


def test_commit_barrier_executes(ydb_driver, ydb_pool):
    table = "integ_commit_conformance"
    ydb_pool.execute_with_retries(f"CREATE TABLE `{table}` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id))")

    try:
        yql = render_script(
            f"UPSERT INTO `{table}` (id, value) VALUES (1, 'before'); "
            "COMMIT; "
            f"UPSERT INTO `{table}` (id, value) VALUES (2, 'after')"
        )
        ydb.ScriptingClient(ydb_driver).execute_yql(yql)

        result = ydb_pool.execute_with_retries(f"SELECT id, value FROM `{table}` ORDER BY id")
        assert [(row["id"], row["value"]) for row in result[0].rows] == [
            (1, "before"),
            (2, "after"),
        ]
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE `{table}`")
