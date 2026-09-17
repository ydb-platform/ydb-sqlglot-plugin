"""Execute generated CREATE VIEW statements against YDB."""

from sqlglot import parse_one


def test_ydb_create_view_options_execute(ydb_pool):
    table = "integ_create_view_source"
    view = "integ_create_view_ydb"
    view_created = False

    try:
        ydb_pool.execute_with_retries(
            f"CREATE TABLE IF NOT EXISTS `{table}` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id))"
        )
        ydb_pool.execute_with_retries(f"UPSERT INTO `{table}` (id, value) VALUES (1, 'visible')")

        source = f"CREATE VIEW IF NOT EXISTS {view} WITH (security_invoker) AS SELECT id, value FROM `{table}`"
        yql = parse_one(source, dialect="ydb").sql(dialect="ydb")
        ydb_pool.execute_with_retries(yql)
        view_created = True
        ydb_pool.execute_with_retries(yql)

        result = ydb_pool.execute_with_retries(f"SELECT * FROM `{view}`")
        assert [(row["id"], row["value"]) for row in result[0].rows] == [(1, "visible")]
    finally:
        if view_created:
            ydb_pool.execute_with_retries(f"DROP VIEW {view}")
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")


def test_postgres_create_view_transpilation_executes(ydb_pool):
    view = "integ_create_view_postgres"
    view_created = False

    try:
        yql = parse_one(
            f"CREATE VIEW {view} AS SELECT 42 AS value",
            dialect="postgres",
        ).sql(dialect="ydb")
        ydb_pool.execute_with_retries(yql)
        view_created = True

        result = ydb_pool.execute_with_retries(f"SELECT * FROM `{view}`")
        assert result[0].rows[0]["value"] == 42
    finally:
        if view_created:
            ydb_pool.execute_with_retries(f"DROP VIEW {view}")
