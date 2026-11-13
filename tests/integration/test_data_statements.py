"""Execute generated VALUES, DELETE, REPLACE, and DROP TABLE statements."""

from sqlglot import parse_one


def transpile(source: str, read: str) -> str:
    return parse_one(source, dialect=read).sql(dialect="ydb")


def test_postgres_values_table_constructor_executes(ydb_pool):
    yql = transpile(
        "SELECT x, y FROM (VALUES (1, 2), (3, 4)) AS t(x, y) ORDER BY x",
        read="postgres",
    )
    result = ydb_pool.execute_with_retries(yql)

    assert [(row["x"], row["y"]) for row in result[0].rows] == [(1, 2), (3, 4)]


def test_generated_delete_forms_execute(ydb_pool):
    table = "integ_delete_conformance"
    ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")
    ydb_pool.execute_with_retries(f"CREATE TABLE `{table}` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id))")
    ydb_pool.execute_with_retries(
        f"UPSERT INTO `{table}` (id, value) VALUES (1, 'postgres'), (2, 'delete-on'), (3, 'keep')"
    )

    try:
        postgres_yql = transpile(
            f"DELETE FROM {table} WHERE id = 1 RETURNING id, value",
            read="postgres",
        )
        deleted = ydb_pool.execute_with_retries(postgres_yql)
        assert [(row["id"], row["value"]) for row in deleted[0].rows] == [(1, "postgres")]

        delete_on_yql = transpile(
            f"DELETE FROM `{table}` ON SELECT id FROM `{table}` WHERE id = 2",
            read="ydb",
        )
        ydb_pool.execute_with_retries(delete_on_yql)

        remaining = ydb_pool.execute_with_retries(f"SELECT id, value FROM `{table}` ORDER BY id")
        assert [(row["id"], row["value"]) for row in remaining[0].rows] == [(3, "keep")]
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE `{table}`")


def test_generated_replace_forms_execute(ydb_pool):
    target = "integ_replace_target"
    source = "integ_replace_source"
    for table in (target, source):
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")

    ydb_pool.execute_with_retries(
        f"CREATE TABLE `{target}` (id Int64 NOT NULL, value Utf8, extra Utf8, PRIMARY KEY (id))"
    )
    ydb_pool.execute_with_retries(f"CREATE TABLE `{source}` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id))")
    ydb_pool.execute_with_retries(f"UPSERT INTO `{target}` (id, value, extra) VALUES (1, 'old', 'remove me')")
    ydb_pool.execute_with_retries(f"UPSERT INTO `{source}` (id, value) VALUES (2, 'from select')")

    try:
        replace_values = transpile(
            f"REPLACE INTO `{target}` (id, value) VALUES (1, 'new')",
            read="ydb",
        )
        ydb_pool.execute_with_retries(replace_values)

        replace_select = transpile(
            f"REPLACE INTO `{target}` SELECT id, value FROM `{source}`",
            read="ydb",
        )
        ydb_pool.execute_with_retries(replace_select)

        result = ydb_pool.execute_with_retries(f"SELECT id, value, extra FROM `{target}` ORDER BY id")
        assert [(row["id"], row["value"], row["extra"]) for row in result[0].rows] == [
            (1, "new", None),
            (2, "from select", None),
        ]
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE `{source}`")
        ydb_pool.execute_with_retries(f"DROP TABLE `{target}`")


def test_postgres_drop_table_transpilation_executes(ydb_pool):
    table = "integ_drop_table_conformance"
    ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")
    ydb_pool.execute_with_retries(f"CREATE TABLE `{table}` (id Int64 NOT NULL, PRIMARY KEY (id))")

    yql = transpile(f"DROP TABLE {table}", read="postgres")
    ydb_pool.execute_with_retries(yql)

    # Recreating the same path proves that the generated DROP TABLE took effect.
    ydb_pool.execute_with_retries(f"CREATE TABLE `{table}` (id Int64 NOT NULL, PRIMARY KEY (id))")
    ydb_pool.execute_with_retries(f"DROP TABLE `{table}`")
