"""
Integration tests — require a live YDB instance.

Start with:  docker compose up -d
             (wait ~15 s for YDB to become ready)

Every test goes through the sqlglot transpiler:
    source SQL  →  transpile(sql, read=dialect)  →  YDB execution
This ensures that generator changes are caught by live YDB validation.

Tests skip automatically when YDB is unreachable, so running the full
test suite without Docker is safe:
    pytest tests/                    # unit tests only (integration skipped)
    pytest tests/test_integration.py # integration tests only
"""
import sqlglot


def transpile(sql: str, read: str = "sqlite") -> str:
    return sqlglot.parse_one(sql, dialect=read).sql(dialect="ydb")


# ---------------------------------------------------------------------------
# Connectivity — not a transpiler test, just a sanity check for the fixture
# ---------------------------------------------------------------------------

def test_connection(ydb_pool):
    ydb_pool.execute_with_retries("SELECT 1")


# ---------------------------------------------------------------------------
# Date / time functions
# ---------------------------------------------------------------------------

def test_extract_year(ydb_pool):
    # EXTRACT(YEAR FROM ...) → DateTime::GetYear(...)
    # The dialect emits AddTimezone(..., "Europe/Moscow"), so compare against Moscow time
    # to avoid flakiness around the UTC year boundary (Moscow is UTC+3).
    from datetime import datetime
    from zoneinfo import ZoneInfo
    yql = transpile("SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP)", read="postgres")
    result = ydb_pool.execute_with_retries(yql)
    assert result[0].rows[0][0] == datetime.now(ZoneInfo("Europe/Moscow")).year


def test_extract_month(ydb_pool):
    yql = transpile("SELECT EXTRACT(MONTH FROM CURRENT_TIMESTAMP)", read="postgres")
    result = ydb_pool.execute_with_retries(yql)
    assert 1 <= result[0].rows[0][0] <= 12


def test_interval_subtraction(ydb_pool):
    # INTERVAL n DAY → DateTime::IntervalFromDays(n)
    # Use a Timestamp column instead of CURRENT_TIMESTAMP to avoid TZ_TIMESTAMP
    # deserialization issues in the YDB Python SDK.
    TABLE = "integ_interval_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id Int64 NOT NULL, ts Timestamp NOT NULL, PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, ts) VALUES (1, Timestamp('2024-01-31T00:00:00Z'))
    """)
    try:
        yql = transpile(
            f"SELECT ts - INTERVAL 30 DAY AS result FROM `{TABLE}`",
            read="clickhouse",
        )
        result = ydb_pool.execute_with_retries(yql)
        assert result[0].rows[0]["result"] is not None
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# String functions
# ---------------------------------------------------------------------------

def test_upper_lower(ydb_pool):
    # UPPER → Unicode::ToUpper, LOWER → Unicode::ToLower
    yql = transpile("SELECT UPPER('hello'), LOWER('WORLD')", read="postgres")
    result = ydb_pool.execute_with_retries(yql)
    assert result[0].rows[0][0] == "HELLO"
    assert result[0].rows[0][1] == "world"


def test_length(ydb_pool):
    # LENGTH → Unicode::GetLength
    yql = transpile("SELECT LENGTH('hello')", read="postgres")
    result = ydb_pool.execute_with_retries(yql)
    assert result[0].rows[0][0] == 5


# ---------------------------------------------------------------------------
# Math
# ---------------------------------------------------------------------------

def test_round(ydb_pool):
    # ROUND(x, n) → Math::Round(x, -n)  (YDB sign convention is reversed)
    yql = transpile("SELECT ROUND(3.14159, 2)", read="postgres")
    result = ydb_pool.execute_with_retries(yql)
    assert abs(result[0].rows[0][0] - 3.14) < 1e-9


# ---------------------------------------------------------------------------
# dateDiff — requires a table with Timestamp columns
# ---------------------------------------------------------------------------

def test_datediff_minute(ydb_pool):
    # dateDiff('minute', start, end) → (CAST(end AS Int64) - CAST(start AS Int64)) / 60000000
    TABLE = "integ_datediff_minute"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id       Int64 NOT NULL,
            start_ts Timestamp NOT NULL,
            end_ts   Timestamp NOT NULL,
            PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, start_ts, end_ts) VALUES
        (1, Timestamp('2024-01-01T00:00:00Z'), Timestamp('2024-01-01T01:00:00Z'))
    """)
    try:
        yql = transpile(
            f"SELECT dateDiff('minute', start_ts, end_ts) AS diff FROM `{TABLE}`",
            read="clickhouse",
        )
        result = ydb_pool.execute_with_retries(yql)
        assert result[0].rows[0]["diff"] == 60
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


def test_datediff_day(ydb_pool):
    # dateDiff('day', ...) → / 86400000000
    TABLE = "integ_datediff_day"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id       Int64 NOT NULL,
            start_ts Timestamp NOT NULL,
            end_ts   Timestamp NOT NULL,
            PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, start_ts, end_ts) VALUES
        (1, Timestamp('2024-01-01T00:00:00Z'), Timestamp('2024-01-31T00:00:00Z'))
    """)
    try:
        yql = transpile(
            f"SELECT dateDiff('day', start_ts, end_ts) AS diff FROM `{TABLE}`",
            read="clickhouse",
        )
        result = ydb_pool.execute_with_retries(yql)
        assert result[0].rows[0]["diff"] == 30
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# UPDATE alias fix — issue #1
# ---------------------------------------------------------------------------

def test_update_alias_stripped(ydb_pool):
    """YDB rejects UPDATE table AS alias; verify the alias is removed and YDB accepts the result."""
    TABLE = "integ_update_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id   Int64 NOT NULL,
            name Utf8,
            PRIMARY KEY (id)
        )
    """)
    try:
        yql = transpile(
            f"UPDATE `{TABLE}` AS u SET name = 'x' WHERE u.id = 1",
            read="sqlite",
        )
        assert "AS u" not in yql, f"Table alias not stripped from UPDATE: {yql}"
        ydb_pool.execute_with_retries(yql)
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# CTE → YDB variable syntax
# ---------------------------------------------------------------------------

def test_cte_as_variable(ydb_pool):
    """WITH cte AS (...) SELECT should become $cte = (...); SELECT * FROM $cte AS cte."""
    TABLE = "integ_cte_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id     Int64 NOT NULL,
            status Int32,
            PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, status) VALUES (1, 1), (2, 0), (3, 1)
    """)
    try:
        yql = transpile(
            f"WITH active AS (SELECT id FROM `{TABLE}` WHERE status = 1)"
            f" SELECT * FROM active",
            read="sqlite",
        )
        assert yql.startswith("$active"), f"CTE not rewritten as YDB variable: {yql}"
        result = ydb_pool.execute_with_retries(yql)
        assert len(result[0].rows) == 2
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


def test_multiple_ctes(ydb_pool):
    """Multiple CTEs should each become a separate $var = (...) statement."""
    TABLE = "integ_multi_cte_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id       Int64 NOT NULL,
            category Utf8,
            amount   Int64,
            PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, category, amount) VALUES
        (1, 'A', 100), (2, 'A', 200), (3, 'B', 50), (4, 'B', 150)
    """)
    try:
        yql = transpile(
            f"""
            WITH
                cat_a AS (SELECT id, amount FROM `{TABLE}` WHERE category = 'A'),
                high  AS (SELECT id, amount FROM cat_a WHERE amount > 100)
            SELECT * FROM high
            """,
            read="sqlite",
        )
        # Both CTEs must be rewritten as YDB variables
        assert "$cat_a" in yql and "$high" in yql, f"CTEs not rewritten: {yql}"
        result = ydb_pool.execute_with_retries(yql)
        assert len(result[0].rows) == 1
        assert result[0].rows[0]["amount"] == 200
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# count() without args → COUNT(*)
# ---------------------------------------------------------------------------

def test_count_no_args(ydb_pool):
    """ClickHouse count() with no arguments should become COUNT(*)."""
    TABLE = "integ_count_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id Int64 NOT NULL, PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id) VALUES (1), (2), (3)
    """)
    try:
        yql = transpile(f"SELECT count() FROM `{TABLE}`", read="clickhouse")
        assert "COUNT(*)" in yql, f"count() not rewritten to COUNT(*): {yql}"
        result = ydb_pool.execute_with_retries(yql)
        assert result[0].rows[0][0] == 3
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# GROUP BY with column aliases (no outer parentheses)
# ---------------------------------------------------------------------------

def test_group_by_with_alias(ydb_pool):
    """GROUP BY items should have AS aliases and not be wrapped in extra parens."""
    TABLE = "integ_groupby_test"
    ydb_pool.execute_with_retries(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            id       Int64 NOT NULL,
            category Utf8,
            amount   Int64,
            PRIMARY KEY (id)
        )
    """)
    ydb_pool.execute_with_retries(f"""
        UPSERT INTO `{TABLE}` (id, category, amount) VALUES
        (1, 'A', 10), (2, 'A', 20), (3, 'B', 5)
    """)
    try:
        yql = transpile(
            f"SELECT category, count(), sum(amount) FROM `{TABLE}` GROUP BY category",
            read="clickhouse",
        )
        result = ydb_pool.execute_with_retries(yql)
        assert len(result[0].rows) == 2
    finally:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{TABLE}`")


# ---------------------------------------------------------------------------
# CONCAT → || operator
# ---------------------------------------------------------------------------

def test_concat_to_pipe(ydb_pool):
    """CONCAT(a, b, ...) should become a || b || ..."""
    yql = transpile("SELECT CONCAT('hello', ' ', 'world')", read="postgres")
    assert "||" in yql, f"CONCAT not rewritten to ||: {yql}"
    result = ydb_pool.execute_with_retries(yql)
    assert result[0].rows[0][0] == b"hello world"
