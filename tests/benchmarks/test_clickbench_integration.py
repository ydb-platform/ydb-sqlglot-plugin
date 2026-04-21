"""
ClickBench integration tests — require a live YDB instance.

Start with:  docker compose up -d

Each test transpiles a ClickBench query (ClickHouse → YDB), runs it on the small
fixture dataset, and asserts row-level parity with ClickHouse (oracle data in
tests/fixtures/clickbench/oracle_compare.py).
"""

from pathlib import Path

import pytest
from sqlglot import parse_one

from tests.fixtures.clickbench.oracle_compare import (
    CLICKBENCH_EXPECTED,
    assert_matches_clickhouse,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "clickbench"
QUERIES_FILE = FIXTURES / "queries.sql"

_queries = [q.strip() for q in QUERIES_FILE.read_text().splitlines() if q.strip()]

assert len(_queries) == len(CLICKBENCH_EXPECTED)


@pytest.fixture(scope="module")
def hits_table(ydb_pool):
    create_sql = (FIXTURES / "create.sql").read_text()
    insert_sql = (FIXTURES / "insert.sql").read_text()

    ydb_pool.execute_with_retries("DROP TABLE IF EXISTS `hits`")
    ydb_pool.execute_with_retries(create_sql)
    ydb_pool.execute_with_retries(insert_sql)

    yield

    ydb_pool.execute_with_retries("DROP TABLE IF EXISTS `hits`")


@pytest.mark.parametrize(
    ("query", "expected"),
    list(zip(_queries, CLICKBENCH_EXPECTED)),
    ids=[f"Q{i:02d}" for i in range(1, len(_queries) + 1)],
)
def test_clickbench_matches_clickhouse_oracle(
    hits_table, ydb_pool, query, expected, request
):
    yql = parse_one(query, dialect="clickhouse").sql(dialect="ydb")
    result = ydb_pool.execute_with_retries(yql)
    assert_matches_clickhouse(result, expected, query_label=request.node.name)
