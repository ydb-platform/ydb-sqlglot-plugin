"""
TPC-H integration tests — require a live YDB instance.

Start with:  docker compose up -d

Each test transpiles a TPC-H query (PostgreSQL → YDB), runs it on the small
fixture dataset, and asserts row-level parity with PostgreSQL oracle results
stored in tests/fixtures/tpch/oracle_compare.py.

Queries with known transpilation limitations are marked xfail.  See
tests/fixtures/tpch/oracle_compare.py for the full list and root causes.
"""

from pathlib import Path

import pytest
from sqlglot import parse_one

from tests.fixtures.tpch.oracle_compare import (
    TPCH_EXPECTED,
    assert_matches_postgres,
)

FIXTURES = Path(__file__).parent / "fixtures" / "tpch"
QUERIES_FILE = FIXTURES / "queries.sql"

_queries = [q.strip() for q in QUERIES_FILE.read_text().split("---") if q.strip()]

assert len(_queries) == len(TPCH_EXPECTED) == 22

# Q numbers (1-based) with known transpilation bugs
_XFAIL_QS = {2, 4, 11, 13, 16, 17, 18, 20, 21, 22}


@pytest.fixture(scope="module")
def tpch_tables(ydb_pool):
    """Create and populate TPC-H tables, drop them after the module."""
    for table in ["lineitem", "partsupp", "orders", "customer",
                  "part", "supplier", "nation", "region"]:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")

    ydb_pool.execute_with_retries((FIXTURES / "create.sql").read_text())
    ydb_pool.execute_with_retries((FIXTURES / "insert.sql").read_text())

    yield

    for table in ["lineitem", "partsupp", "orders", "customer",
                  "part", "supplier", "nation", "region"]:
        ydb_pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table}`")


@pytest.mark.parametrize(
    ("query", "expected"),
    list(zip(_queries, TPCH_EXPECTED)),
    ids=[f"Q{i:02d}" for i in range(1, 23)],
)
def test_tpch_matches_postgres_oracle(
    tpch_tables, ydb_pool, query, expected, request
):
    q_num = int(request.node.name.split("[Q")[1].rstrip("]"))
    if q_num in _XFAIL_QS:
        pytest.xfail(f"Q{q_num:02d}: known transpilation issue")

    yql = parse_one(query, dialect="postgres").sql(dialect="ydb")
    result = ydb_pool.execute_with_retries(yql)
    assert_matches_postgres(result, expected, query_label=request.node.name)
