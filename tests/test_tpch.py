"""
TPC-H smoke tests.

Source queries: tests/fixtures/tpch/queries.sql
  — 22 standard TPC-H queries from sqlglot's optimizer test fixtures (MIT)
  — PostgreSQL dialect

Each test checks that the query can be transpiled to YDB without raising
an exception. Correctness of the generated YQL (i.e. whether it actually
runs on a live YDB instance) is not verified here.
"""

from pathlib import Path

import pytest
from sqlglot import parse_one

QUERIES_FILE = Path(__file__).parent / "fixtures" / "tpch" / "queries.sql"

_queries = [q.strip() for q in QUERIES_FILE.read_text().split("---") if q.strip()]


@pytest.mark.parametrize(
    "query",
    _queries,
    ids=[f"Q{i:02d}" for i in range(1, len(_queries) + 1)],
)
def test_tpch_transpiles(query: str) -> None:
    result = parse_one(query, dialect="postgres").sql(dialect="ydb")
    assert result
