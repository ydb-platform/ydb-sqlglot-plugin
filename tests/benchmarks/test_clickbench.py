"""
ClickBench smoke tests.

Source queries: tests/fixtures/clickbench/queries.sql
  — 43 analytic queries from https://github.com/ClickHouse/ClickBench
  — ClickHouse dialect

Each test checks that the query can be transpiled to YDB without raising
an exception. Correctness of the generated YQL (i.e. whether it actually
runs on a live YDB instance) is not verified here.
"""

from pathlib import Path

import pytest
from sqlglot import parse_one

QUERIES_FILE = Path(__file__).parent.parent / "fixtures" / "clickbench" / "queries.sql"

_queries = [q.strip() for q in QUERIES_FILE.read_text().splitlines() if q.strip()]


@pytest.mark.parametrize(
    "query",
    _queries,
    ids=[f"Q{i:02d}" for i in range(1, len(_queries) + 1)],
)
def test_clickbench_transpiles(query: str) -> None:
    result = parse_one(query, dialect="clickhouse").sql(dialect="ydb")
    assert result
