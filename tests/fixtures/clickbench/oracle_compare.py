"""
Expected ClickHouse oracle rows for the small ClickBench fixture (5 rows) and
comparison helpers for YDB integration tests.

Oracle source: ClickHouse 24.x with create_clickhouse.sql / insert_clickhouse.sql
(same data as create.sql / insert.sql for YDB).
"""

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any, Sequence

_Q30 = [[1920 + 5 * i for i in range(90)]]

CLICKBENCH_EXPECTED: list[list[list[object]] | None] = [
    [[5]],
    [[2]],
    [[3, 5, 384]],
    [[300]],
    [[5]],
    [[3]],
    [["2013-06-01", "2013-07-20"]],
    [[1, 1], [2, 1]],
    [[0, 3], [1, 1], [225, 1]],
    [[0, 2, 3, 0, 3], [1, 0, 1, 0, 1], [225, 1, 1, 1920, 1]],
    [["Samsung", 1], ["iPhone", 1]],
    [[0, "iPhone", 1], [1, "Samsung", 1]],
    [["test query", 1], ["another query", 1]],
    [["test query", 1], ["another query", 1]],
    [[0, "another query", 1], [0, "test query", 1]],
    [[400, 1], [300, 1], [500, 1], [100, 1], [200, 1]],
    [[500, "", 1], [100, "test query", 1], [300, "another query", 1], [400, "", 1], [200, "", 1]],
    [[500, "", 1], [100, "test query", 1], [300, "another query", 1], [400, "", 1], [200, "", 1]],
    [[100, 0, "test query", 1], [200, 0, "", 1], [400, 0, "", 1], [300, 0, "another query", 1], [500, 0, "", 1]],
    [],
    [[3]],
    [["test query", "https://www.google.com/search?q=test", 1], ["another query", "https://google.com", 1]],
    [],
    None,
    [["another query"], ["test query"]],
    [["another query"], ["test query"]],
    [["another query"], ["test query"]],
    [],
    [],
    _Q30,
    [[0, 0, 2, 0, 960]],
    [[3, 0, 1, 0, 0], [1, 0, 1, 0, 1920]],
    [[5, 0, 1, 0, 0], [2, 0, 1, 1, 0], [3, 0, 1, 0, 0], [1, 0, 1, 0, 1920], [4, 0, 1, 0, 0]],
    [
        ["https://news.google.com", 1],
        ["https://www.example.org", 1],
        ["https://google.com", 1],
        ["https://example.com", 1],
        ["https://www.google.com/search?q=test", 1],
    ],
    [
        [1, "https://news.google.com", 1],
        [1, "https://www.example.org", 1],
        [1, "https://google.com", 1],
        [1, "https://example.com", 1],
        [1, "https://www.google.com/search?q=test", 1],
    ],
    [[0, -1, -2, -3, 5]],
    [
        ["https://news.google.com", 1],
        ["https://www.example.org", 1],
        ["https://www.google.com/search?q=test", 1],
    ],
    [["Google News", 1]],
    [],
    [],
    [],
    [],
    [],
]

assert len(CLICKBENCH_EXPECTED) == 43

Q24_KEY_ROWS = [
    (1, 100, "https://www.google.com/search?q=test"),
    (3, 300, "https://google.com"),
    (5, 500, "https://news.google.com"),
]


def _normalize_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        return v.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, float):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return v


def _materialize_ydb_row(row: Any) -> tuple[Any, ...]:
    if hasattr(row, "__len__") and not isinstance(row, (str, bytes)):
        n = len(row)
        return tuple(_normalize_cell(row[i]) for i in range(n))
    raise TypeError(f"unexpected row type: {type(row)}")


def _floaty(a: Any, b: Any) -> bool:
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return math.isclose(fa, fb, rel_tol=0.0, abs_tol=1e-4)


def _row_almost_equal(a: Sequence[Any], b: Sequence[Any]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if x == y:
            continue
        if _floaty(x, y):
            continue
        if str(x) == str(y):
            continue
        return False
    return True


def _multiset_equal(expected: list[list[Any]], actual_rows: Sequence[Any]) -> None:
    act = [_materialize_ydb_row(r) for r in actual_rows]
    exp = [tuple(_normalize_cell(c) for c in row) for row in expected]
    if len(exp) != len(act):
        raise AssertionError(f"row count: expected {len(exp)}, got {len(act)}")
    used = [False] * len(act)
    for er in exp:
        found = False
        for j, ar in enumerate(act):
            if used[j]:
                continue
            if _row_almost_equal(er, ar):
                used[j] = True
                found = True
                break
        if not found:
            raise AssertionError(f"no matching row for expected {er!r}; actual {act!r}")
    if not all(used):
        raise AssertionError(f"extra actual rows: {act!r}")


def assert_matches_clickhouse(
    result_sets: Sequence[Any],
    expected: list[list[Any]] | None,
    *,
    query_label: str,
) -> None:
    if result_sets is None or len(result_sets) == 0:
        raise AssertionError(f"{query_label}: empty result")
    rows = result_sets[0].rows
    if expected is None:
        _assert_q24_select_star(rows, query_label)
        return
    if len(expected) == 0:
        assert len(rows) == 0, f"{query_label}: expected 0 rows, got {len(rows)}"
        return
    _multiset_equal(expected, rows)


def _assert_q24_select_star(rows: Sequence[Any], query_label: str) -> None:
    if len(rows) != 3:
        raise AssertionError(f"{query_label}: expected 3 rows, got {len(rows)}")
    triples = []
    for r in rows:
        # YDB COLUMN store returns SELECT * in alphabetical column order,
        # so use named access instead of positional.
        try:
            watch_id = int(_normalize_cell(r["WatchID"]))
            user_id = int(_normalize_cell(r["UserID"]))
            url = str(_normalize_cell(r["URL"]))
        except (KeyError, TypeError):
            # Fallback to positional (should not happen for a COLUMN store hits table)
            t = _materialize_ydb_row(r)
            if len(t) < 14:
                raise AssertionError(f"{query_label}: row too short: {len(t)} cols")
            watch_id, user_id, url = int(t[0]), int(t[9]), str(t[13])
        triples.append((watch_id, user_id, url))
    got = sorted(triples)
    want = sorted(Q24_KEY_ROWS)
    if got != want:
        raise AssertionError(f"{query_label}: Q24 key triples {got!r} != {want!r}")
