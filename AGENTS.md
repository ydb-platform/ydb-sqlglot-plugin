# ydb-sqlglot-plugin — project guide

sqlglot dialect plugin that transpiles SQL from any source dialect into YDB/YQL.

## Quick start

```bash
pip install -e .
python -m pytest tests/        # run all tests
```

Usage:
```python
import sqlglot

sql = parse_one(query, dialect="clickhouse").sql(dialect="ydb")
```

## Project layout

```
ydb_sqlglot/ydb.py   — the entire dialect (Tokenizer, Parser, Generator)
tests/test_ydb.py    — test suite (unittest; pytest discovers it)
tt.py                — scratch file for manual testing
```

## How the dialect works

sqlglot parses source SQL into an AST, then the YDB `Generator` walks the AST
and emits YQL. The three main customisation points:

| Layer | Class | Purpose |
|---|---|---|
| `Tokenizer` | `YDB.Tokenizer` | backtick identifiers, single/double-quoted strings |
| `Parser` | `YDB.Parser` | lambda `(x) -> {RETURN ...}` syntax, struct types |
| `Generator` | `YDB.Generator` | all SQL → YQL transformations (see below) |

### Key Generator behaviours

**CTE → YDB variables** (`_cte_to_lambda`)
```sql
-- input
WITH t AS (SELECT ...) SELECT * FROM t
-- output
$t = (SELECT ...);  SELECT * FROM $t AS t
```

**Table names** (`table_sql`)
`db.table` → `` `db/table` ``

**Function transforms** (TRANSFORMS dict + handler methods)
- `CONCAT(a, b)` → `a || b`
- `DATE_TRUNC('month', x)` → `DateTime::MakeDate(DateTime::StartOfMonth(x))`
- `EXTRACT(YEAR FROM x)` → `DateTime::GetYear(x)`
- `DATE_ADD/SUB` → `DateTime::ShiftMonths/Years` or `x ± DateTime::IntervalFrom*(n)`
- `INTERVAL n UNIT` → `DateTime::IntervalFromDays/Hours/Minutes/Seconds(n)`
- `dateDiff('unit', a, b)` → `(CAST(b AS Int64) - CAST(a AS Int64)) / factor`
- `COUNT()` (no args) → `COUNT(*)`
- `ROUND` → `Math::Round` (precision sign negated), `UPPER/LOWER` → `Unicode::ToUpper/ToLower`, `LENGTH` → `Unicode::GetLength`
- `NULLIF(x, y)` → `IF(x = y, NULL, x)`
- Lambda `x -> expr` → `($x) -> {RETURN expr}`

**Subquery unnesting** (`unnest_subqueries`)
Correlated and scalar subqueries are rewritten as LEFT/CROSS JOINs so YQL
can execute them (YQL does not support correlated subqueries natively).

**GROUP BY** (`_group_by`)
Columns in GROUP BY get `AS alias` to enable alias references in HAVING/SELECT.
The whole list is NOT wrapped in parentheses (unlike earlier versions).

**JOIN ON** (`join_sql`)
YQL only allows equality conditions in ON. Non-equality conditions are moved
to WHERE; joins without valid ON become CROSS JOINs.

**Type mapping** (TYPE_MAPPING)
`BIGINT→INT64`, `INT→INT32`, `BOOLEAN→Uint8`, `VARCHAR→Utf8`, `BIT→Uint8`, etc.

### Important dialect flags

```python
SAFE_DIVISION = True      # prevents double-wrapping of denominators with NULLIF
NORMALIZE_FUNCTIONS = False  # function names are case-sensitive in YQL
NULL_ORDERING = "nulls_are_small"  # inherited from Dialect base
NULL_ORDERING_SUPPORTED = True     # generator can emit NULLS FIRST/LAST
```

## Adding a new transform

1. Write a handler method on `Generator`:
   ```python
   def _my_func(self, expression: exp.MyFunc) -> str:
       arg = self.sql(expression, "this")
       return f"YQL::Equivalent({arg})"
   ```
2. Register it in the `TRANSFORMS` dict at the bottom of `Generator`:
   ```python
   exp.MyFunc: _my_func,
   ```
3. Add a test in `TestYDBTransforms` or `TestYDBFromClickHouse`.

## Test structure

| Class | What it covers |
|---|---|
| `TestYDBIdentity` | YDB SQL roundtrips (parse → generate → same string) |
| `TestYDBTransforms` | YDB-specific output: table quoting, CTEs, function transforms, subquery rewriting |
| `TestYDBFromClickHouse` | Source-dialect → YDB transpilation using ClickHouse syntax (Interval, dateDiff, COUNT(), GROUP BY) |

## Known pass-through (not transformed)

Dialect-specific functions that sqlglot does not parse into typed AST nodes are left as-is
and must be replaced manually. Common examples from ClickHouse:
`now()`, `today()`, `parseDateTimeBestEffort()`,
`toDate()`, `toFloat64()`, `toString()`, `countDistinct()`, `groupArray()`.
