# ydb-sqlglot-plugin — project guide

sqlglot dialect plugin for bidirectional transpilation between YDB/YQL and any SQL dialect.

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
ydb_sqlglot/ydb.py        — the entire dialect (Tokenizer, Parser, Generator)
tests/unit/test_ydb.py    — test suite (unittest; pytest discovers it)
tt.py                     — scratch file for manual testing
```

## How the dialect works

sqlglot parses source SQL into an AST, then the YDB `Generator` walks the AST
and emits YQL. The three main customisation points:

| Layer | Class | Purpose |
|---|---|---|
| `Tokenizer` | `YDB.Tokenizer` | backtick identifiers, `$` as `PARAMETER` token, single/double-quoted strings |
| `Parser` | `YDB.Parser` | `$var`, `Module::Func()`, `DECLARE`, `FLATTEN BY`, `Optional<T>` / `T?`, container types, `ASSUME ORDER BY`, named exprs |
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
`BIGINT→Int64`, `INT→Int32`, `BOOLEAN→Uint8`, `VARCHAR→Utf8`, `BIT→Uint8`, etc.
YDB types `Utf8` and `String` are mapped at the Tokenizer level (`UTF8→TokenType.TEXT`, `STRING→TokenType.BLOB`) so they behave as standard SQL types and round-trip correctly to Postgres (`TEXT`/`BYTEA`) and ClickHouse (`String`).

### Important dialect flags

```python
SAFE_DIVISION = True      # prevents double-wrapping of denominators with NULLIF
NORMALIZE_FUNCTIONS = False  # function names are case-sensitive in YQL
NULL_ORDERING = "nulls_are_small"  # inherited from Dialect base
NULL_ORDERING_SUPPORTED = True     # generator can emit NULLS FIRST/LAST
```

## Parser — YDB-specific constructs

### `$varname` — dollar-sign variables
`$` tokenises as `TokenType.PARAMETER`; `VAR_SINGLE_TOKENS = {"$"}` makes `$foo` a two-token sequence. `PARAMETER_TOKEN = "$"` on the Generator restores `$` on output. `table_sql` detects `Table(this=Parameter(...))` and skips backtick-quoting.

### `Module::Function()` — colon-colon namespace
`COLUMN_OPERATORS[DCOLON]` is overridden to build `exp.Anonymous("Module::Func", args)` instead of triggering a Postgres-style CAST. `_parse_dcolon` parses the right-hand side as a function call.

### `DECLARE $p AS Type`
`STATEMENT_PARSERS[DECLARE]` calls `_parse_ydb_declare` → `_parse_ydb_declareitem`. `declareitem_sql` emits `$name AS Type`.

### `FLATTEN [LIST|DICT] BY col`
`_parse_table_alias` is overridden to prevent FLATTEN from being consumed as a table alias. `_parse_table` detects FLATTEN after the table ref and wraps it in the module-level `FlattenBy` expression. `FlattenBy` is registered in `TRANSFORMS`.

### Optional types `Optional<T>` / `T?`
`_parse_types` is overridden to handle both forms:
- `Optional<T>` — detected when `_curr.text == "Optional"` and next token is `<`; sets `nullable=True` on the inner `DataType`
- `T?` — detected as a trailing `?` (`TokenType.PLACEHOLDER`) after the base type

`datatype_sql` emits `Optional<T>` (not `T?`) for both forms.

### Container types `List<T>`, `Dict<K,V>`, `Set<T>`, `Tuple<T1,...>`
`_parse_types` detects `Name<...>` syntax for `List`, `Dict`, `Set`, `Tuple` (regardless of their token type — e.g. `Set` tokenizes as `TokenType.SET`).
- `List<T>` → `DataType(LIST, T)`
- `Dict<K,V>` → `DataType(MAP, K, V)`
- `Set<T>` → `DataType(SET, T)`
- `Tuple<T1,T2>` → `DataType(STRUCT, kind=Var("tuple"), T1, T2)` — the `kind` marker lets the YDB generator emit `Tuple<...>` while other dialects use their STRUCT representation

### `ASSUME ORDER BY`
`_parse_table_alias` blocks ASSUME from being consumed as alias. `_parse_query_modifiers` detects `ASSUME` followed by `ORDER_BY` token (single compound token) and wraps the order node in `AssumeOrderBy`. `assumeorderby_sql` prepends `ASSUME`.

### Named expressions `$name = expr`
`STATEMENT_PARSERS[PARAMETER]` calls `_parse_ydb_named_expr`. **Important:** `_match_set` already consumes `$` before the handler fires, so the retreat index is `self._index - 1` (to include `$`). If `=` doesn't follow, falls back to `_parse_expression()` which handles `$x` via `PLACEHOLDER_PARSERS[PARAMETER]`.

### CTE reassembly (YDB → other dialects)
`Parser.parse()` is overridden to call `_reassemble_ctes(statements)` after parsing. This post-processing pass converts sequences of `$name = (SELECT ...)` named expressions followed by a `SELECT` into a standard `WITH` CTE node, enabling round-trips to Postgres, ClickHouse, etc.

### Custom expression classes
`FlattenBy` and `AssumeOrderBy` are defined at module level (before `class YDB`). They must be registered in `Generator.TRANSFORMS` because `_build_dispatch` only looks up `exp.EXPR_CLASSES` for auto-dispatch — custom classes outside `sqlglot.expressions` won't be found otherwise.

---

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
| `TestYDBParser` | Parser round-trips for all YDB-specific constructs (`$var`, `::`, DECLARE, FLATTEN BY, `Optional<T>`, container types, ASSUME ORDER BY, named exprs) |
| `TestYDBToOther` | YDB → other dialects: CTE reassembly, type mapping (Int32/Int64, Optional, containers), module functions pass-through |

## Known pass-through (not transformed)

Dialect-specific functions that sqlglot does not parse into typed AST nodes are left as-is
and must be replaced manually. Common examples from ClickHouse:
`now()`, `today()`, `parseDateTimeBestEffort()`,
`toDate()`, `toFloat64()`, `toString()`, `countDistinct()`, `groupArray()`.
