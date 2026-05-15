# ydb-sqlglot-plugin

YDB dialect plugin for [sqlglot](https://github.com/tobymao/sqlglot) — bidirectional transpilation between YDB/YQL and any SQL dialect.

## Installation

```bash
pip install ydb-sqlglot-plugin
```

## Usage

After installing the package, the `ydb` dialect is available in sqlglot automatically — no extra imports needed:

```python
import sqlglot

# Any dialect → YDB
result = sqlglot.transpile("SELECT * FROM users WHERE id = 1", read="mysql", write="ydb")[0]
# → SELECT * FROM `users` WHERE id = 1

# YDB → any dialect
result = sqlglot.transpile("$t = (SELECT id FROM users); SELECT * FROM $t AS t", read="ydb", write="postgres")[0]
# → WITH t AS (SELECT id FROM users) SELECT * FROM t AS t
```

## What the plugin does

### YDB syntax documentation conformance

Source index: https://ydb.tech/docs/en/yql/reference/syntax/

This is a working checklist for doc-conformance work against YDB syntax
documentation. A checked item means `tests/unit/test_ydb.py` has focused tests
derived from that page's snippets, syntax variants, or documented negative
cases. An unchecked item is a backlog item.

- [x] [Lexical structure](https://ydb.tech/docs/en/yql/reference/syntax/lexer) - `test_lexer_doc_*`
- [x] [Expressions](https://ydb.tech/docs/en/yql/reference/syntax/expressions) - `test_expressions_doc_*`
- [x] [SELECT](https://ydb.tech/docs/en/yql/reference/syntax/select/) - all tracked SELECT subpages below
  - [x] [Overview](https://ydb.tech/docs/en/yql/reference/syntax/select/) - `test_select_overview_doc_*`
  - [x] [FROM](https://ydb.tech/docs/en/yql/reference/syntax/select/from) - `test_from_doc_snippets`
  - [x] [FROM AS_TABLE](https://ydb.tech/docs/en/yql/reference/syntax/select/from_as_table) - `test_from_as_table_doc_snippet`
  - [x] [FROM SELECT](https://ydb.tech/docs/en/yql/reference/syntax/select/from_select) - `test_from_select_doc_snippets`
  - [x] [FLATTEN](https://ydb.tech/docs/en/yql/reference/syntax/select/flatten) - `test_flatten_*_page_snippet_roundtrip_stable`
  - [x] [GROUP BY](https://ydb.tech/docs/en/yql/reference/syntax/select/group_by) - `test_group_by_doc_*`
  - [x] [JOIN](https://ydb.tech/docs/en/yql/reference/syntax/select/join) - `test_join_doc_*`
  - [x] [WINDOW](https://ydb.tech/docs/en/yql/reference/syntax/select/window) - `test_window_functions`, `test_window_doc_partition_compact_hint`
  - [x] [DISTINCT](https://ydb.tech/docs/en/yql/reference/syntax/select/distinct) - `test_distinct_doc_*`
  - [x] [UNIQUE DISTINCT](https://ydb.tech/docs/en/yql/reference/syntax/select/unique_distinct_hints) - `test_unique_distinct_hints`
  - [x] [UNION](https://ydb.tech/docs/en/yql/reference/syntax/select/union) - `test_union_doc_*`
  - [x] [VIEW secondary_index](https://ydb.tech/docs/en/yql/reference/syntax/select/secondary_index) - `test_secondary_index_doc_*`
  - [x] [VIEW vector_index](https://ydb.tech/docs/en/yql/reference/syntax/select/vector_index) - `test_vector_index_doc_*`
  - [x] [WITH](https://ydb.tech/docs/en/yql/reference/syntax/select/with) - `test_with_doc_*`
  - [x] [WITHOUT](https://ydb.tech/docs/en/yql/reference/syntax/select/without) - `test_without_doc_*`
  - [x] [WHERE](https://ydb.tech/docs/en/yql/reference/syntax/select/where) - `test_where_doc_filter_snippet`
  - [x] [ORDER BY](https://ydb.tech/docs/en/yql/reference/syntax/select/order_by) - `test_order_by_doc_*`
  - [x] [ASSUME ORDER BY](https://ydb.tech/docs/en/yql/reference/syntax/select/assume_order_by) - `test_assume_order_by_doc_*`
  - [x] [LIMIT OFFSET](https://ydb.tech/docs/en/yql/reference/syntax/select/limit_offset) - `test_limit_offset_doc_*`
  - [x] [SAMPLE / TABLESAMPLE](https://ydb.tech/docs/en/yql/reference/syntax/select/sample) - `test_sample_doc_*`
  - [x] [MATCH_RECOGNIZE](https://ydb.tech/docs/en/yql/reference/syntax/select/match_recognize) - `test_match_recognize_doc_*`
- [ ] [VALUES](https://ydb.tech/docs/en/yql/reference/syntax/values)
- [x] [CREATE TABLE](https://ydb.tech/docs/en/yql/reference/syntax/create_table/) - `test_create_table_doc_*`, `test_create_table_secondary_index_doc_*`, `test_create_table_family_doc_*`, TTL tests
- [ ] [DROP TABLE](https://ydb.tech/docs/en/yql/reference/syntax/drop_table)
- [x] [INSERT](https://ydb.tech/docs/en/yql/reference/syntax/insert_into) - `test_insert_into_doc_snippets`, `test_insert_into_external_file_doc_snippet` (skipped: external sources), pg→ydb DML coverage
- [ ] [ALTER TABLE](https://ydb.tech/docs/en/yql/reference/syntax/alter_table/)
- [x] [UPDATE](https://ydb.tech/docs/en/yql/reference/syntax/update) - `test_update_doc_*`, `test_update_on_doc_snippet`
- [ ] [DELETE](https://ydb.tech/docs/en/yql/reference/syntax/delete)
- [ ] [REPLACE](https://ydb.tech/docs/en/yql/reference/syntax/replace_into)
- [x] [UPSERT](https://ydb.tech/docs/en/yql/reference/syntax/upsert_into) - `test_upsert_into_doc_*`
- [ ] [ACTION](https://ydb.tech/docs/en/yql/reference/syntax/action)
- [ ] [INTO RESULT](https://ydb.tech/docs/en/yql/reference/syntax/into_result)
- [ ] [PRAGMA](https://ydb.tech/docs/en/yql/reference/syntax/pragma)
- [ ] [DECLARE](https://ydb.tech/docs/en/yql/reference/syntax/declare)
- [ ] [CREATE TOPIC](https://ydb.tech/docs/en/yql/reference/syntax/create-topic)
- [ ] [ALTER TOPIC](https://ydb.tech/docs/en/yql/reference/syntax/alter-topic)
- [ ] [DROP TOPIC](https://ydb.tech/docs/en/yql/reference/syntax/drop-topic)
- [ ] [CREATE ASYNC REPLICATION](https://ydb.tech/docs/en/yql/reference/syntax/create-async-replication)
- [ ] [ALTER ASYNC REPLICATION](https://ydb.tech/docs/en/yql/reference/syntax/alter-async-replication)
- [ ] [DROP ASYNC REPLICATION](https://ydb.tech/docs/en/yql/reference/syntax/drop-async-replication)
- [ ] [CREATE TRANSFER](https://ydb.tech/docs/en/yql/reference/syntax/create-transfer)
- [ ] [ALTER TRANSFER](https://ydb.tech/docs/en/yql/reference/syntax/alter-transfer)
- [ ] [DROP TRANSFER](https://ydb.tech/docs/en/yql/reference/syntax/drop-transfer)
- [ ] [COMMIT](https://ydb.tech/docs/en/yql/reference/syntax/commit)
- [ ] [CREATE VIEW](https://ydb.tech/docs/en/yql/reference/syntax/create-view)
- [ ] [ALTER VIEW](https://ydb.tech/docs/en/yql/reference/syntax/alter-view)
- [ ] [DROP VIEW](https://ydb.tech/docs/en/yql/reference/syntax/drop-view)
- [ ] [CREATE EXTERNAL DATA SOURCE](https://ydb.tech/docs/en/yql/reference/syntax/create-external-data-source)
- [ ] [CREATE EXTERNAL TABLE](https://ydb.tech/docs/en/yql/reference/syntax/create-external-table)
- [ ] [DROP EXTERNAL DATA SOURCE](https://ydb.tech/docs/en/yql/reference/syntax/drop-external-data-source)
- [ ] [DROP EXTERNAL TABLE](https://ydb.tech/docs/en/yql/reference/syntax/drop-external-table)
- [ ] [CREATE OBJECT (TYPE SECRET)](https://ydb.tech/docs/en/yql/reference/syntax/create-object-type-secret)
- [ ] [CREATE OBJECT (TYPE SECRET_ACCESS)](https://ydb.tech/docs/en/yql/reference/syntax/create-object-type-secret-access)
- [ ] [DROP OBJECT (TYPE SECRET)](https://ydb.tech/docs/en/yql/reference/syntax/drop-object-type-secret)
- [ ] [DROP OBJECT (TYPE SECRET_ACCESS)](https://ydb.tech/docs/en/yql/reference/syntax/drop-object-type-secret-access)
- [ ] [UPSERT OBJECT (TYPE SECRET)](https://ydb.tech/docs/en/yql/reference/syntax/upsert-object-type-secret)
- [ ] [CREATE RESOURCE POOL](https://ydb.tech/docs/en/yql/reference/syntax/create-resource-pool)
- [ ] [ALTER RESOURCE POOL](https://ydb.tech/docs/en/yql/reference/syntax/alter-resource-pool)
- [ ] [DROP RESOURCE POOL](https://ydb.tech/docs/en/yql/reference/syntax/drop-resource-pool)
- [ ] [CREATE RESOURCE POOL CLASSIFIER](https://ydb.tech/docs/en/yql/reference/syntax/create-resource-pool-classifier)
- [ ] [ALTER RESOURCE POOL CLASSIFIER](https://ydb.tech/docs/en/yql/reference/syntax/alter-resource-pool-classifier)
- [ ] [DROP RESOURCE POOL CLASSIFIER](https://ydb.tech/docs/en/yql/reference/syntax/drop-resource-pool-classifier)
- [ ] [CREATE USER](https://ydb.tech/docs/en/yql/reference/syntax/create-user)
- [ ] [ALTER USER](https://ydb.tech/docs/en/yql/reference/syntax/alter-user)
- [ ] [DROP USER](https://ydb.tech/docs/en/yql/reference/syntax/drop-user)
- [ ] [CREATE GROUP](https://ydb.tech/docs/en/yql/reference/syntax/create-group)
- [ ] [ALTER GROUP](https://ydb.tech/docs/en/yql/reference/syntax/alter-group)
- [ ] [DROP GROUP](https://ydb.tech/docs/en/yql/reference/syntax/drop-group)
- [ ] [GRANT](https://ydb.tech/docs/en/yql/reference/syntax/grant)
- [ ] [REVOKE](https://ydb.tech/docs/en/yql/reference/syntax/revoke)
- [ ] [Unsupported syntax](https://ydb.tech/docs/en/yql/reference/syntax/unsupported)

Unchecked pages may already have incidental parser support. They remain
unchecked until their specific documentation page has been used as the normative
source and focused tests have been added or corrected.

### Any SQL → YDB

#### Table names

Database-qualified names are rewritten to the YDB path format and wrapped in backticks:

```sql
-- input
SELECT * FROM analytics.events

-- output
SELECT * FROM `analytics/events`
```

#### CTEs → YDB variables

```sql
-- input
WITH active AS (SELECT * FROM users WHERE status = 'active')
SELECT * FROM active

-- output
$active = (SELECT * FROM `users` WHERE status = 'active');

SELECT * FROM $active AS active
```

#### Subquery decorrelation

Correlated subqueries (which YQL does not support) are rewritten as JOINs:

```sql
-- input
SELECT id, (SELECT MAX(amount) FROM orders WHERE orders.user_id = users.id) AS max_order
FROM users

-- output
SELECT users.id AS id, _u_0._u_2 AS max_order
FROM `users`
LEFT JOIN (
    SELECT MAX(amount) AS _u_2, user_id AS _u_1
    FROM `orders`
    WHERE TRUE
    GROUP BY user_id AS _u_1
) AS _u_0 ON users.id = _u_0._u_1
```

The same rewriting applies to `EXISTS`, `IN (subquery)`, and `ANY/ALL` subqueries.

#### GROUP BY aliases

YDB accepts aliases directly inside `GROUP BY` items. The generator uses this
form for grouped columns so later clauses and decorrelated subqueries can refer
to a stable grouping name:

```sql
-- input
SELECT user_id, COUNT(*) FROM events GROUP BY user_id

-- output
SELECT user_id, COUNT(*) FROM `events` GROUP BY user_id AS user_id
```

If a grouped column is selected under a generated alias, the `GROUP BY` item uses
that alias as well:

```sql
SELECT user_id AS _u_1, COUNT(*) FROM `events` GROUP BY user_id AS _u_1
```

Positional `GROUP BY` references are expanded before generation. When a
positional reference points to a constant expression, the grouping item is
removed because YDB rejects grouping by constants.

---

### YDB → any SQL

The plugin parses YDB/YQL back into sqlglot's AST, enabling round-trips, YDB-to-YDB transformations, and transpilation to other dialects.

#### Supported YQL constructs

| Construct | Example |
|---|---|
| `$variable` references | `SELECT * FROM $t AS t` |
| `Module::Function()` | `DateTime::GetYear(ts)` |
| `DECLARE $p AS Type` | `DECLARE $p AS Int32` |
| `FLATTEN [LIST\|DICT\|OPTIONAL] BY ...` / `FLATTEN COLUMNS` | `FROM t FLATTEN LIST BY col AS item`, `FROM t FLATTEN BY (a, b)`, `FROM t FLATTEN COLUMNS` |
| `Optional<T>` / `T?` | `CAST(x AS Optional<Utf8>)` |
| Container types | `CAST(x AS List<Int32>)`, `Dict<Utf8, Int64>`, `Set<Utf8>`, `Tuple<Int32, Utf8>` |
| `ASSUME ORDER BY` | `SELECT * FROM t ASSUME ORDER BY id` |
| `GROUP BY expr AS alias` / `GROUP COMPACT BY` | `SELECT v, COUNT(*) FROM t GROUP BY v AS v` |
| `LEFT ONLY JOIN` | `SELECT * FROM a LEFT ONLY JOIN b USING (id)` |
| `* WITHOUT (...)` projections | `SELECT b.* WITHOUT (b.id) FROM t AS b` |
| Named expressions | `$t = (SELECT 1 AS x)` |
| Lambda expressions | `($x, $y?) -> ($x + COALESCE($y, 0))`, `($y) -> { $p = "x"; RETURN $p \|\| $y }` |
| YQL struct literals | `AsList(<|user_id: "u1", description: NULL|>)` |
| `IN COMPACT` | `WHERE key IN COMPACT $values` |
| `PRAGMA` | `PRAGMA AnsiImplicitCrossJoin` |
| Table-valued functions | `SELECT * FROM AS_TABLE($Input) AS k` |
| Table source options and index views | ``FROM `t` WITH TabletId='...'``, ``FROM `t` VIEW PRIMARY KEY v`` |
| Function-valued expressions | `$grep(x)`, `DateTime::Format("%Y-%m-%d")(ts)`, `Interval("P7D")` |

Table names without backticks are accepted on input; the generator always produces backtick-quoted output.

The parser also tolerates case variants that appear in real YQL dumps, such as
`set<Utf8>`, `Tuple<Int32, Utf8>?`, and lowercase `return` in lambda blocks.

#### CTEs reassembly

YDB-style named expressions are automatically reassembled into standard `WITH` CTEs when targeting other dialects:

```python
ydb_sql = "$t = (SELECT 1 AS x); SELECT * FROM $t AS t"
parse_one(ydb_sql, dialect="ydb").sql(dialect="postgres")
# → WITH t AS (SELECT 1 AS x) SELECT * FROM t AS t
```

---

### Column lineage

Because YDB SQL is fully parsed into sqlglot's AST, column-level lineage works out of the box:

```python
from sqlglot.lineage import lineage

node = lineage("total", "$orders = (SELECT user_id, amount FROM orders); SELECT SUM(amount) AS total FROM $orders AS o", dialect="ydb")
for dep in node.walk():
    print(dep.name, "→", dep.source)
```

---

## Function reference

Functions below are recognized by sqlglot as standard SQL expressions and translated to their YQL equivalents. Dialect-specific functions that sqlglot does not parse into typed AST nodes are **passed through unchanged** — see [Limitations](#limitations).

### Date / time

| Input | YQL output |
|---|---|
| `DATE_TRUNC('day', x)` | `DATE(x)` |
| `DATE_TRUNC('week', x)` | `DateTime::MakeDate(DateTime::StartOfWeek(x))` |
| `DATE_TRUNC('month', x)` | `DateTime::MakeDate(DateTime::StartOfMonth(x))` |
| `DATE_TRUNC('quarter', x)` | `DateTime::MakeDate(DateTime::StartOfQuarter(x))` |
| `DATE_TRUNC('year', x)` | `DateTime::MakeDate(DateTime::StartOfYear(x))` |
| `EXTRACT(WEEK FROM x)` | `DateTime::GetWeekOfYear(x)` |
| `EXTRACT(MONTH FROM x)` | `DateTime::GetMonth(x)` |
| `EXTRACT(YEAR FROM x)` | `DateTime::GetYear(x)` |
| `CURRENT_TIMESTAMP` | `CurrentUtcTimestamp()` |
| `STR_TO_DATE(str, fmt)` / `TO_DATE(str, fmt)` | `DateTime::MakeTimestamp(DateTime::Parse(fmt)(str))` |
| `DATE_ADD(x, INTERVAL n MONTH)` | `DateTime::MakeDate(DateTime::ShiftMonths(x, n))` |
| `DATE_ADD(x, INTERVAL n YEAR)` | `DateTime::MakeDate(DateTime::ShiftYears(x, n))` |
| `DATE_ADD(x, INTERVAL n DAY)` | `x + DateTime::IntervalFromDays(n)` |
| `DATE_ADD(x, INTERVAL n HOUR)` | `x + DateTime::IntervalFromHours(n)` |
| `DATE_ADD(x, INTERVAL n MINUTE)` | `x + DateTime::IntervalFromMinutes(n)` |
| `DATE_ADD(x, INTERVAL n SECOND)` | `x + DateTime::IntervalFromSeconds(n)` |
| `DATE_SUB(x, INTERVAL n ...)` | same as `DATE_ADD` with `−` |
| `INTERVAL n DAY` (literal) | `DateTime::IntervalFromDays(n)` |
| `INTERVAL n HOUR` (literal) | `DateTime::IntervalFromHours(n)` |
| `INTERVAL n MINUTE` (literal) | `DateTime::IntervalFromMinutes(n)` |
| `INTERVAL n SECOND` (literal) | `DateTime::IntervalFromSeconds(n)` |
| `Interval("P7D")` (YQL input) | passed through unchanged |
| `dateDiff('minute', a, b)` | `(CAST(b AS Int64) - CAST(a AS Int64)) / 60000000` |
| `dateDiff('hour', a, b)` | `(CAST(b AS Int64) - CAST(a AS Int64)) / 3600000000` |
| `dateDiff('day', a, b)` | `(CAST(b AS Int64) - CAST(a AS Int64)) / 86400000000` |
| `dateDiff('week', a, b)` | `(CAST(b AS Int64) - CAST(a AS Int64)) / 604800000000` |

> **Note on `dateDiff`:** YDB stores `Timestamp` as microseconds since epoch. The formula above gives exact integer units assuming both arguments are `Timestamp`. Results for `Date`-typed columns will differ.

### Strings

| Input | YQL output |
|---|---|
| `CONCAT(a, b, ...)` | `a \|\| b \|\| ...` |
| `UPPER(x)` | `Unicode::ToUpper(x)` |
| `LOWER(x)` | `Unicode::ToLower(x)` |
| `LENGTH(x)` / `CHAR_LENGTH(x)` | `Unicode::GetLength(x)` |
| `POSITION(sub IN x)` / `STRPOS(x, sub)` | `Find(x, sub)` |
| `STRING_TO_ARRAY(x, delim)` | `String::SplitToList(x, delim)` |
| `ARRAY_TO_STRING(arr, delim)` | `String::JoinFromList(arr, delim)` |

### Arrays / collections

| Input | YQL output |
|---|---|
| `ARRAY(v1, v2, ...)` | `AsList(v1, v2, ...)` |
| `ARRAY_LENGTH(x)` / `ARRAY_SIZE(x)` | `ListLength(x)` |
| `ARRAY_FILTER(arr, x -> cond)` | `ListFilter(arr, ($x) -> (cond))` |
| `ARRAY_ANY(arr, x -> cond)` | `ListHasItems(ListFilter(arr, ($x) -> (cond)))` |
| `ARRAY_AGG(x)` | `AGGREGATE_LIST(x)` |
| `UNNEST(x)` | `FLATTEN BY x` |

Lambda expressions are represented with sqlglot's standard `exp.Lambda` AST node.
When a source dialect parses lambdas, the YDB generator emits YQL lambda syntax:

```sql
-- DuckDB input
SELECT list_filter(arr, x -> x > 0) FROM t

-- YDB output
SELECT ListFilter(arr, ($x) -> ($x > 0)) FROM `t`
```

YDB input also supports documented YQL lambda forms, including optional
arguments and block bodies with local named expressions:

```sql
($x, $y?) -> ($x + COALESCE($y, 0));
($y) -> { $prefix = "x"; RETURN $prefix || $y; };
```

ClickHouse `ARRAY JOIN` and simple `arrayJoin(...)` projections, and PostgreSQL
`LATERAL unnest(...)`, are converted to YDB `FLATTEN BY` when the operation is
directly tied to the source table.

### Conditional / math

| Input | YQL output |
|---|---|
| `NULLIF(x, y)` | `IF(x = y, NULL, x)` |
| `ROUND(x, n)` | `Math::Round(x, -n)` |
| `COUNT()` *(zero-argument form)* | `COUNT(*)` |

### JSON

| Input | YQL output |
|---|---|
| `jsonb_col @> value` (PostgreSQL) | `Yson::Contains(jsonb_col, value)` |

YDB JSON functions are parsed and round-tripped, including `PASSING`,
`RETURNING`, wrapper modes, and `ON EMPTY` / `ON ERROR` clauses:

```sql
JSON_VALUE(payload, '$.value + $delta' PASSING 1 AS delta RETURNING Int64 DEFAULT 0 ON EMPTY ERROR ON ERROR)
JSON_QUERY(payload, '$.items' WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY ERROR ON ERROR)
JSON_EXISTS(payload, '$.items[$Index]' PASSING 0 AS "Index" FALSE ON ERROR)
```

JSON paths can contain quoted keys, for example
`JSON_EXISTS(item_result, "$.'P_008 device playback test'")`.

---

## Type mapping

### Standard SQL → YDB

| SQL type | YDB type |
|---|---|
| `TINYINT` | `Int8` |
| `SMALLINT` | `Int16` |
| `INT` / `INTEGER` | `Int32` |
| `BIGINT` | `Int64` |
| `FLOAT` | `Float` |
| `DOUBLE` / `DOUBLE PRECISION` | `Double` |
| `DECIMAL(p, s)` | `Decimal(p, s)` |
| `BOOLEAN` / `BIT` | `Uint8` |
| `TIMESTAMP` | `Timestamp` |
| `VARCHAR` / `NVARCHAR` / `CHAR` / `TEXT` | `Utf8` |
| `BLOB` / `BINARY` / `VARBINARY` | `String` |

### YDB types → standard SQL

| YDB type | Standard SQL | Postgres | ClickHouse |
|---|---|---|---|
| `Utf8` | `TEXT` | `TEXT` | `String` |
| `String` | `BLOB` | `BYTEA` | `String` |
| `Int32` | `INT` | `INT` | `Int32` |
| `Int64` | `BIGINT` | `BIGINT` | `Int64` |
| `Optional<T>` | `T` (nullable) | `T` | `Nullable(T)` |
| `List<T>` | `LIST<T>` | `LIST<T>` | `Array(T)` |
| `Dict<K,V>` | `MAP<K,V>` | `MAP<K,V>` | `Map(K,V)` |
| `Tuple<T1,T2>` | `STRUCT<...>` | `STRUCT<...>` | `Tuple(T1,T2)` |

---

## Limitations

### Dialect-specific functions

Functions that sqlglot does not parse into typed AST nodes are passed through unchanged and must be replaced manually. Common examples from ClickHouse: `now()`, `today()`, `parseDateTimeBestEffort()`, `toDate()`, `toFloat64()`, `toString()`, `countDistinct()`, `groupArray()`.

### Correlated subqueries in DML

Correlated subqueries inside `UPDATE` or `INSERT` statements cannot be automatically decorrelated — YDB does not support them natively, and rewriting requires knowledge of the table's primary key. Rewrite manually using a `$variable`:

```sql
-- not supported (will raise an error)
UPDATE t SET col = (SELECT val FROM other WHERE other.id = t.id)

-- workaround
$vals = (SELECT id, val FROM other);
UPDATE t SET col = (SELECT val FROM $vals WHERE id = t.id)
```

Correlated subqueries inside `SELECT` are handled automatically via JOIN rewriting.

### `dateDiff` with month granularity

`dateDiff('month', a, b)` has no exact equivalent in YDB because months have variable length. Use `DateTime::ShiftMonths` for date arithmetic instead.

### YDB container types in other dialects

`Uint8`/`Uint16`/`Uint32`/`Uint64` and YDB-specific container types (`Struct<...>`, `Variant<...>`, `Enum<...>`) do not have direct equivalents in standard SQL and are passed through as-is when targeting other dialects.

---

## Development

```bash
git clone https://github.com/ydb-platform/ydb-sqlglot-plugin.git
cd ydb-sqlglot-plugin
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest tests/
```
