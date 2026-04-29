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
| `FLATTEN [LIST\|DICT] BY col` | `FROM t FLATTEN LIST BY col` |
| `Optional<T>` / `T?` | `CAST(x AS Optional<Utf8>)` |
| Container types | `CAST(x AS List<Int32>)`, `Dict<Utf8, Int64>`, `Set<Utf8>`, `Tuple<Int32, Utf8>` |
| `ASSUME ORDER BY` | `SELECT * FROM t ASSUME ORDER BY id` |
| `GROUP BY expr AS alias` | `SELECT v, COUNT(*) FROM t GROUP BY v AS v` |
| Named expressions | `$t = (SELECT 1 AS x)` |
| Lambda expressions | `($x, $y?) -> ($x + COALESCE($y, 0))`, `($y) -> { $p = "x"; RETURN $p \|\| $y }` |
| `IN COMPACT` | `WHERE key IN COMPACT $values` |
| `PRAGMA` | `PRAGMA AnsiImplicitCrossJoin` |

Table names without backticks are accepted on input; the generator always produces backtick-quoted output.

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
