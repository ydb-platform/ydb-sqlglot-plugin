# ydb-sqlglot-plugin

YDB dialect plugin for [sqlglot](https://github.com/tobymao/sqlglot) — transpiles SQL from any dialect into YDB/YQL.

## Installation

```bash
pip install ydb-sqlglot-plugin
```

## Usage

After installing the package, the `ydb` dialect is available in sqlglot automatically — no extra imports needed:

```python
import sqlglot

# Transpile from any dialect
result = sqlglot.transpile("SELECT * FROM users WHERE id = 1", read="mysql", write="ydb")[0]
# → SELECT * FROM `users` WHERE id = 1

# Or parse first, then generate
query = "SELECT * FROM orders WHERE user_id = 1"
parsed = sqlglot.parse_one(query, dialect="postgres")
yql = parsed.sql(dialect="ydb")
```

## What the plugin does

### Table names

Database-qualified names are rewritten to the YDB path format and wrapped in backticks:

```sql
-- input
SELECT * FROM analytics.events

-- output
SELECT * FROM `analytics/events`
```

### CTEs → YDB variables

```sql
-- input
WITH active AS (SELECT * FROM users WHERE status = 'active')
SELECT * FROM active

-- output
$active = (SELECT * FROM `users` WHERE status = 'active');

SELECT * FROM $active AS active
```

### Subquery decorrelation

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
| `ARRAY_FILTER(arr, x -> cond)` | `ListFilter(arr, ($x) -> {RETURN cond})` |
| `ARRAY_ANY(arr, x -> cond)` | `ListHasItems(ListFilter(arr, ($x) -> {RETURN cond}))` |
| `ARRAY_AGG(x)` | `AGGREGATE_LIST(x)` |
| `UNNEST(x)` | `FLATTEN BY x` |

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

| SQL type | YQL type |
|---|---|
| `TINYINT` | `INT8` |
| `SMALLINT` | `INT16` |
| `INT` / `INTEGER` | `INT32` |
| `BIGINT` | `INT64` |
| `FLOAT` | `Float` |
| `DOUBLE` / `DOUBLE PRECISION` | `Double` |
| `DECIMAL(p, s)` | `Decimal(p, s)` |
| `BOOLEAN` / `BIT` | `Uint8` |
| `TIMESTAMP` | `Timestamp` |
| `VARCHAR` / `NVARCHAR` / `CHAR` | `Utf8` |
| `TEXT` / `TINYTEXT` / `MEDIUMTEXT` / `LONGTEXT` | `Utf8` |
| `BLOB` / `TINYBLOB` / `MEDIUMBLOB` / `LONGBLOB` / `VARBINARY` | `String` |

---

## Limitations

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

---

## Development

```bash
git clone https://github.com/ydb-platform/ydb-sqlglot-plugin.git
cd ydb-sqlglot-plugin
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest tests/
```
