* Add official YDB syntax documentation conformance tests for FLATTEN, GROUP BY, JOIN, DISTINCT, WITH, WITHOUT, ORDER BY, SAMPLE/TABLESAMPLE, MATCH_RECOGNIZE, lexical structure, expressions, and CREATE TABLE
* Extend YDB parser/generator support for documented SELECT syntax including FLATTEN variants, GROUP BY aliases/windows/extensions, JOIN variants, DISTINCT rewrites, WITH table options, WITHOUT projections, ORDER BY limits, TABLESAMPLE/SAMPLE, MATCH_RECOGNIZE, and secondary/vector index views
* Add lexer and expression coverage for comments, identifiers, string and numeric literals, operators, predicates, casts, named expressions, table expressions, lambda functions, and container access
* Add CREATE TABLE support for documented row/column store syntax, partitioning, secondary indexes, column families, and TTL options
* Add YDB syntax documentation conformance coverage checklist to README
* Validate remaining SELECT documentation pages, including FROM variants, AS_TABLE, WINDOW, vector indexes, and multi-table functions
* Support YDB UPDATE ON and UPSERT INTO parsing/generation
* Preserve YDB table functions in FROM such as CONCAT, LIKE, FILTER, and qualified RANGE
* Improve PostgreSQL to YDB transpilation coverage for core SELECT, CTE, DML, date/time, string, array, and JSONB constructs
* Map PostgreSQL DATE_TRUNC, plural INTERVAL units, and JSONB containment to YDB-compatible output

## 0.2.4 ##
* Parse YQL Interval(...) calls, struct literals, leading BOM tokens, and lowercase lambda RETURN blocks
* Support GROUP COMPACT BY, LEFT ONLY JOIN, YDB WITHOUT projections, escaped double-quoted strings, and optional generic type shorthand
* Handle dollar-parameter table references and table-attached joins more robustly, and avoid decorrelating set-operation subqueries
* Parse quoted type names in DECLARE statements
* Preserve unparenthesized table source options such as WITH TabletId='...'
* Generate helper SQL with the YDB dialect when comparing expressions, so custom nodes like JSON_VALUE work inside GROUP BY rewrites
* Parse YDB numeric literals with hex and unsigned/long suffixes
* Preserve table source options and VIEW PRIMARY KEY aliases
* Extend FLATTEN parsing for OPTIONAL, COLUMNS, grouped expressions, aliases,
and named expressions
* Support parameter-style function calls such as $Func(...)
* Keep derived table scopes out of subquery decorrelation
* Preserve parenthesized equality predicates in JOIN ON
* Convert ClickHouse ARRAY JOIN/arrayJoin and Postgres LATERAL unnest to
YDB FLATTEN BY
* Fix nested lambda assignments in lambda blocks and add a parser progress guard

## 0.2.3 ##
* Fix function variables
* Lambdas support
* Fix DECLARE comments and empty statements
* Structs support
* Fix group by aliases round trip and other fixes

## 0.2.2 ##
* Fix backward compat with older sqlglot versions

## 0.2.1 ##
* Remove AnsiImplicitCrossJoin pragma

## 0.2.0 ##
* Basic parser

## 0.1.1 ##
* Integration tests and small fixes
* Clickbench validation & TPC-H validation draft

## 0.1.0 ##
* sqlglot plugin for YDB
