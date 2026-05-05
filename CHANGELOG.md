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
