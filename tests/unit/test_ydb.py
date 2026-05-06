import unittest

from sqlglot import ErrorLevel, ParseError, UnsupportedError, parse, parse_one
from sqlglot.parser import logger as parser_logger

from ydb_sqlglot.ydb import eliminate_join_marks, make_db_name_lower, table_names_to_lower_case

# ---------------------------------------------------------------------------
# Base validator (sqlglot convention)
# ---------------------------------------------------------------------------

class Validator(unittest.TestCase):
    dialect = None

    def parse_one(self, sql, **kwargs):
        return parse_one(sql, read=self.dialect, **kwargs)

    def validate_identity(self, sql, write_sql=None, pretty=False, check_command_warning=False):
        if check_command_warning:
            with self.assertLogs(parser_logger) as cm:
                expression = self.parse_one(sql)
                assert f"'{sql[:100]}' contains unsupported syntax" in cm.output[0]
        else:
            expression = self.parse_one(sql)
        self.assertEqual(
            write_sql or sql,
            expression.sql(dialect=self.dialect, pretty=pretty),
        )
        return expression

    def validate_transpile(self, sql, expected, read=None):
        result = parse_one(sql, read=read or self.dialect).sql(dialect=self.dialect)
        self.assertEqual(expected, result)

    def validate_all(self, sql, read=None, write=None, pretty=False):
        expression = self.parse_one(sql)

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                self.assertEqual(
                    parse_one(read_sql, read_dialect).sql(
                        self.dialect,
                        unsupported_level=ErrorLevel.IGNORE,
                        pretty=pretty,
                    ),
                    sql,
                )

        for write_dialect, write_sql in (write or {}).items():
            with self.subTest(f"{sql} -> {write_dialect}"):
                if write_sql is UnsupportedError:
                    with self.assertRaises(UnsupportedError):
                        expression.sql(write_dialect, unsupported_level=ErrorLevel.RAISE)
                else:
                    self.assertEqual(
                        expression.sql(write_dialect, unsupported_level=ErrorLevel.IGNORE, pretty=pretty),
                        write_sql,
                    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ydb(sql, read=None):
    """Parse *sql* (optionally with *read* dialect) and generate YDB SQL."""
    return parse_one(sql, dialect=read).sql(dialect="ydb")


# ---------------------------------------------------------------------------
# YDB identity: we parse YDB SQL and expect to get the same string back.
# This confirms the dialect supports these constructs without mangling them.
# ---------------------------------------------------------------------------

class TestYDBIdentity(Validator):
    """Round-trip tests: YDB → parse → generate → same YDB."""

    maxDiff = None
    dialect = "ydb"

    def test_select_basics(self):
        cases = [
            "SELECT * FROM `table`",
            "SELECT DISTINCT id FROM `table`",
            "SELECT * FROM `table` WHERE id = 1",
            "SELECT * FROM `table` ORDER BY id DESC",
            "SELECT * FROM `table` LIMIT 10",
            "SELECT * FROM `table` LIMIT 10 OFFSET 5",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_unique_distinct_hints(self):
        cases = [
            "SELECT /*+ unique */ id FROM `table`",
            "SELECT /*+ distinct */ id FROM `table`",
            "SELECT /*+ unique(id category) */ id, category FROM `table`",
            "SELECT /*+ distinct(id category) */ id, category FROM `table`",
            "SELECT /*+ unique(id category) distinct(source_id) */ id, category, source_id FROM `table`",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_joins(self):
        cases = [
            "SELECT * FROM `a` INNER JOIN `b` ON a.id = b.id",
            "SELECT * FROM `a` LEFT JOIN `b` ON a.id = b.id",
            "SELECT * FROM `a` RIGHT JOIN `b` ON a.id = b.id",
            "SELECT * FROM `a` FULL JOIN `b` ON a.id = b.id",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)
        # Implicit comma joins are normalised to explicit CROSS JOIN.
        self.validate_identity(
            "SELECT * FROM `a`, `b`",
            write_sql="SELECT * FROM `a` CROSS JOIN `b`",
        )

    def test_set_operations(self):
        cases = [
            "SELECT * FROM `a` UNION SELECT * FROM `b`",
            "SELECT * FROM `a` UNION ALL SELECT * FROM `b`",
            "SELECT * FROM `a` INTERSECT SELECT * FROM `b`",
            "SELECT * FROM `a` EXCEPT SELECT * FROM `b`",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_union_doc_examples(self):
        self.validate_identity(
            "SELECT key FROM `T1` UNION SELECT key FROM `T2`",
        )
        self.validate_identity(
            "SELECT 1 AS x UNION ALL SELECT 2 AS y UNION ALL SELECT 3 AS z",
        )

    def test_union_doc_positional_union_all_pragma_example(self):
        sql = (
            "PRAGMA PositionalUnionAll;\n\n"
            "SELECT 1 AS x, 2 AS y\n"
            "UNION ALL\n"
            "SELECT * FROM AS_TABLE([<|x:3, y:4|>])"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "PRAGMA PositionalUnionAll;\n"
            "SELECT 1 AS x, 2 AS y UNION ALL SELECT * FROM AS_TABLE(AsList(<|x: 3, y: 4|>))",
            generated,
        )

    def test_without_doc_exclude_columns_snippet(self):
        self.validate_identity(
            "SELECT * WITHOUT foo, bar FROM `my_table`",
            write_sql="SELECT * WITHOUT (foo, bar) FROM `my_table`",
        )

    def test_without_doc_simplecolumns_qualified_snippet(self):
        sql = (
            "PRAGMA simplecolumns;\n"
            "SELECT * WITHOUT t.foo FROM my_table AS t\n"
            "CROSS JOIN (SELECT 1 AS foo) AS v"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "PRAGMA simplecolumns;\n"
            "SELECT * WITHOUT (t.foo) FROM `my_table` AS t CROSS JOIN (SELECT 1 AS foo) AS v",
            generated,
        )

    def test_expressions(self):
        cases = [
            "SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM `table`",
            "SELECT COALESCE(name, 'unknown') FROM `table`",
            "SELECT IF(value = 0, NULL, value) FROM `table`",
            "SELECT CAST(id AS Utf8) FROM `table`",
            "SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM `table`",
            "SELECT a + b, a - b, a * b, a / b FROM `table`",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_predicates(self):
        cases = [
            "SELECT * FROM `table` WHERE name LIKE 'test%'",
            "SELECT * FROM `table` WHERE id BETWEEN 1 AND 10",
            "SELECT * FROM `table` WHERE name IS NULL",
            "SELECT * FROM `table` WHERE a > 0 AND b < 10 OR c = 5",
            "SELECT * FROM `table` WHERE a = b AND a <> c AND a > d AND a < e AND a >= f AND a <= g",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_where_doc_filter_snippet(self):
        self.validate_identity(
            "SELECT key FROM my_table WHERE value > 0",
            write_sql="SELECT key FROM `my_table` WHERE value > 0",
        )

    def test_order_by_doc_sorting_criteria_snippet(self):
        self.validate_identity(
            "SELECT key, string_column FROM my_table ORDER BY key DESC, LENGTH(string_column) ASC",
            write_sql=(
                "SELECT key, string_column FROM `my_table` "
                "ORDER BY key DESC, Unicode::GetLength(string_column) ASC"
            ),
        )

    def test_order_by_doc_rejects_column_sequence_number(self):
        with self.assertRaises(UnsupportedError):
            parse_one("SELECT key, string_column FROM my_table ORDER BY 1", dialect="ydb").sql(dialect="ydb")

    def test_limit_offset_doc_limit_snippet(self):
        self.validate_identity(
            "SELECT key FROM my_table LIMIT 7",
            write_sql="SELECT key FROM `my_table` LIMIT 7",
        )

    def test_limit_offset_doc_limit_offset_snippet(self):
        self.validate_identity(
            "SELECT key FROM my_table LIMIT 7 OFFSET 3",
            write_sql="SELECT key FROM `my_table` LIMIT 7 OFFSET 3",
        )

    def test_limit_offset_doc_comma_form_snippet(self):
        self.validate_identity(
            "SELECT key FROM my_table LIMIT 3, 7",
            write_sql="SELECT key FROM `my_table` LIMIT 7 OFFSET 3",
        )

    def test_sample_doc_tablesample_bernoulli_repeatable_snippet(self):
        self.validate_identity(
            "SELECT * FROM my_table TABLESAMPLE BERNOULLI(1.0) REPEATABLE(123)",
            write_sql="SELECT * FROM `my_table` TABLESAMPLE BERNOULLI(1.0) REPEATABLE(123)",
        )

    def test_sample_doc_tablesample_system_snippet(self):
        self.validate_identity(
            "SELECT * FROM my_table TABLESAMPLE SYSTEM(1.0)",
            write_sql="SELECT * FROM `my_table` TABLESAMPLE SYSTEM(1.0)",
        )

    def test_sample_doc_sample_fraction_snippet(self):
        self.validate_identity(
            "SELECT * FROM my_table SAMPLE 1.0 / 3",
            write_sql="SELECT * FROM `my_table` SAMPLE 1.0 / 3",
        )

    def test_match_recognize_doc_usage_snippet(self):
        sql = (
            'PRAGMA FeatureR010="prototype"; '
            "SELECT * FROM input MATCH_RECOGNIZE ("
            "PARTITION BY device_id, zone_id "
            "ORDER BY ts "
            "MEASURES LAST(B1.ts) AS b1, LAST(B3.ts) AS b3 "
            "ONE ROW PER MATCH "
            "AFTER MATCH SKIP TO NEXT ROW "
            "PATTERN (B1 B2+ B3) "
            "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
            ")"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "PRAGMA FeatureR010 = 'prototype';\n"
            "SELECT * FROM `input` MATCH_RECOGNIZE ("
            "PARTITION BY device_id, zone_id "
            "ORDER BY ts "
            "MEASURES LAST(B1.ts) AS b1, LAST(B3.ts) AS b3 "
            "ONE ROW PER MATCH "
            "AFTER MATCH SKIP TO NEXT ROW "
            "PATTERN (B1 B2+ B3) "
            "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
            ")",
            generated,
        )

    def test_match_recognize_doc_pattern_and_define_snippets(self):
        self.validate_identity(
            "SELECT * FROM input MATCH_RECOGNIZE ("
            "PATTERN (B1 E* B2+ B3) "
            "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
            ")",
            write_sql=(
                "SELECT * FROM `input` MATCH_RECOGNIZE ( "
                "PATTERN (B1 E* B2+ B3) "
                "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
                ")"
            ),
        )
        self.validate_identity(
            "SELECT * FROM input MATCH_RECOGNIZE ("
            "PATTERN (A B) "
            "DEFINE A AS A.button = 1 AND LAST(A.zone_id) = 12, "
            "B AS B.button = 2 AND FIRST(A.zone_id) = 12"
            ")",
            write_sql=(
                "SELECT * FROM `input` MATCH_RECOGNIZE ( "
                "PATTERN (A B) "
                "DEFINE A AS A.button = 1 AND LAST(A.zone_id) = 12, "
                "B AS B.button = 2 AND FIRST(A.zone_id) = 12"
                ")"
            ),
        )

    def test_match_recognize_doc_rows_per_match_and_after_skip_snippets(self):
        self.validate_identity(
            "SELECT * FROM input MATCH_RECOGNIZE ("
            "MEASURES FIRST(B1.ts) AS first_ts, FIRST(B2.ts) AS mid_ts, LAST(B3.ts) AS last_ts "
            "ALL ROWS PER MATCH "
            "AFTER MATCH SKIP PAST LAST ROW "
            "PATTERN (B1 {- B2 -} B3) "
            "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
            ")",
            write_sql=(
                "SELECT * FROM `input` MATCH_RECOGNIZE ( "
                "MEASURES FIRST(B1.ts) AS first_ts, FIRST(B2.ts) AS mid_ts, LAST(B3.ts) AS last_ts "
                "ALL ROWS PER MATCH "
                "AFTER MATCH SKIP PAST LAST ROW "
                "PATTERN (B1 {- B2 -} B3) "
                "DEFINE B1 AS B1.button = 1, B2 AS B2.button = 2, B3 AS B3.button = 3"
                ")"
            ),
        )

    def test_match_recognize_doc_order_and_partition_snippets(self):
        self.validate_identity(
            "SELECT * FROM input MATCH_RECOGNIZE ("
            "PARTITION BY device_id, zone_id "
            "ORDER BY CAST(ts AS Timestamp) "
            "PATTERN (B1) "
            "DEFINE B1 AS B1.button = 1"
            ")",
            write_sql=(
                "SELECT * FROM `input` MATCH_RECOGNIZE ("
                "PARTITION BY device_id, zone_id "
                "ORDER BY CAST(ts AS Timestamp) "
                "PATTERN (B1) "
                "DEFINE B1 AS B1.button = 1"
                ")"
            ),
        )

    def test_match_recognize_doc_rejects_unsupported_after_skip_modes(self):
        with self.assertRaises(UnsupportedError):
            parse_one(
                "SELECT * FROM input MATCH_RECOGNIZE ("
                "AFTER MATCH SKIP TO FIRST B1 "
                "PATTERN (B1) "
                "DEFINE B1 AS B1.button = 1"
                ")",
                dialect="ydb",
            ).sql(dialect="ydb")

    def test_window_functions(self):
        cases = [
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM `table`",
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) FROM `table`",
            "SELECT COUNT(*) OVER w AS rows_count_in_window, some_other_value FROM `my_table` WINDOW w AS (PARTITION BY partition_key_column ORDER BY int_column)",
            "SELECT LAG(my_column, 2) OVER w AS row_before_previous_one FROM `my_table` WINDOW w AS (PARTITION BY partition_key_column)",
            "SELECT LAG(my_column, 2) OVER w AS row_before_previous_one FROM `my_table` WINDOW w AS (PARTITION BY partition_key_column ORDER BY my_column)",
            "SELECT AVG(some_value) OVER w AS avg_of_prev_current_next, some_other_value FROM `my_table` WINDOW w AS (PARTITION BY partition_key_column ORDER BY int_column ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            "SELECT SUM(x) OVER (PARTITION BY a + b AS c ORDER BY t) FROM `my_table`",
            "SELECT SUM(x) OVER (PARTITION COMPACT BY key ORDER BY t) FROM `my_table`",
            "SELECT SUM(x) OVER (PARTITION COMPACT BY () ORDER BY t) FROM `my_table`",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_window_frame_begin_defaults_to_current_row(self):
        cases = [
            (
                "SELECT SUM(x) OVER (ORDER BY t ROWS UNBOUNDED PRECEDING) FROM `my_table`",
                "SELECT SUM(x) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM `my_table`",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY t ROWS 3 PRECEDING) FROM `my_table`",
                "SELECT SUM(x) OVER (ORDER BY t ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM `my_table`",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY t ROWS CURRENT ROW) FROM `my_table`",
                "SELECT SUM(x) OVER (ORDER BY t ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM `my_table`",
            ),
        ]
        for sql, write_sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql, write_sql=write_sql)

    def test_aggregates(self):
        self.validate_identity(
            "SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM `table`"
        )


# ---------------------------------------------------------------------------
# YDB-specific transformations
# ---------------------------------------------------------------------------

class TestYDBTransforms(Validator):
    """Tests for YDB-specific output: table quoting, CTEs, type mapping, etc."""

    maxDiff = None
    dialect = "ydb"

    # --- Table names --------------------------------------------------------

    def test_table_name_backtick_quoting(self):
        self.assertEqual(ydb("SELECT * FROM t"), "SELECT * FROM `t`")

    def test_table_name_with_db_prefix(self):
        # db part preserved as-is; use make_db_name_lower() helper to lowercase it
        self.assertEqual(ydb("SELECT * FROM 'A'.'B'"), "SELECT * FROM `A/B`")

    def test_bracket_table_path(self):
        self.assertEqual(
            parse_one(
                "SELECT * FROM [rtlog-index/request/2026-01-25T08:00:00]",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "SELECT * FROM `rtlog-index/request/2026-01-25T08:00:00`",
        )

    def test_table_view_index(self):
        self.assertEqual(
            parse_one(
                "SELECT * FROM tracks VIEW tracks_session_id_groupstamp_index WHERE groupstamp >= 0",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "SELECT * FROM `tracks` VIEW tracks_session_id_groupstamp_index WHERE groupstamp >= 0",
        )

    def test_table_view_primary_key_alias(self):
        self.assertEqual(
            parse_one(
                "SELECT * FROM `/db/table` VIEW PRIMARY KEY d",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "SELECT * FROM `/db/table` VIEW PRIMARY KEY d",
        )

    def test_secondary_index_doc_select_snippet(self):
        self.validate_transpile(
            "SELECT series_id, title, info, release_date, views, uploaded_user_id "
            "FROM series VIEW views_index WHERE views >= someValue",
            "SELECT series_id, title, info, release_date, views, uploaded_user_id "
            "FROM `series` VIEW views_index WHERE views >= someValue",
        )

    def test_secondary_index_doc_join_snippet(self):
        self.validate_transpile(
            "SELECT t1.series_id, t1.title "
            "FROM series VIEW users_index AS t1 "
            "INNER JOIN users VIEW name_index AS t2 "
            "ON t1.uploaded_user_id == t2.user_id "
            "WHERE t2.name == userName",
            "SELECT t1.series_id AS series_id, t1.title AS title "
            "FROM `series` VIEW users_index AS t1 "
            "INNER JOIN `users` VIEW name_index AS t2 "
            "ON t1.uploaded_user_id = t2.user_id "
            "WHERE t2.name = userName",
        )

    def test_table_with_source_options(self):
        sql = (
            "SELECT * FROM `table` WITH ("
            "FORMAT = parquet, "
            "SCHEMA (class String?, timestamp Timestamp, year Int NOT NULL), "
            "PARTITIONED_BY = (year)"
            ") WHERE year = 1"
        )
        self.assertEqual(
            parse_one(sql, dialect="ydb").sql(dialect="ydb"),
            sql,
        )

    def test_table_with_unparenthesized_source_option(self):
        sql = "SELECT * FROM `table` WITH TabletId='tablet-1' WHERE id = 1"
        self.assertEqual(parse_one(sql, dialect="ydb").sql(dialect="ydb"), sql)

    def test_with_doc_infer_schema_snippets(self):
        self.validate_transpile(
            "SELECT key FROM my_table WITH INFER_SCHEMA",
            "SELECT key FROM `my_table` WITH INFER_SCHEMA",
        )
        self.validate_transpile(
            'SELECT key FROM my_table WITH FORCE_INFER_SCHEMA="42"',
            'SELECT key FROM `my_table` WITH FORCE_INFER_SCHEMA="42"',
        )

    def test_with_doc_named_expression_xlock_snippet(self):
        self.validate_identity("$s = (SELECT COUNT(*) FROM `my_table` WITH XLOCK)")

    def test_with_doc_schema_and_columns_snippets(self):
        self.validate_transpile(
            "SELECT key, value FROM my_table WITH SCHEMA Struct<key:String, value:Int32>",
            "SELECT key, value FROM `my_table` WITH SCHEMA Struct<key:String, value:Int32>",
        )
        self.validate_transpile(
            "SELECT key, value FROM my_table WITH COLUMNS Struct<value:Int32?>",
            "SELECT key, value FROM `my_table` WITH COLUMNS Struct<value:Int32?>",
        )

    def test_with_doc_each_schema_snippet(self):
        self.validate_identity(
            "SELECT key, value FROM EACH($my_tables) WITH SCHEMA Struct<key:String, value:List<Int32>>"
        )

    def test_at_raw_string_literal(self):
        self.assertEqual(
            parse_one(
                "SELECT * FROM `tracks` WHERE session_id IN (@@21b7d5b7-efe9-f94c-b6ff-2362c1b00fd4@@)",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "SELECT * FROM `tracks` WHERE session_id IN (@@21b7d5b7-efe9-f94c-b6ff-2362c1b00fd4@@)",
        )

    def test_table_name_lower_case_helper(self):
        parsed = table_names_to_lower_case(parse_one("SELECT * FROM B, (SELECT * from D) as E"))
        self.assertEqual(parsed.sql(), "SELECT * FROM b, (SELECT * FROM d) AS E")

    def test_db_name_lower_case_helper(self):
        parsed = make_db_name_lower(parse_one("SELECT * FROM 'A'.'B'"))
        self.assertEqual(parsed.sql(dialect="ydb"), "SELECT * FROM `a/B`")

    def test_subselect(self):
        self.assertEqual(
            ydb("SELECT * FROM (select * from b) T"),
            "SELECT * FROM (SELECT * FROM `b`) AS T",
        )

    def test_distinct_doc_value_snippet(self):
        self.validate_transpile(
            "SELECT DISTINCT value FROM my_table",
            "SELECT DISTINCT value FROM `my_table`",
        )

    def test_distinct_doc_aggregate_distinct_values(self):
        self.validate_transpile(
            "SELECT COUNT(DISTINCT value) AS count FROM my_table",
            "SELECT COUNT(DISTINCT value) AS count FROM `my_table`",
        )

    def test_distinct_doc_calculated_value_uses_subquery(self):
        self.validate_transpile(
            "SELECT DISTINCT value + 1 AS value FROM my_table",
            "SELECT DISTINCT value FROM (SELECT value + 1 AS value FROM `my_table`) AS _distinct",
        )

    def test_derived_table_join_using_is_not_decorrelated(self):
        self.assertEqual(
            parse_one(
                "SELECT COUNT(*) AS count FROM (SELECT * FROM `/db/table1` "
                "WHERE billing_account_id = $billing_account_id_0 AND created_at > $since_0) AS idx "
                "JOIN `/db/table` AS table USING (id) WHERE status IN ($status_0, $status_1)",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "SELECT COUNT(*) AS count FROM (SELECT * FROM `/db/table1` "
            "WHERE billing_account_id = $billing_account_id_0 AND created_at > $since_0) AS idx "
            "JOIN `/db/table` AS table USING (id) WHERE status IN ($status_0, $status_1)",
        )

    def test_column_with_table_alias(self):
        self.assertEqual(ydb("SELECT a.a FROM T"), "SELECT a.a AS a FROM `T`")

    def test_struct_access(self):
        self.assertEqual(
            ydb("SELECT struct.field, struct.subfield.subsub FROM table"),
            "SELECT struct.field AS field, struct.subfield.subsub AS subsub FROM `table`",
        )

    def test_escape_single_quotes(self):
        self.assertEqual(ydb("SELECT 'it''s a test' FROM table"), "SELECT 'it''s a test' FROM `table`")

    def test_star_with_table_prefix(self):
        self.assertEqual(ydb("SELECT B.* FROM B"), "SELECT B.* FROM `B`")

    # --- CTEs → YDB variables -----------------------------------------------

    def test_cte_single(self):
        result = ydb("with ct as (select * from b) SELECT * from ct")
        self.assertIn("$ct = (SELECT * FROM `b`);\n\nSELECT * FROM $ct AS ct", result)

    def test_cte_embedded(self):
        self.assertEqual(
            ydb("SELECT * from (with ct as (select * from b) select * from ct)"),
            "$ct = (SELECT * FROM `b`);\n\nSELECT * FROM (SELECT * FROM $ct AS ct)",
        )

    def test_cte_multiple(self):
        sql = """
            WITH cte1 AS (SELECT * FROM table1),
                 cte2 AS (SELECT * FROM table2)
            SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id
        """
        self.assertEqual(
            ydb(sql),
            "$cte1 = (SELECT * FROM `table1`);\n\n$cte2 = (SELECT * FROM `table2`);\n\n"
            "SELECT * FROM $cte1 AS cte1 JOIN $cte2 AS cte2 ON cte1.id = cte2.id",
        )

    def test_cte_recursive(self):
        sql = """
            WITH RECURSIVE cte AS (
                SELECT 1 as level
                UNION ALL
                SELECT level + 1 FROM cte WHERE level < 10
            )
            SELECT * FROM cte
        """
        self.assertEqual(
            ydb(sql),
            "$cte = (SELECT 1 AS level UNION ALL SELECT level + 1 FROM $cte AS cte WHERE level < 10);\n\n"
            "SELECT * FROM $cte AS cte",
        )

    # --- Type mapping -------------------------------------------------------

    def test_type_bit_to_uint8(self):
        self.assertEqual(ydb("SELECT CAST(1 as BIT)"), "SELECT CAST(1 AS Uint8)")

    def test_type_varchar_to_utf8_in_alter(self):
        self.assertEqual(
            ydb("ALTER TABLE table ADD COLUMN new_column String"),
            "ALTER TABLE `table` ADD COLUMN new_column Utf8",
        )

    def test_create_table_simple_types(self):
        sql = """
            CREATE TABLE table (
                id         Uint64 NOT NULL,
                name       String,
                created_at Timestamp,
                PRIMARY KEY (id)
            )
        """
        self.assertEqual(
            ydb(sql),
            "CREATE TABLE `table` (id Uint64 NOT NULL, name Utf8, created_at Timestamp, PRIMARY KEY (id))",
        )

    def test_create_table_doc_row_oriented_snippet(self):
        self.assertEqual(
            ydb(
                """
                CREATE TABLE table_name (
                  a Uint64,
                  b Uint64,
                  c Float,
                  PRIMARY KEY (a, b)
                )
                """
            ),
            "CREATE TABLE `table_name` (a Uint64, b Uint64, c Float, PRIMARY KEY (a, b))",
        )

    def test_create_table_doc_partitioning_options_snippet(self):
        self.assertEqual(
            ydb(
                """
                CREATE TABLE table_name (
                  a Uint64,
                  b Uint64,
                  c Float,
                  PRIMARY KEY (a, b)
                )
                WITH (
                  AUTO_PARTITIONING_BY_SIZE = ENABLED,
                  AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
                )
                """
            ),
            "CREATE TABLE `table_name` (a Uint64, b Uint64, c Float, PRIMARY KEY (a, b)) "
            "WITH (AUTO_PARTITIONING_BY_SIZE=ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB=512)",
        )

    def test_create_table_doc_column_store_snippet(self):
        self.assertEqual(
            ydb(
                """
                CREATE TABLE table_name (
                  a Uint64 NOT NULL,
                  b Timestamp NOT NULL,
                  c Float,
                  PRIMARY KEY (a, b)
                )
                PARTITION BY HASH(b)
                WITH (
                  STORE = COLUMN
                )
                """
            ),
            "CREATE TABLE `table_name` (a Uint64 NOT NULL, b Timestamp NOT NULL, c Float, PRIMARY KEY (a, b)) "
            "PARTITION BY HASH(b) WITH (STORE=COLUMN)",
        )

    def test_create_table_doc_column_store_min_partitions_snippet(self):
        self.assertEqual(
            ydb(
                """
                CREATE TABLE table_name (
                  a Uint64 NOT NULL,
                  b Timestamp NOT NULL,
                  c Float,
                  PRIMARY KEY (a, b)
                )
                PARTITION BY HASH(b)
                WITH (
                  STORE = COLUMN,
                  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                )
                """
            ),
            "CREATE TABLE `table_name` (a Uint64 NOT NULL, b Timestamp NOT NULL, c Float, PRIMARY KEY (a, b)) "
            "PARTITION BY HASH(b) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=10)",
        )

    def test_create_table_doc_if_not_exists(self):
        self.assertEqual(
            ydb("CREATE TABLE IF NOT EXISTS table_name (a Uint64, PRIMARY KEY (a))"),
            "CREATE TABLE IF NOT EXISTS `table_name` (a Uint64, PRIMARY KEY (a))",
        )

    def test_create_table_as_select_doc_example(self):
        self.assertEqual(
            ydb(
                """
                CREATE TABLE my_table (
                    PRIMARY KEY (key1, key2)
                ) WITH (
                    STORE=COLUMN
                ) AS SELECT
                    key AS key1,
                    Unwrap(other_key) AS key2,
                    value,
                    String::Contains(value, "test") AS has_test
                FROM other_table
                """,
                read="ydb",
            ),
            "CREATE TABLE `my_table` (PRIMARY KEY (key1, key2)) WITH (STORE=COLUMN) "
            "AS SELECT key AS key1, Unwrap(other_key) AS key2, value, "
            "String::Contains(value, 'test') AS has_test FROM `other_table`",
        )

    def test_create_table_doc_requires_primary_key(self):
        with self.assertRaises(UnsupportedError):
            ydb("CREATE TABLE table_name (a Uint64)")

    def test_create_table_as_select_without_store_passes_through(self):
        self.assertEqual(
            ydb("CREATE TABLE dst (PRIMARY KEY (a)) AS SELECT a FROM src", read="ydb"),
            "CREATE TABLE `dst` (PRIMARY KEY (a)) AS SELECT a FROM `src`",
        )

    def test_create_table_as_select_with_column_definitions_passes_through(self):
        self.assertEqual(
            ydb(
                "CREATE TABLE dst (a Uint64, PRIMARY KEY (a)) WITH (STORE=COLUMN) AS SELECT a FROM src",
                read="ydb",
            ),
            "CREATE TABLE `dst` (a Uint64, PRIMARY KEY (a)) WITH (STORE=COLUMN) AS SELECT a FROM `src`",
        )

    def test_create_table_as_select_with_indexes_passes_through(self):
        self.assertEqual(
            ydb(
                "CREATE TABLE dst (a Uint64, INDEX idx ON (a), PRIMARY KEY (a)) "
                "WITH (STORE=COLUMN) AS SELECT a FROM src",
                read="ydb",
            ),
            "CREATE TABLE `dst` (a Uint64, INDEX idx ON (a), PRIMARY KEY (a)) "
            "WITH (STORE=COLUMN) AS SELECT a FROM `src`",
        )

    def test_create_table_as_select_with_column_groups_passes_through(self):
        self.assertEqual(
            ydb(
                "CREATE TABLE dst (a Uint64, PRIMARY KEY (a), FAMILY family_large (COMPRESSION = \"zstd\")) "
                "WITH (STORE=COLUMN) AS SELECT a FROM src",
                read="ydb",
            ),
            "CREATE TABLE `dst` (a Uint64, PRIMARY KEY (a), FAMILY family_large (COMPRESSION='zstd')) "
            "WITH (STORE=COLUMN) AS SELECT a FROM `src`",
        )

    def test_create_table_secondary_index_doc_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE my_table (
                    a Uint64,
                    b Bool,
                    c Utf8,
                    d Date,
                    INDEX idx_d GLOBAL ON (d),
                    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
                    PRIMARY KEY (a)
                )
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `my_table` (a Uint64, b Uint8, c Utf8, d DATE, "
            "INDEX idx_d GLOBAL ON (d), "
            "INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c), PRIMARY KEY (a))",
        )

    def test_create_table_secondary_index_doc_defaults(self):
        self.assertEqual(
            self.parse_one("CREATE TABLE t (a Uint64, b Utf8, INDEX idx ON (b), PRIMARY KEY (a))").sql(dialect="ydb"),
            "CREATE TABLE `t` (a Uint64, b Utf8, INDEX idx ON (b), PRIMARY KEY (a))",
        )

    def test_create_table_secondary_index_doc_full_syntax(self):
        self.assertEqual(
            self.parse_one(
                "CREATE TABLE t ("
                "a Uint64, b Utf8, "
                "INDEX idx LOCAL SYNC USING secondary ON (b) COVER (a) WITH (foo = 1), "
                "PRIMARY KEY (a)"
                ")"
            ).sql(dialect="ydb"),
            "CREATE TABLE `t` (a Uint64, b Utf8, "
            "INDEX idx LOCAL SYNC USING secondary ON (b) COVER (a) WITH (foo=1), "
            "PRIMARY KEY (a))",
        )

    def test_create_table_secondary_index_doc_requires_index_columns(self):
        with self.assertRaises(ParseError):
            self.parse_one("CREATE TABLE t (a Uint64, INDEX idx ON (), PRIMARY KEY (a))")

    def test_create_table_secondary_index_doc_rejects_column_store(self):
        with self.assertRaises(UnsupportedError):
            self.parse_one(
                "CREATE TABLE t (a Uint64, b Utf8, INDEX idx ON (b), PRIMARY KEY (a)) WITH (STORE = COLUMN)"
            ).sql(dialect="ydb")

    def test_create_table_family_doc_row_oriented_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE series_with_families (
                    series_id Uint64,
                    title Utf8,
                    series_info Utf8 FAMILY family_large,
                    release_date Uint64,
                    PRIMARY KEY (series_id),
                    FAMILY default (
                        DATA = "ssd",
                        COMPRESSION = "off"
                    ),
                    FAMILY family_large (
                        DATA = "rot",
                        COMPRESSION = "lz4"
                    )
                )
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `series_with_families` (series_id Uint64, title Utf8, "
            "series_info Utf8 FAMILY family_large, release_date Uint64, "
            "PRIMARY KEY (series_id), FAMILY default (DATA='ssd', COMPRESSION='off'), "
            "FAMILY family_large (DATA='rot', COMPRESSION='lz4'))",
        )

    def test_create_table_family_doc_column_oriented_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE series_with_families (
                    series_id Uint64,
                    title Utf8,
                    series_info Utf8 FAMILY family_large,
                    release_date Uint64,
                    PRIMARY KEY (series_id),
                    FAMILY default (
                        COMPRESSION = "lz4"
                    ),
                    FAMILY family_large (
                        COMPRESSION = "zstd",
                        COMPRESSION_LEVEL = 5
                    )
                )
                WITH (STORE = COLUMN)
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `series_with_families` (series_id Uint64, title Utf8, "
            "series_info Utf8 FAMILY family_large, release_date Uint64, "
            "PRIMARY KEY (series_id), FAMILY default (COMPRESSION='lz4'), "
            "FAMILY family_large (COMPRESSION='zstd', COMPRESSION_LEVEL=5)) WITH (STORE=COLUMN)",
        )

    def test_create_table_family_doc_data_rejects_column_store(self):
        with self.assertRaises(UnsupportedError):
            self.parse_one(
                'CREATE TABLE t (a Uint64 FAMILY fam, PRIMARY KEY (a), FAMILY fam (DATA = "ssd")) '
                "WITH (STORE = COLUMN)"
            ).sql(dialect="ydb")

    def test_create_table_family_doc_compression_level_requires_column_store(self):
        with self.assertRaises(UnsupportedError):
            self.parse_one(
                "CREATE TABLE t (a Uint64 FAMILY fam, PRIMARY KEY (a), FAMILY fam (COMPRESSION_LEVEL = 5))"
            ).sql(dialect="ydb")

    def test_create_table_family_doc_zstd_requires_column_store(self):
        with self.assertRaises(UnsupportedError):
            self.parse_one(
                'CREATE TABLE t (a Uint64 FAMILY fam, PRIMARY KEY (a), FAMILY fam (COMPRESSION = "zstd"))'
            ).sql(dialect="ydb")

    def test_create_table_with_doc_ttl_row_oriented_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE my_table (
                    id Uint64,
                    title Utf8,
                    expire_at Timestamp,
                    PRIMARY KEY (id)
                )
                WITH (
                    TTL = Interval("PT0S") ON expire_at
                )
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `my_table` (id Uint64, title Utf8, expire_at Timestamp, "
            "PRIMARY KEY (id)) WITH (TTL=Interval('PT0S') ON expire_at)",
        )

    def test_create_table_with_doc_ttl_column_oriented_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE table_name (
                    a Uint64 NOT NULL,
                    b Timestamp NOT NULL,
                    c Float,
                    PRIMARY KEY (a, b)
                )
                PARTITION BY HASH(b)
                WITH (
                    STORE = COLUMN,
                    TTL = Interval("PT0S") ON b
                )
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `table_name` (a Uint64 NOT NULL, b Timestamp NOT NULL, c Float, "
            "PRIMARY KEY (a, b)) PARTITION BY HASH(b) WITH (STORE=COLUMN, TTL=Interval('PT0S') ON b)",
        )

    def test_create_table_with_doc_ttl_external_data_source_example(self):
        self.assertEqual(
            self.parse_one(
                """
                CREATE TABLE table_name (
                    a Uint64 NOT NULL,
                    b Timestamp NOT NULL,
                    c Float,
                    PRIMARY KEY (a, b)
                )
                PARTITION BY HASH(b)
                WITH (
                    STORE = COLUMN,
                    TTL =
                        Interval("PT1D") TO EXTERNAL DATA SOURCE `/Root/s3`,
                        Interval("P2D") DELETE
                    ON b
                )
                """
            ).sql(dialect="ydb"),
            "CREATE TABLE `table_name` (a Uint64 NOT NULL, b Timestamp NOT NULL, c Float, "
            "PRIMARY KEY (a, b)) PARTITION BY HASH(b) WITH (STORE=COLUMN, "
            "TTL=Interval('PT1D') TO EXTERNAL DATA SOURCE `/Root/s3`, Interval('P2D') DELETE ON b)",
        )

    def test_create_table_with_doc_ttl_numeric_units(self):
        for unit in ("SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS"):
            with self.subTest(unit=unit):
                self.assertEqual(
                    self.parse_one(
                        f'CREATE TABLE t (id Uint64, ttl Uint64, PRIMARY KEY (id)) '
                        f'WITH (TTL = Interval("PT1S") ON ttl AS {unit})'
                    ).sql(dialect="ydb"),
                    f"CREATE TABLE `t` (id Uint64, ttl Uint64, PRIMARY KEY (id)) "
                    f"WITH (TTL=Interval('PT1S') ON ttl AS {unit})",
                )

    def test_create_table_with_doc_ttl_external_data_source_requires_column_store(self):
        with self.assertRaises(UnsupportedError):
            self.parse_one(
                'CREATE TABLE t (id Uint64, ttl Timestamp, PRIMARY KEY (id)) '
                'WITH (TTL = Interval("PT1D") TO EXTERNAL DATA SOURCE `/Root/s3` ON ttl)'
            ).sql(dialect="ydb")

    # --- Function transforms ------------------------------------------------

    def test_concat_to_pipe(self):
        self.assertEqual(ydb("SELECT CONCAT(A,B) FROM data"), "SELECT A || B FROM `data`")

    def test_round_negates_precision(self):
        # SQL ROUND(x, n) → Math::Round(x, -n) because YDB sign convention is reversed
        self.assertEqual(ydb("SELECT ROUND(3.14159, 2)"), "SELECT Math::Round(3.14159, -2)")
        self.assertEqual(ydb("SELECT ROUND(3.14159)"), "SELECT Math::Round(3.14159)")

    def test_nullif_to_if(self):
        self.assertEqual(ydb("SELECT NULLIF('a','a') FROM data"), "SELECT IF('a' = 'a', NULL, 'a') FROM `data`")

    def test_if_passthrough(self):
        self.assertEqual(ydb("SELECT IF(10 > 20, 'TRUE', 'FALSE') FROM data"), "SELECT IF(10 > 20, 'TRUE', 'FALSE') FROM `data`")

    def test_if_without_else_passthrough(self):
        self.assertEqual(ydb("SELECT IF(x IS NOT NULL, AsStruct(x AS x)) FROM data"), "SELECT IF(x IS NOT NULL, AsStruct(x AS x)) FROM `data`")

    def test_array_any(self):
        self.assertEqual(
            ydb("SELECT * FROM TABLE WHERE ARRAY_ANY(arr, x -> x)"),
            "SELECT * FROM `TABLE` WHERE ListHasItems(ListFilter(($x) -> ($x)))",
        )

    def test_duckdb_list_filter_lambda_to_ydb(self):
        self.assertEqual(
            ydb("SELECT list_filter(arr, x -> x > 0) FROM t", read="duckdb"),
            "SELECT ListFilter(arr, ($x) -> ($x > 0)) FROM `t`",
        )

    def test_nested_nullif_coalesce(self):
        self.assertEqual(
            ydb("SELECT COALESCE(NULLIF(name, ''), 'default') FROM table"),
            "SELECT COALESCE(IF(name = '', NULL, name), 'default') FROM `table`",
        )

    def test_datetrunc_year(self):
        self.assertEqual(
            ydb("SELECT DATE_TRUNC('year', dt) from table"),
            "SELECT DateTime::MakeDate(DateTime::StartOfYear(dt)) FROM `table`",
        )

    def test_datetrunc_month(self):
        self.assertEqual(
            ydb("SELECT DATE_TRUNC('month', dt) from table"),
            "SELECT DateTime::MakeDate(DateTime::StartOfMonth(dt)) FROM `table`",
        )

    def test_extract_year(self):
        self.assertEqual(ydb("SELECT EXTRACT(YEAR FROM dt) from table"), "SELECT DateTime::GetYear(dt) FROM `table`")

    def test_str_to_date(self):
        self.assertEqual(
            parse_one("SELECT to_date('29.03.2023', 'DD.MM.YYYY') from table", dialect="oracle").sql(dialect="ydb"),
            "SELECT DateTime::MakeTimestamp(DateTime::Parse('%d.%m.%Y')(\"29.03.2023\")) FROM `table`",
        )

    def test_date_add(self):
        cases = [
            ("select date_add('2025-01-01', interval 2 month)",
             "SELECT DateTime::MakeDate(DateTime::ShiftMonths(CAST('2025-01-01' AS DATE), 2))"),
            ("select date_add('2025-01-01', interval -2 month)",
             "SELECT DateTime::MakeDate(DateTime::ShiftMonths(CAST('2025-01-01' AS DATE), -2))"),
            ("select date_add('2025-01-01', interval 2 years)",
             "SELECT DateTime::MakeDate(DateTime::ShiftYears(CAST('2025-01-01' AS DATE), 2))"),
            ("select date_add('2025-01-01', interval 1 day)",
             "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromDays(1)"),
            ("select date_add('2025-01-01', interval 1 hour)",
             "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromHours(1)"),
            ("select date_add('2025-01-01', interval 1 minute)",
             "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromMinutes(1)"),
            ("select date_add('2025-01-01', interval 1 second)",
             "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromSeconds(1)"),
            ("select date_add('2025-01-01 01:01:01', interval 1 second)",
             "SELECT DateTime::MakeDateTime(DateTime::ParseIso8601('2025-01-01T01:01:01')) + DateTime::IntervalFromSeconds(1)"),
            ("select date_sub('2025-01-01', interval 1 day)",
             "SELECT CAST('2025-01-01' AS DATE) - DateTime::IntervalFromDays(1)"),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(parse_one(sql).sql(dialect="ydb"), expected)

    # --- JOIN ON restrictions -----------------------------------------------

    def test_join_marks_oracle(self):
        parsed = parse_one("select * from a, b where a.id(+) = b.id", dialect="oracle")
        self.assertEqual(eliminate_join_marks(parsed).sql(), "SELECT * FROM b LEFT JOIN a ON a.id = b.id")

    def test_join_on_parenthesized_equalities_stay_in_on(self):
        sql = (
            "SELECT * FROM `a` AS a JOIN `b` AS b "
            "ON (a.x = b.x AND a.y = b.y) AND a.z = b.z"
        )
        self.validate_identity(sql)

    # --- Subquery unnesting / decorrelation ---------------------------------

    def test_decorrelate_scalar_subquery(self):
        sql = "SELECT a.id, (SELECT MAX(b.value) FROM b WHERE b.id = a.id) as max_value FROM a"
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id, _u_0._u_2 AS max_value FROM `a` LEFT JOIN "
            "(SELECT MAX(b.value) AS _u_2, b.id AS _u_1 FROM `b` WHERE TRUE GROUP BY id AS id) AS _u_0 "
            "ON a.id = _u_0._u_1",
        )

    def test_decorrelate_exists_subquery(self):
        sql = "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)"
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id FROM `a` LEFT JOIN "
            "(SELECT a_id AS _u_1, 1 AS _exists_flag FROM `b` WHERE TRUE GROUP BY a_id AS _u_1) AS _u_0 "
            "ON a.id = _u_0._u_1 WHERE _u_0._u_1 IS NOT NULL",
        )

    def test_decorrelate_multiple_subqueries(self):
        sql = """
            SELECT a.id,
                   (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) as cnt,
                   (SELECT AVG(b.value) FROM b WHERE b.a_id = a.id) as avg_val
            FROM a
        """
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id, COALESCE(_u_0._u_2, 0) AS cnt, _u_3._u_5 AS avg_val FROM `a` "
            "LEFT JOIN (SELECT COUNT(*) AS _u_2, b.a_id AS _u_1 FROM `b` WHERE TRUE GROUP BY a_id AS a_id) AS _u_0 "
            "ON a.id = _u_0._u_1 "
            "LEFT JOIN (SELECT AVG(b.value) AS _u_5, b.a_id AS _u_4 FROM `b` WHERE TRUE GROUP BY a_id AS a_id) AS _u_3 "
            "ON a.id = _u_3._u_4",
        )

    def test_decorrelate_nested_subqueries(self):
        sql = """
            SELECT a.id FROM a
            WHERE EXISTS (
                SELECT 1 FROM b
                WHERE b.a_id = a.id
                  AND EXISTS (SELECT 1 FROM c WHERE c.b_id = b.id)
            )
        """
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id FROM `a` LEFT JOIN "
            "(SELECT a_id AS _u_3, 1 AS _exists_flag FROM `b` "
            "LEFT JOIN (SELECT b_id AS _u_1, 1 AS _exists_flag FROM `c` WHERE TRUE GROUP BY b_id AS _u_1) AS _u_0 "
            "ON b.id = _u_0._u_1 WHERE TRUE AND _u_0._u_1 IS NOT NULL GROUP BY a_id AS a_id) AS _u_2 "
            "ON a.id = _u_2._u_3 WHERE _u_2._u_3 IS NOT NULL",
        )

    def test_unnest_uncorrelated_in_subquery(self):
        sql = "SELECT a.id FROM a WHERE a.id IN (SELECT b.a_id FROM b WHERE b.value > 10)"
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id FROM `a` LEFT JOIN "
            "(SELECT b.a_id AS a_id FROM `b` WHERE b.value > 10 GROUP BY a_id AS a_id) AS _u_0 "
            "ON a.id = _u_0.a_id WHERE _u_0.a_id IS NOT NULL",
        )

    def test_unnest_correlated_in_subquery(self):
        sql = "SELECT * FROM x WHERE x.a IN (SELECT y.a FROM y WHERE y.b = x.b)"
        self.assertEqual(
            ydb(sql),
            "SELECT * FROM `x` LEFT JOIN "
            "(SELECT y.a AS a, y.b AS _u_1 FROM `y` WHERE TRUE GROUP BY b AS b) AS _u_0 "
            "ON x.b = _u_0._u_1 WHERE ListHasItems(($_x, $p_0)->(ListFilter($_x, ($_x) -> ($_x = $p_0)))(a, x.a))",
        )

    def test_unnest_scalar_subquery(self):
        sql = "SELECT * FROM x WHERE (SELECT y.a FROM y WHERE x.a = y.a) = 1"
        self.assertEqual(
            ydb(sql),
            "SELECT * FROM `x` LEFT JOIN "
            "(SELECT y.a AS a, y.a AS _u_1 FROM `y` WHERE TRUE GROUP BY a AS a) AS _u_0 "
            "ON x.a = _u_0._u_1 WHERE _u_0.a = 1",
        )

    def test_unnest_any_subquery(self):
        sql = "SELECT * FROM x WHERE x.a > ANY (SELECT y.a FROM y WHERE y.b = x.b)"
        self.assertEqual(
            ydb(sql),
            "SELECT * FROM `x` LEFT JOIN "
            "(SELECT y.a AS a, y.b AS _u_1 FROM `y` WHERE TRUE GROUP BY b AS b) AS _u_0 "
            "ON x.b = _u_0._u_1 "
            "WHERE x.a > ListHasItems(($_x, $p_0)->(ListFilter($_x, ($_x) -> ($p_0 > $_x)))(a, x.a))",
        )

    def test_unnest_aggregate_subquery(self):
        sql = "SELECT * FROM x WHERE (SELECT MAX(y.value) FROM y WHERE y.x_id = x.id) > 100"
        self.assertEqual(
            ydb(sql),
            "SELECT * FROM `x` LEFT JOIN "
            "(SELECT MAX(y.value) AS _u_2, y.x_id AS _u_1 FROM `y` WHERE TRUE GROUP BY x_id AS x_id) AS _u_0 "
            "ON x.id = _u_0._u_1 WHERE _u_0._u_2 > 100",
        )

    def test_unnest_correlated_count_subquery(self):
        sql = "SELECT x.id, (SELECT COUNT(*) FROM y WHERE y.x_id = x.id) as y_count FROM x"
        self.assertEqual(
            ydb(sql),
            "SELECT x.id AS id, COALESCE(_u_0._u_2, 0) AS y_count FROM `x` LEFT JOIN "
            "(SELECT COUNT(*) AS _u_2, y.x_id AS _u_1 FROM `y` WHERE TRUE GROUP BY x_id AS x_id) AS _u_0 "
            "ON x.id = _u_0._u_1",
        )

    def test_case_with_subquery_in_then(self):
        sql = "SELECT CASE WHEN a = 1 THEN (SELECT MAX(b) FROM t2 WHERE t2.a = t1.a) ELSE 0 END as val FROM t1"
        self.assertEqual(
            ydb(sql),
            "SELECT CASE WHEN a = 1 THEN _u_1._col0 ELSE 0 END AS val FROM `t1` CROSS JOIN "
            "(SELECT MAX(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1",
        )

    def test_case_with_subquery_in_else(self):
        sql = "SELECT CASE WHEN a = 1 THEN 100 ELSE (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a) END as val FROM t1"
        self.assertEqual(
            ydb(sql),
            "SELECT CASE WHEN a = 1 THEN 100 ELSE _u_1._col0 END AS val FROM `t1` CROSS JOIN "
            "(SELECT MIN(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1",
        )

    def test_nested_case(self):
        sql = "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 'A' ELSE 'B' END ELSE 'C' END as result FROM t"
        self.assertEqual(
            ydb(sql),
            "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 'A' ELSE 'B' END ELSE 'C' END AS result FROM `t`",
        )

    # --- UPDATE ---------------------------------------------------------------

    def test_update_strips_table_alias(self):
        # YDB does not support UPDATE table AS alias; alias must be removed and
        # column references in SET/WHERE rewritten to unqualified names.
        sql = "UPDATE `user` AS u SET login = u.name WHERE u.id = 1"
        result = parse_one(sql, dialect="sqlite").sql(dialect="ydb")
        self.assertEqual(result, "UPDATE `user` SET login = name WHERE id = 1")

    def test_update_correlated_subquery_raises(self):
        # Correlated subqueries inside UPDATE cannot be decorrelated in YDB.
        # The generator must raise UnsupportedError so the caller knows manual rewriting is required.
        from sqlglot.errors import UnsupportedError
        sql = "UPDATE `user` AS u SET active = 0 WHERE NOT EXISTS (SELECT 1 FROM log AS l WHERE l.user_id = u.id)"
        with self.assertRaises(UnsupportedError):
            parse_one(sql, dialect="sqlite").sql(dialect="ydb")


# ---------------------------------------------------------------------------
# YDB parser: new syntax features parsed and round-tripped
# ---------------------------------------------------------------------------

class TestYDBParser(Validator):
    """Identity tests for YDB-specific parser additions."""

    dialect = "ydb"
    maxDiff = None

    def assert_roundtrip_stable(self, sql: str) -> None:
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb", error_level=ErrorLevel.RAISE)
            if expression is not None
        )
        regenerated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(generated, dialect="ydb", error_level=ErrorLevel.RAISE)
            if expression is not None
        )
        self.assertEqual(generated, regenerated)

    # --- $varname -----------------------------------------------------------

    def test_dollar_variable_in_expr(self):
        self.validate_identity("$x + 1")

    def test_dollar_variable_as_table(self):
        self.validate_identity("SELECT * FROM $t AS t")

    def test_table_name_before_dollar_variable_table(self):
        self.validate_transpile(
            "SELECT * FROM `a` AS a JOIN `placeholder` $rows AS b ON a.id = b.id",
            "SELECT * FROM `a` AS a JOIN $rows AS b ON a.id = b.id",
        )

    def test_dollar_variable_in_select(self):
        self.validate_identity("SELECT $limit FROM `table`")

    # --- Module::Function() -------------------------------------------------

    def test_module_function_simple(self):
        self.validate_identity("DateTime::GetYear(ts)")

    def test_double_quoted_string_with_escaped_quotes(self):
        self.validate_transpile(
            'SELECT JSON_QUERY(data, "$.dynamicValues.\\"check in\\"") FROM `table`',
            "SELECT JSON_QUERY(data, '$.dynamicValues.\"check in\"') FROM `table`",
        )

    def test_module_function_nested(self):
        self.validate_identity(
            "DateTime::ShiftMonths(DateTime::StartOfMonth(ts), 1)"
        )

    def test_module_function_in_select(self):
        self.validate_identity(
            "SELECT DateTime::GetYear(ts) FROM `events`"
        )

    def test_module_function_math(self):
        self.validate_identity("Math::Round(x, -2)")

    def test_module_function_string_namespace(self):
        self.validate_identity("String::Contains(value, 'test')")

    # --- DECLARE $p AS Type -------------------------------------------------

    def test_declare_utf8(self):
        self.validate_identity("DECLARE $name AS Utf8")

    def test_declare_timestamp(self):
        self.validate_identity("DECLARE $ts AS Timestamp")

    def test_declare_uint64(self):
        self.validate_identity("DECLARE $id AS Uint64")

    def test_declare_trailing_block_comments(self):
        self.validate_identity(
            "DECLARE $id AS Uint64 /* GetItem{test} */ /* Page 0 */"
        )

    def test_declare_trailing_block_comments_roundtrip_stable(self):
        sql = "DECLARE $id AS Uint64 /* GetItem{test} */ /* Page 0 */"
        generated = parse_one(sql, dialect="ydb").sql(dialect="ydb")
        regenerated = parse_one(generated, dialect="ydb").sql(dialect="ydb")
        self.assertEqual(generated, regenerated)

    # --- FLATTEN [LIST|DICT] BY ---------------------------------------------

    def test_flatten_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN BY col")

    def test_flatten_list_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN LIST BY col")

    def test_flatten_dict_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN DICT BY col")

    def test_flatten_optional_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN OPTIONAL BY col")

    def test_flatten_by_alias(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN BY col AS item")

    def test_flatten_by_multiple_columns(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN BY (a, b)")

    def test_flatten_by_parenthesized_single_column(self):
        self.validate_identity("SELECT value, id FROM as_table($sample) FLATTEN BY (value)")

    def test_flatten_dict_by_alias_after_table_alias(self):
        self.validate_identity("SELECT * FROM `my_table` AS t FLATTEN DICT BY dict_column AS item")

    def test_flatten_by_named_expression(self):
        self.validate_identity(
            "SELECT * FROM `t` FLATTEN LIST BY (String::SplitToList(a, ';') AS a, b)"
        )

    def test_flatten_page_sample_snippet_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            """$sample = AsList(
    AsStruct(AsList('a','b','c') AS value, CAST(1 AS Uint32) AS id),
    AsStruct(AsList('d') AS value, CAST(2 AS Uint32) AS id),
    AsStruct(AsList() AS value, CAST(3 AS Uint32) AS id)
);

SELECT value, id FROM as_table($sample) FLATTEN BY (value);"""
        )

    def test_flatten_dict_page_snippet_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            """SELECT
  t.item.0 AS key,
  t.item.1 AS value,
  t.dict_column AS original_dict,
  t.other_column AS other
FROM my_table AS t
FLATTEN DICT BY dict_column AS item;"""
        )

    def test_flatten_list_multiple_columns_page_snippet_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            """SELECT * FROM (
    SELECT
        AsList(1, 2, 3) AS a,
        AsList("x", "y", "z") AS b
) FLATTEN LIST BY (a, b);"""
        )

    def test_flatten_named_expression_page_snippet_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            """SELECT * FROM (
    SELECT
        "1;2;3" AS a,
        AsList("x", "y", "z") AS b
) FLATTEN LIST BY (String::SplitToList(a, ";") as a, b);"""
        )

    def test_flatten_columns_page_snippet_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            """SELECT x, y, z
FROM (
  SELECT
    AsStruct(
        1 AS x,
        "foo" AS y),
    AsStruct(
        false AS z)
) FLATTEN COLUMNS;"""
        )

    def test_flatten_by_multiple_columns_requires_parentheses(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT * FROM `t` FLATTEN BY a, b")

    def test_flatten_by_arbitrary_expression_requires_alias_and_parentheses(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT * FROM `t` FLATTEN BY ListSkip(col, 1)")

    def test_flatten_by_parenthesized_arbitrary_expression_requires_alias(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT * FROM `t` FLATTEN BY (ListSkip(col, 1))")

    def test_flatten_columns(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN COLUMNS")

    def test_flatten_by_dollar_table(self):
        self.validate_identity("SELECT k, v FROM $t FLATTEN BY vals")

    # --- T? optional types --------------------------------------------------

    def test_optional_type_utf8(self):
        self.validate_identity("CAST(x AS Optional<Utf8>)")

    def test_optional_type_utf8_shorthand(self):
        self.validate_transpile("CAST(x AS Utf8?)", "CAST(x AS Optional<Utf8>)")

    def test_optional_type_timestamp(self):
        self.validate_identity("CAST(x AS Optional<Timestamp>)")

    def test_optional_type_uint64(self):
        self.validate_identity("CAST(x AS Optional<Uint64>)")

    def test_non_optional_unchanged(self):
        self.validate_identity("CAST(x AS Utf8)")

    # --- Container types ----------------------------------------------------

    def test_list(self):
        self.validate_identity("CAST(x AS List<Int32>)")

    def test_dict(self):
        self.validate_identity("CAST(x AS Dict<Utf8, Int64>)")

    def test_set(self):
        self.validate_identity("CAST(x AS Set<Utf8>)")

    def test_lowercase_set(self):
        self.validate_transpile("CAST(x AS set<Utf8>)", "CAST(x AS Set<Utf8>)")

    def test_tuple(self):
        self.validate_identity("CAST(x AS Tuple<Int32, Utf8>)")

    def test_optional_list(self):
        self.validate_identity("CAST(x AS Optional<List<Int32>>)")

    def test_list_optional_element(self):
        self.validate_identity("CAST(x AS List<Optional<Utf8>>)")

    def test_nested_containers(self):
        self.validate_identity("CAST(x AS List<Dict<Utf8, Int64>>)")

    def test_optional_tuple(self):
        self.validate_identity("CAST(x AS Optional<Tuple<Int32, Utf8>>)")

    def test_tuple_shorthand_optional(self):
        self.validate_transpile("CAST(x AS Tuple<Int32, Utf8>?)", "CAST(x AS Optional<Tuple<Int32, Utf8>>)")

    def test_struct_type(self):
        self.validate_identity("DECLARE $x AS Struct<a: Int64, b: Utf8>")

    def test_list_struct_type(self):
        self.validate_identity("DECLARE $x AS List<Struct<a: Int64, b: Utf8>>")

    def test_struct_type_trailing_comma(self):
        self.validate_identity(
            "DECLARE $x AS List<Struct<a: Optional<Utf8>, b: Utf8,>>",
            write_sql="DECLARE $x AS List<Struct<a: Optional<Utf8>, b: Utf8>>",
        )

    # --- ASSUME ORDER BY ----------------------------------------------------

    def test_assume_order_by(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id")

    def test_assume_order_by_desc(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id DESC")

    def test_assume_order_by_doc_alias_snippet(self):
        self.validate_identity(
            'SELECT key || "suffix" AS key, -CAST(subkey AS Int32) AS subkey '
            "FROM my_table ASSUME ORDER BY key, subkey DESC",
            write_sql=(
                "SELECT key || 'suffix' AS key, -CAST(subkey AS Int32) AS subkey "
                "FROM `my_table` ASSUME ORDER BY key, subkey DESC"
            ),
        )

    def test_assume_order_by_doc_rejects_expressions(self):
        with self.assertRaises(UnsupportedError):
            parse_one("SELECT key FROM my_table ASSUME ORDER BY key + 1", dialect="ydb").sql(dialect="ydb")

    def test_group_compact_by(self):
        self.validate_transpile(
            "SELECT source_id FROM `table` GROUP COMPACT BY source_id",
            "SELECT source_id FROM `table` GROUP COMPACT BY source_id AS source_id",
        )

    def test_group_by_doc_basic_count_snippet(self):
        self.validate_transpile(
            "SELECT key, COUNT(*) FROM my_table GROUP BY key",
            "SELECT key, COUNT(*) FROM `my_table` GROUP BY key AS key",
        )

    def test_group_by_doc_expression_alias_snippet(self):
        self.validate_transpile(
            "SELECT double_key, COUNT(*) FROM my_table GROUP BY key + key AS double_key",
            "SELECT double_key, COUNT(*) FROM `my_table` GROUP BY key + key AS double_key",
        )

    def test_group_by_doc_multiple_aliases_snippet(self):
        self.assert_roundtrip_stable(
            """SELECT
   double_key,
   COUNT(*) AS group_size,
   SUM(key + subkey) AS sum1,
   CAST(SUM(1 + 2) AS String) AS sum2,
   SUM(SUM(1) + key) AS sum3,
   key AS k1,
   key * 2 AS dk1
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey AS sk;"""
        )

    def test_group_by_doc_session_window_snippet(self):
        self.validate_transpile(
            "SELECT user, session_start, SessionStart() AS same_session_start, "
            "COUNT(*) AS session_size, SUM(value) AS sum_over_session FROM my_table "
            "GROUP BY user, SessionWindow(ts, timeout) AS session_start",
            "SELECT user, session_start, SessionStart() AS same_session_start, "
            "COUNT(*) AS session_size, SUM(value) AS sum_over_session FROM `my_table` "
            "GROUP BY user AS user, SessionWindow(ts, timeout) AS session_start",
        )

    def test_group_by_doc_extended_session_window_snippet(self):
        self.assert_roundtrip_stable(
            """$max_len = 1000; $timeout = 100;
$init = ($row) -> (AsTuple($row.ts, $row.ts)); $update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start,
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session
FROM my_table
GROUP BY user, SessionWindow(ts, $init, $update, $calculate) AS session_start;"""
        )

    def test_group_by_doc_rollup_grouping_sets_snippet(self):
        self.validate_transpile(
            "SELECT column1, column2, column3, "
            "CASE GROUPING(column1, column2, column3) "
            "WHEN 1 THEN 'Subtotal: column1 and column2' "
            "WHEN 3 THEN 'Subtotal: column1' "
            "WHEN 4 THEN 'Subtotal: column2 and column3' "
            "WHEN 6 THEN 'Subtotal: column3' "
            "WHEN 7 THEN 'Grand total' "
            "ELSE 'Individual group' END AS subtotal, "
            "COUNT(*) AS rows_count FROM my_table "
            "GROUP BY ROLLUP(column1, column2, column3), "
            "GROUPING SETS ((column2, column3), (column3))",
            "SELECT column1, column2, column3, "
            "CASE GROUPING(column1, column2, column3) "
            "WHEN 1 THEN 'Subtotal: column1 and column2' "
            "WHEN 3 THEN 'Subtotal: column1' "
            "WHEN 4 THEN 'Subtotal: column2 and column3' "
            "WHEN 6 THEN 'Subtotal: column3' "
            "WHEN 7 THEN 'Grand total' "
            "ELSE 'Individual group' END AS subtotal, "
            "COUNT(*) AS rows_count FROM `my_table` "
            "GROUP BY ROLLUP (column1, column2, column3), "
            "GROUPING SETS ((column2, column3), (column3))",
        )

    def test_group_by_doc_distinct_aggregate_snippet(self):
        self.validate_transpile(
            "SELECT key, COUNT(DISTINCT value) AS count FROM my_table "
            "GROUP BY key ORDER BY count DESC LIMIT 3",
            "SELECT key, COUNT(DISTINCT value) AS count FROM `my_table` "
            "GROUP BY key AS key ORDER BY count DESC LIMIT 3",
        )

    def test_group_by_doc_group_compact_snippet(self):
        self.validate_transpile(
            "SELECT key, COUNT(DISTINCT value) AS count FROM my_table "
            "GROUP COMPACT BY key ORDER BY count DESC LIMIT 3",
            "SELECT key, COUNT(DISTINCT value) AS count FROM `my_table` "
            "GROUP COMPACT BY key AS key ORDER BY count DESC LIMIT 3",
        )

    def test_group_by_doc_having_snippet(self):
        self.validate_transpile(
            "SELECT key FROM my_table GROUP BY key HAVING COUNT(value) > 100",
            "SELECT key FROM `my_table` GROUP BY key AS key HAVING COUNT(value) > 100",
        )

    def test_left_only_join(self):
        self.validate_identity("SELECT * FROM `a` LEFT ONLY JOIN `b` USING (id)")

    def test_join_doc_default_inner_join_type(self):
        self.validate_identity("SELECT * FROM `a` JOIN `b` ON a.id = b.id")

    def test_join_doc_join_types(self):
        cases = [
            "SELECT * FROM `a` LEFT JOIN `b` USING (key)",
            "SELECT * FROM `a` RIGHT JOIN `b` USING (key)",
            "SELECT * FROM `a` FULL JOIN `b` USING (key)",
            "SELECT * FROM `a` LEFT SEMI JOIN `b` USING (key)",
            "SELECT * FROM `a` RIGHT SEMI JOIN `b` USING (key)",
            "SELECT * FROM `a` LEFT ONLY JOIN `b` USING (key)",
            "SELECT * FROM `a` RIGHT ONLY JOIN `b` USING (key)",
            "SELECT * FROM `a` CROSS JOIN `b`",
            "SELECT * FROM `a` EXCLUSION JOIN `b` USING (key)",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_join_doc_full_join_using_snippet(self):
        self.validate_transpile(
            "SELECT a.value AS a_value, b.value AS b_value "
            "FROM a_table AS a FULL JOIN b_table AS b USING (key)",
            "SELECT a.value AS a_value, b.value AS b_value "
            "FROM `a_table` AS a FULL JOIN `b_table` AS b USING (key)",
        )

    def test_join_doc_full_join_on_snippet(self):
        self.validate_transpile(
            "SELECT a.value AS a_value, b.value AS b_value "
            "FROM a_table AS a FULL JOIN b_table AS b ON a.key = b.key",
            "SELECT a.value AS a_value, b.value AS b_value "
            "FROM `a_table` AS a FULL JOIN `b_table` AS b ON a.key = b.key",
        )

    def test_join_doc_cross_then_left_join_snippet(self):
        self.validate_transpile(
            "SELECT a.value AS a_value, b.value AS b_value, c.column2 "
            "FROM a_table AS a CROSS JOIN b_table AS b "
            "LEFT JOIN c_table AS c ON c.ref = a.key AND c.column1 = b.value",
            "SELECT a.value AS a_value, b.value AS b_value, c.column2 AS column2 "
            "FROM `a_table` AS a CROSS JOIN `b_table` AS b "
            "LEFT JOIN `c_table` AS c ON c.ref = a.key AND c.column1 = b.value",
        )

    def test_join_doc_index_lookup_join_snippet(self):
        self.validate_transpile(
            "SELECT a.value AS a_value, b.value AS b_value FROM a_table AS a "
            "INNER JOIN b_table VIEW b_index_ref AS b ON a.ref = b.ref",
            "SELECT a.value AS a_value, b.value AS b_value FROM `a_table` AS a "
            "INNER JOIN `b_table` VIEW b_index_ref AS b ON a.ref = b.ref",
        )

    def test_join_doc_on_allows_equality_over_expressions(self):
        self.validate_transpile(
            "SELECT * FROM a JOIN b ON a.key + 1 = b.key",
            "SELECT * FROM `a` JOIN `b` ON a.key + 1 = b.key",
        )

    def test_join_doc_on_moves_non_equality_to_where(self):
        self.validate_transpile(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND b.value > 0",
            "SELECT * FROM `a` LEFT JOIN `b` ON a.id = b.id WHERE b.value > 0",
        )

    def test_exclusion_join(self):
        self.validate_identity("SELECT t.* FROM $t AS t EXCLUSION JOIN `m` AS m ON t.id = m.id")

    def test_named_select_with_exclusion_join(self):
        expressions = parse(
            "$t = SELECT * FROM `table_1` WHERE source_event_type != 'delete';\n"
            "SELECT t.* FROM $t AS t EXCLUSION JOIN `table_2` AS m "
            "ON t.iam_authorized_key_id = m.iam_authorized_key_id",
            dialect="ydb",
        )
        generated = ";\n".join(expression.sql(dialect="ydb") for expression in expressions if expression)
        self.assertEqual(
            generated,
            "$t = (SELECT * FROM `table_1` WHERE source_event_type <> 'delete');\n\n"
            "SELECT t.* FROM $t AS t EXCLUSION JOIN `table_2` AS m "
            "ON t.iam_authorized_key_id = m.iam_authorized_key_id",
        )

    def test_without_projection_after_table_star(self):
        self.validate_transpile(
            "SELECT b.*, WITHOUT b.`date`, b.scale FROM `t` AS b",
            "SELECT b.* WITHOUT (b.`date`, b.scale) FROM `t` AS b",
        )

    def test_from_first_join_with_without_projection(self):
        self.validate_transpile(
            "FROM $t AS a LEFT JOIN (SELECT * FROM $t) AS b USING(id) "
            "SELECT a.id, b.*, WITHOUT b.id WHERE a.id = 1",
            "SELECT a.id AS id, b.* WITHOUT (b.id) FROM $t AS a "
            "LEFT JOIN (SELECT * FROM $t) AS b USING (id) WHERE a.id = 1",
        )

    def test_assume_order_by_multi_col(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id, name")

    def test_regular_order_by_unchanged(self):
        self.validate_identity("SELECT * FROM `t` ORDER BY id")

    def test_yql_interval_function_before_group_by(self):
        self.validate_transpile(
            "SELECT device_id FROM `table` WHERE CurrentUtcDate() - Interval('P7D') > ts GROUP BY device_id",
            "SELECT device_id FROM `table` WHERE CurrentUtcDate() - Interval('P7D') > ts GROUP BY device_id AS device_id",
        )

    def test_leading_bom_before_named_expression(self):
        expressions = parse("\ufeff\n$date_parse = DateTime::Parse('format');", dialect="ydb")
        self.assertEqual(expressions[0].sql(dialect="ydb"), "$date_parse = DateTime::Parse('format')")

    def test_leading_mojibake_bom_before_named_expression(self):
        expressions = parse("ï»¿\n$date_parse = DateTime::Parse('format');", dialect="ydb")
        self.assertEqual(expressions[0].sql(dialect="ydb"), "$date_parse = DateTime::Parse('format')")

    def test_struct_literal(self):
        self.validate_identity("$profile = AsList(<|user_id: 'u1', description: NULL|>)")

    def test_json_path_with_quoted_key(self):
        self.validate_transpile(
            'SELECT JSON_EXISTS(item_result, "$.\'P_008 device playback test\'") FROM `t`',
            "SELECT JSON_EXISTS(item_result, '$.''P_008 device playback test''') FROM `t`",
        )

    # --- Lambda expressions -------------------------------------------------

    def test_lambda_return_body_with_semicolon(self):
        self.validate_identity(
            "ListFilter($counterIds, ($x) -> {RETURN $x > $startCounterId;})",
            write_sql="ListFilter($counterIds, ($x) -> ($x > $startCounterId))",
        )

    def test_lambda_lowercase_return_body_with_semicolon(self):
        self.validate_identity(
            "ListMap($contacts, ($i) -> { return Digest::MurMurHash($i); })",
            write_sql="ListMap($contacts, ($i) -> (Digest::MurMurHash($i)))",
        )

    def test_in_compact_parameter(self):
        self.validate_identity(
            "SELECT CounterID FROM `counter_stat` WHERE CounterID IN COMPACT $ids"
        )

    def test_lambda_parenthesized_body(self):
        self.validate_identity(
            "($x, $y?) -> ($x + ($y ?? 0))",
            write_sql="($x, $y?) -> ($x + (COALESCE($y, 0)))",
        )

    def test_lambda_block_with_named_expression(self):
        self.validate_identity(
            '($y) -> { $prefix = "x"; RETURN $prefix || $y; }',
            write_sql="($y) -> { $prefix = 'x'; RETURN $prefix || $y }",
        )

    def test_lambda_block_calls_named_lambda(self):
        self.validate_identity(
            "$f = ($key1) -> { $INIT = 0xDEADC0DEul; $Combine = ($first, $second) -> { RETURN Digest::FarmHashFingerprint2($first, $second) }; RETURN $Combine($INIT, Digest::FarmHashFingerprint64($key1)) ^ 1ul; }",
            write_sql="$f = ($key1) -> { $INIT = 0xDEADC0DEul; $Combine = ($first, $second) -> { RETURN Digest::FarmHashFingerprint2($first, $second) }; RETURN $Combine($INIT, Digest::FarmHashFingerprint64($key1)) ^ 1ul }",
        )

    def test_lambda_named_expression_with_in_compact_roundtrip_stable(self):
        sql = (
            "$ids = ListFilter($counterIds, ($x) -> {RETURN $x > $startCounterId;});\n"
            "SELECT CounterID FROM `counter_stat` WHERE CounterID IN COMPACT $ids"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        regenerated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(generated, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(generated, regenerated)

    def test_line_directive_stays_line_comment(self):
        self.validate_identity("--!syntax_v1\nSELECT 1")
        self.validate_identity("--!ansi_lexer\nSELECT 1")

    def test_line_directive_before_pragma(self):
        self.validate_identity(
            "--!syntax_v1\nPRAGMA TablePathPrefix = '/db/name'",
        )

    # --- Lexical structure --------------------------------------------------

    def test_lexer_doc_comments_are_whitespace(self):
        sql = "SELECT 1; -- A single-line comment\n/* Some multi-line comment */ SELECT 2"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "SELECT 1;\n /* A single-line comment */;\n/* Some multi-line comment */ SELECT 2",
            generated,
        )

    def test_lexer_doc_identifiers(self):
        self.validate_identity(
            "SELECT my_column FROM my_table",
            write_sql="SELECT my_column FROM `my_table`",
        )
        self.validate_identity(
            "SELECT `column with space` from T",
            write_sql="SELECT `column with space` FROM `T`",
        )
        self.validate_identity("SELECT * FROM `my_dir/my_table`")
        self.validate_identity(
            "SELECT `select` FROM T",
            write_sql="SELECT `select` FROM `T`",
        )

    def test_lexer_doc_backtick_identifier_escapes(self):
        self.validate_identity(
            "SELECT 1 as `column with\\n newline, \\x0a newline and \\` backtick `",
            write_sql="SELECT 1 AS `column with\n newline, \\x0a newline and `` backtick `",
        )

    def test_lexer_doc_ansi_double_quoted_identifier(self):
        self.validate_identity(
            '--!ansi_lexer\nSELECT 1 as "column with "" double quote"',
            write_sql='--!ansi_lexer\nSELECT 1 AS `column with " double quote`',
        )

    def test_lexer_doc_string_literals(self):
        self.validate_identity(
            "SELECT 'string with\\n newline, \\x0a newline and \\' backtick '",
            write_sql="SELECT 'string with\\n newline, \\\\x0a newline and '' backtick '",
        )
        self.validate_identity(
            'SELECT "string with\\n newline, \\x0a newline and \\" backtick "',
            write_sql='SELECT \'string with\\n newline, \\\\x0a newline and " backtick \'',
        )
        self.validate_identity(
            "--!ansi_lexer\nSELECT 'string with '' quote'",
            write_sql="--!ansi_lexer\nSELECT 'string with '' quote'",
        )

    def test_lexer_doc_multiline_string_literals(self):
        sql = "$text = @@some\nmultiline\ntext@@;\nSELECT LENGTH($text)"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$text = @@some\nmultiline\ntext@@;\nSELECT Unicode::GetLength($text)",
            generated,
        )

    def test_lexer_doc_multiline_string_escaped_double_at(self):
        sql = "$text = @@some\nmultiline with double at: @@@@\ntext@@;\nSELECT $text"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(sql, generated)

    def test_lexer_doc_typed_string_literals(self):
        self.validate_identity(
            'SELECT "foo"u, \'[1;2]\'y, @@{"a":null}@@j',
            write_sql='SELECT \'foo\'u, \'[1;2]\'y, @@{"a":null}@@j',
        )
        self.validate_identity("SELECT 'foo's, 'foo'u, 'foo'y, 'foo'j")

    def test_lexer_doc_numeric_literals(self):
        self.validate_identity(
            "SELECT 123l AS `Int64`, 0b01u AS `Uint32`, 0xfful AS `Uint64`, "
            "0o7ut AS `Uint8`, 456s AS `Int16`, 1.2345f AS `Float`"
        )
        self.validate_identity("SELECT 7t AS `Int8`, 8us AS `Uint16`")

    # --- Expressions --------------------------------------------------------

    def test_expressions_doc_string_concatenation(self):
        self.validate_identity('SELECT "fo" || "o"', write_sql="SELECT 'fo' || 'o'")

    def test_expressions_doc_pattern_matching(self):
        self.validate_identity(
            "SELECT * FROM my_table WHERE string_column REGEXP '\\\\d+'",
            write_sql="SELECT * FROM `my_table` WHERE Re2::Grep('\\\\d+')(string_column)",
        )
        self.validate_identity(
            "SELECT string_column RLIKE '^[a-z]+', string_column MATCH 'foo' FROM my_table",
            write_sql=(
                "SELECT Re2::Grep('^[a-z]+')(string_column), "
                "Re2::Match('foo')(string_column) FROM `my_table`"
            ),
        )
        self.validate_identity(
            "SELECT string_column LIKE '___!_!_!_!!!!!!' ESCAPE '!' FROM my_table",
            write_sql=(
                "SELECT string_column LIKE '___!_!_!_!!!!!!' ESCAPE '!' "
                "FROM `my_table`"
            ),
        )
        self.validate_identity(
            "SELECT * FROM my_table WHERE key LIKE 'foo%bar'",
            write_sql="SELECT * FROM `my_table` WHERE key LIKE 'foo%bar'",
        )

    def test_expressions_doc_operators(self):
        self.validate_identity("SELECT 2 + 2")
        self.validate_identity("SELECT 0.0 / 0.0")
        self.validate_identity("SELECT 2 > 1")
        self.validate_identity(
            "SELECT a == b, a != b, a <> b",
            write_sql="SELECT a = b, a <> b, a <> b",
        )
        self.validate_identity("SELECT 3 > 0 AND false", write_sql="SELECT 3 > 0 AND FALSE")
        self.validate_identity("SELECT a XOR b")
        self.validate_identity(
            "SELECT key << 10 AS key, ~value AS value FROM my_table",
            write_sql="SELECT key << 10 AS key, ~value AS value FROM `my_table`",
        )
        self.validate_identity("SELECT a |< b, a >| b")
        self.validate_identity(
            "SELECT a ?? b ?? c",
            write_sql="SELECT COALESCE(COALESCE(a, b), c)",
        )

    def test_expressions_doc_predicates(self):
        self.validate_identity(
            "SELECT key FROM my_table WHERE value IS NOT NULL",
            write_sql="SELECT key FROM `my_table` WHERE value IS NOT NULL",
        )
        self.validate_identity("SELECT a IS DISTINCT FROM b, a IS NOT DISTINCT FROM b")
        self.validate_identity(
            "SELECT * FROM my_table WHERE key BETWEEN 10 AND 20",
            write_sql="SELECT * FROM `my_table` WHERE key BETWEEN 10 AND 20",
        )

    def test_expressions_doc_in(self):
        self.validate_identity(
            "SELECT column IN (1, 2, 3) FROM my_table",
            write_sql="SELECT column IN (1, 2, 3) FROM `my_table`",
        )
        self.validate_identity(
            'SELECT * FROM my_table WHERE string_column IN ("a", "b", "c")',
            write_sql="SELECT * FROM `my_table` WHERE string_column IN ('a', 'b', 'c')",
        )

        sql = '$foo = AsList(1, 2, 3);\nSELECT 1 IN $foo'
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual("$foo = AsList(1, 2, 3);\nSELECT 1 IN $foo", generated)

        sql = (
            "$values = (SELECT column + 1 FROM table);\n"
            "SELECT * FROM my_table WHERE column1 IN COMPACT $values "
            "AND column2 NOT IN (SELECT other_column FROM other_table)"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$values = (SELECT column + 1 FROM `table`);\n\n"
            "SELECT * FROM `my_table` WHERE column1 IN COMPACT $values "
            "AND NOT column2 IN (SELECT other_column AS other_column FROM `other_table`)",
            generated,
        )

    def test_expressions_doc_as_cast_bitcast(self):
        self.validate_identity(
            "SELECT key AS k FROM my_table",
            write_sql="SELECT key AS k FROM `my_table`",
        )
        self.validate_identity(
            "SELECT t.key FROM my_table AS t",
            write_sql="SELECT t.key AS key FROM `my_table` AS t",
        )
        self.validate_identity(
            "SELECT MyFunction(key, 123 AS my_optional_arg) FROM my_table",
            write_sql="SELECT MyFunction(key, 123 AS my_optional_arg) FROM `my_table`",
        )
        self.validate_identity(
            'SELECT CAST("12345" AS Double), CAST(1.2345 AS Uint8), CAST(12345 AS String), '
            'CAST("1.2345" AS Decimal(5, 2)), CAST("xyz" AS Uint64) IS NULL, '
            "CAST(-1 AS Uint16) IS NULL, CAST([-1, 0, 1] AS List<Uint8?>), "
            'CAST(["3.14", "bad", "42"] AS List<Float>), CAST(255 AS Uint8), '
            "CAST(256 AS Uint8) IS NULL",
            write_sql=(
                "SELECT CAST('12345' AS Double), CAST(1.2345 AS Uint8), "
                "CAST(12345 AS String), CAST('1.2345' AS Decimal(5, 2)), "
                "CAST('xyz' AS Uint64) IS NULL, CAST(-1 AS Uint16) IS NULL, "
                "CAST(AsList(-1, 0, 1) AS List<Optional<Uint8>>), "
                "CAST(AsList('3.14', 'bad', '42') AS List<Float>), "
                "CAST(255 AS Uint8), CAST(256 AS Uint8) IS NULL"
            ),
        )
        self.validate_identity(
            "SELECT BITCAST(100000ul AS Uint32), BITCAST(100000ul AS Int16), "
            "BITCAST(100000ul AS Uint16), BITCAST(-1 AS Int16), BITCAST(-1 AS Uint16)"
        )

    def test_expressions_doc_case(self):
        self.validate_identity(
            'SELECT CASE WHEN value > 0 THEN "positive" ELSE "negative" END FROM my_table',
            write_sql="SELECT CASE WHEN value > 0 THEN 'positive' ELSE 'negative' END FROM `my_table`",
        )
        self.validate_identity(
            'SELECT CASE value WHEN 0 THEN "zero" WHEN 1 THEN "one" ELSE "not zero or one" END FROM my_table',
            write_sql=(
                "SELECT CASE value WHEN 0 THEN 'zero' WHEN 1 THEN 'one' "
                "ELSE 'not zero or one' END FROM `my_table`"
            ),
        )

    def test_expressions_doc_named_expressions(self):
        sql = (
            "$multiplier = 712;\n"
            "SELECT a * $multiplier, b * $multiplier, (a + b) * $multiplier FROM abc_table;\n"
            "$multiplier = c;\n"
            "SELECT a * $multiplier FROM abc_table"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$multiplier = 712;\n"
            "SELECT a * $multiplier, b * $multiplier, (a + b) * $multiplier FROM `abc_table`;\n"
            "$multiplier = c;\n"
            "SELECT a * $multiplier FROM `abc_table`",
            generated,
        )

        sql = (
            "$intermediate = (SELECT value * value AS square, value FROM my_table);\n"
            "SELECT a.square * b.value FROM $intermediate AS a "
            "INNER JOIN $intermediate AS b ON a.value == b.square"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$intermediate = (SELECT value * value AS square, value FROM `my_table`);\n\n"
            "SELECT a.square * b.value FROM $intermediate AS a "
            "INNER JOIN $intermediate AS b ON a.value = b.square",
            generated,
        )

        self.validate_identity(
            '$a, $_, $c = AsTuple(1, 5u, "test")',
            write_sql="$a, $_, $c = AsTuple(1, 5u, 'test')",
        )
        self.validate_identity("$x, $y = AsTuple($y, $x)")

    def test_expressions_doc_table_expressions(self):
        sql = "$input = SELECT a, b, c FROM T;\nSELECT * FROM $input"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual("$input = (SELECT a, b, c FROM `T`);\n\nSELECT * FROM $input AS input", generated)

        self.validate_identity(
            "SELECT * FROM T WHERE key IN (SELECT k FROM T1)",
            write_sql=(
                "SELECT * FROM `T` LEFT JOIN (SELECT k AS k FROM `T1` GROUP BY k AS k) AS _u_0 "
                "ON key = _u_0.k WHERE _u_0.k IS NOT NULL"
            ),
        )

        sql = "$count = SELECT COUNT(*) FROM T;\nSELECT * FROM T ORDER BY key LIMIT $count / 2"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$count = (SELECT COUNT(*) FROM `T`);\n\nSELECT * FROM `T` ORDER BY key LIMIT $count / 2",
            generated,
        )

    def test_expressions_doc_lambda_functions(self):
        sql = (
            '$f = ($y) -> { $prefix = "x"; RETURN $prefix || $y; };\n'
            '$g = ($y) -> ("x" || $y);\n'
            "$h = ($x, $y?) -> ($x + ($y ?? 0));\n"
            'SELECT $f("y"), $g("z"), $h(1), $h(2, 3)'
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$f = ($y) -> { $prefix = 'x'; RETURN $prefix || $y };\n"
            "$g = ($y) -> ('x' || $y);\n"
            "$h = ($x, $y?) -> ($x + (COALESCE($y, 0)));\n"
            "SELECT $f('y'), $g('z'), $h(1), $h(2, 3)",
            generated,
        )

        sql = '$f = ($x, $_) -> ($x || "suffix");\nSELECT $f("prefix_", "whatever")'
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(
            "$f = ($x, $_) -> ($x || 'suffix');\nSELECT $f('prefix_', 'whatever')",
            generated,
        )

    def test_expressions_doc_container_access(self):
        self.validate_identity(
            'SELECT t.struct.member, t.tuple.7, t.dict["key"], t.list[7] FROM my_table AS t',
            write_sql=(
                "SELECT t.struct.member AS member, t.tuple.7 AS `7`, "
                "t.dict['key'], t.list[7] FROM `my_table` AS t"
            ),
        )
        self.validate_identity("SELECT Sample::ReturnsStruct().member")

    # --- Named expressions $name = expr -------------------------------------

    def test_named_expression_subquery(self):
        self.validate_identity("$t = (SELECT 1)")

    def test_named_expression_from_table(self):
        self.validate_identity("$t = (SELECT * FROM `table`)")

    def test_named_expression_chain_roundtrip_stable(self):
        sql = (
            "$abc_services = SELECT id FROM `table_1`;\n"
            "$max_sync_time = SELECT MAX(_cq_sync_time) FROM `table_2` WHERE organization_id = $org_id;\n"
            "$result = SELECT ycmcc.folder_id AS folder_id FROM `table_2` AS ycmcc "
            "JOIN $abc_services AS ycrc ON ycmcc.cloud_id = ycrc.id "
            "WHERE ycmcc._cq_sync_time = $max_sync_time;\n"
            "SELECT * FROM $result"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        regenerated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(generated, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(generated, regenerated)

    def test_named_expression_chain_with_empty_statement_roundtrip_stable(self):
        sql = "$a = SELECT 1;;\nSELECT * FROM $a"
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb")
            if expression is not None
        )
        regenerated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(generated, dialect="ydb")
            if expression is not None
        )
        self.assertEqual(generated, regenerated)


class TestYDBAdvancedSyntax(Validator):
    """Parser and generator coverage for less common YQL constructs."""

    dialect = "ydb"
    maxDiff = None

    def assert_roundtrip_stable(self, sql: str) -> None:
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb", error_level=ErrorLevel.RAISE)
            if expression is not None
        )
        regenerated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(generated, dialect="ydb", error_level=ErrorLevel.RAISE)
            if expression is not None
        )
        self.assertEqual(generated, regenerated)

    def test_variable_call_expression(self):
        sql = (
            "$grep = Re2::Grep($needle);\n"
            "SELECT * FROM `t` WHERE $grep(Unicode::ToLower(name))"
        )
        generated = ";\n".join(
            expression.sql(dialect="ydb")
            for expression in parse(sql, dialect="ydb", error_level=ErrorLevel.RAISE)
            if expression is not None
        )
        self.assertEqual(
            "$grep = Re2::Grep($needle);\nSELECT * FROM `t` WHERE $grep(Unicode::ToLower(name))",
            generated,
        )

    def test_curried_module_function_call(self):
        self.validate_identity(
            "SELECT DateTime::Format('%Y-%m-%d')(created_at) AS created_at FROM `t`"
        )

    def test_struct_type_with_quoted_field_name(self):
        self.validate_identity(
            "DECLARE $items AS List<Struct<'source_id': Utf8>>",
            write_sql="DECLARE $items AS List<Struct<'source_id': Utf8>>",
        )

    def test_declare_quoted_type_name(self):
        self.validate_identity('DECLARE $uid AS "UserId"')

    def test_utf8_string_literal_suffix(self):
        self.validate_identity("SELECT 'value'u AS value")

    def test_json_string_literal_suffix(self):
        self.validate_identity(
            "SELECT options ?? '[]'j AS options FROM `zones`",
            write_sql="SELECT COALESCE(options, '[]'j) AS options FROM `zones`",
        )

    def test_json_value_returning_type(self):
        self.validate_identity(
            "SELECT JSON_VALUE(payload, '$.size' RETURNING Int64) AS size FROM `events`"
        )

    def test_json_value_passing_and_on_clauses(self):
        self.validate_identity(
            "SELECT JSON_VALUE(payload, '$.value + $delta' PASSING 1 AS delta RETURNING Int64 DEFAULT 0 ON EMPTY ERROR ON ERROR)"
        )

    def test_json_value_null_on_empty_and_default_on_error(self):
        self.validate_identity(
            "SELECT JSON_VALUE(payload, '$.age' RETURNING Uint64 NULL ON EMPTY DEFAULT 20 ON ERROR)"
        )

    def test_json_value_multiple_passing_items(self):
        self.validate_identity(
            "SELECT JSON_VALUE(payload, '$.timestamp - $Now + $Hour' PASSING 24 * 60 AS Hour, CurrentUtcTimestamp() AS \"Now\" RETURNING Timestamp)"
        )

    def test_json_value_default_string_on_empty(self):
        self.validate_identity(
            "SELECT JSON_VALUE(payload, '$.name' RETURNING String DEFAULT \"empty\" ON EMPTY NULL ON ERROR)",
            write_sql="SELECT JSON_VALUE(payload, '$.name' RETURNING String DEFAULT 'empty' ON EMPTY NULL ON ERROR)",
        )

    def test_json_value_with_complex_group_by(self):
        self.assert_roundtrip_stable(
            "SELECT SUM(CASE COALESCE(CAST(JSON_VALUE(statistics, '$.retries') AS Uint64), 0) "
            "WHEN 0 THEN 0 ELSE 1 END) AS with_retries FROM `jobs` "
            "GROUP BY CASE WHEN flowLaunchId IS NOT NULL THEN 2 ELSE 1 END AS flow_engine_version"
        )

    def test_json_exists_simple(self):
        self.validate_identity("SELECT JSON_EXISTS(payload, '$.name') FROM `events`")

    def test_json_exists_passing_and_on_error(self):
        cases = [
            "SELECT JSON_EXISTS(payload, '$.items[$index]' PASSING 0 AS index TRUE ON ERROR)",
            "SELECT JSON_EXISTS(payload, '$.items[$Index]' PASSING 0 AS \"Index\" FALSE ON ERROR)",
            "SELECT JSON_EXISTS(payload, '$.name' UNKNOWN ON ERROR)",
            "SELECT JSON_EXISTS(payload, '$.name' ERROR ON ERROR)",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_json_query_simple(self):
        self.validate_identity("SELECT JSON_QUERY(payload, '$.items') FROM `events`")

    def test_json_query_wrapper_modes(self):
        cases = [
            "SELECT JSON_QUERY(payload, '$.items' WITHOUT ARRAY WRAPPER)",
            "SELECT JSON_QUERY(payload, '$.items' WITH ARRAY WRAPPER)",
            "SELECT JSON_QUERY(payload, '$.items' WITH CONDITIONAL ARRAY WRAPPER)",
            "SELECT JSON_QUERY(payload, '$.items' WITH UNCONDITIONAL ARRAY WRAPPER)",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_json_query_passing_and_on_clauses(self):
        cases = [
            "SELECT JSON_QUERY(payload, '$.items[$Index]' PASSING 0 AS \"Index\" WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY ERROR ON ERROR)",
            "SELECT JSON_QUERY(payload, '$.items' EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

    def test_tuple_expression_in_named_query_and_in_filter(self):
        self.validate_identity(
            "$keys = SELECT (a, b) FROM `lookup`;\n"
            "SELECT * FROM `items` WHERE (a, b) IN $keys",
            write_sql="$keys = (SELECT (a, b) FROM `lookup`);\n\n"
            "SELECT * FROM `items` WHERE (a, b) IN $keys",
        )

    def test_join_named_query_roundtrip_does_not_generate_invalid_with(self):
        self.assert_roundtrip_stable(
            "$user_likes = (SELECT * FROM `likes` WHERE user_id = $user_id);\n"
            "SELECT p.id, ul.post_id IS NOT NULL AS liked\n"
            "FROM `posts` AS p\n"
            "LEFT JOIN $user_likes AS ul ON ul.post_id = p.id\n"
            "WHERE p.id = $id"
        )

    def test_named_query_with_group_by_roundtrip_does_not_generate_empty_with(self):
        sql = (
            "$recent_failed_tests_d = SELECT DISTINCT join_key FROM `failed`;\n"
            "SELECT pta.folder_name AS folder_name, "
            "SUM(CASE WHEN status LIKE 'FAILED' THEN 1 ELSE 0 END) AS status "
            "FROM `tests` AS pta "
            "INNER JOIN $recent_failed_tests_d AS ot ON pta.join_key = ot.join_key "
            "WHERE status LIKE 'FAILED' "
            "GROUP BY pta.folder_name, pta.status "
            "HAVING SUM(CASE WHEN status LIKE 'FAILED' THEN 1 ELSE 0 END) > 0 "
            "ORDER BY status DESC LIMIT 20 OFFSET 0"
        )
        generated = parse_one(sql, dialect="ydb").sql(dialect="ydb")
        self.assertNotIn("WITH  SELECT", generated)
        parse_one(generated, dialect="ydb")

    def test_table_valued_function_roundtrip_stable(self):
        self.validate_identity("SELECT * FROM AS_TABLE($Input) AS k")
        self.assert_roundtrip_stable(
            "DECLARE $Input AS List<Struct<`shard`: Int64>>;\n"
            "SELECT t.`shard`\n"
            "FROM AS_TABLE($Input) AS k\n"
            "JOIN `target` AS t ON t.`shard` = k.`shard`"
        )

    def test_multi_equality_join_roundtrip_stable(self):
        self.assert_roundtrip_stable(
            "SELECT * FROM `a` AS a\n"
            "JOIN `b` AS b ON a.id = b.id AND a.kind = b.kind"
        )


# ---------------------------------------------------------------------------
# YDB → other dialects
# ---------------------------------------------------------------------------

class TestYDBToOther(unittest.TestCase):
    """Tests for YDB parsing → other dialect generation."""

    maxDiff = None

    def ydb_to(self, sql, dialect):
        return parse_one(sql, dialect="ydb").sql(dialect=dialect)

    # --- CTEs ($var = SELECT) → WITH ----------------------------------------

    def test_cte_to_postgres(self):
        self.assertEqual(
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t AS t",
            self.ydb_to("$t = (SELECT 1 AS x); SELECT * FROM $t AS t", "postgres"),
        )

    def test_multi_cte_to_postgres(self):
        self.assertEqual(
            "WITH a AS (SELECT 1), b AS (SELECT x FROM a AS a) SELECT * FROM b AS b",
            self.ydb_to("$a = (SELECT 1); $b = (SELECT x FROM $a AS a); SELECT * FROM $b AS b", "postgres"),
        )

    def test_cte_to_clickhouse(self):
        self.assertEqual(
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t AS t",
            self.ydb_to("$t = (SELECT 1 AS x); SELECT * FROM $t AS t", "clickhouse"),
        )

    # --- Integer type mappings -----------------------------------------------

    def test_int32_to_postgres(self):
        self.assertEqual(
            "CAST(x AS INT)",
            self.ydb_to("CAST(x AS Int32)", "postgres"),
        )

    def test_int64_to_postgres(self):
        self.assertEqual(
            "CAST(x AS BIGINT)",
            self.ydb_to("CAST(x AS Int64)", "postgres"),
        )

    def test_int32_to_clickhouse(self):
        self.assertEqual(
            "CAST(x AS Nullable(Int32))",
            self.ydb_to("CAST(x AS Int32)", "clickhouse"),
        )

    # --- Optional<T> → nullable type ----------------------------------------

    def test_optional_to_postgres(self):
        # Optional wrapper is dropped; Utf8 maps to TEXT
        self.assertEqual(
            "CAST(x AS TEXT)",
            self.ydb_to("CAST(x AS Optional<Utf8>)", "postgres"),
        )

    def test_optional_int_to_postgres(self):
        self.assertEqual(
            "CAST(x AS INT)",
            self.ydb_to("CAST(x AS Optional<Int32>)", "postgres"),
        )

    def test_optional_to_clickhouse(self):
        # ClickHouse wraps in Nullable(); Utf8 maps to String (ClickHouse text type)
        self.assertEqual(
            "CAST(x AS Nullable(String))",
            self.ydb_to("CAST(x AS Optional<Utf8>)", "clickhouse"),
        )

    # --- Container types -----------------------------------------------------

    def test_tuple_to_clickhouse(self):
        self.assertEqual(
            "CAST(x AS Tuple(_0 Nullable(Int32), _1 Nullable(String)))",
            self.ydb_to("CAST(x AS Tuple<Int32, Utf8>)", "clickhouse"),
        )

    def test_dict_to_clickhouse(self):
        self.assertEqual(
            "CAST(x AS Map(String, Nullable(Int64)))",
            self.ydb_to("CAST(x AS Dict<Utf8, Int64>)", "clickhouse"),
        )

    # --- Module functions survive transpilation ------------------------------

    def test_module_func_preserved_in_clickhouse(self):
        # ClickHouse preserves :: function calls as-is (different identifier quoting)
        self.assertEqual(
            'SELECT DateTime::GetYear(ts) FROM "t"',
            self.ydb_to("SELECT DateTime::GetYear(ts) FROM `t`", "clickhouse"),
        )


# ---------------------------------------------------------------------------
# ClickHouse → YDB transpilation
# ---------------------------------------------------------------------------

class TestYDBFromClickHouse(unittest.TestCase):
    """Tests for source-dialect → YDB transpilation (ClickHouse as primary source)."""

    maxDiff = None

    def ch(self, sql: str) -> str:
        return parse_one(sql, dialect="clickhouse").sql(dialect="ydb")

    # --- Interval literals --------------------------------------------------

    def test_interval(self):
        cases = [
            ("SELECT ts - INTERVAL 30 DAY",   "SELECT ts - DateTime::IntervalFromDays(30)"),
            ("SELECT ts - INTERVAL 6 HOUR",   "SELECT ts - DateTime::IntervalFromHours(6)"),
            ("SELECT ts + INTERVAL 15 MINUTE", "SELECT ts + DateTime::IntervalFromMinutes(15)"),
            ("SELECT ts + INTERVAL 90 SECOND", "SELECT ts + DateTime::IntervalFromSeconds(90)"),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(self.ch(sql), expected)

    # --- dateDiff -----------------------------------------------------------

    def test_datediff(self):
        cases = [
            ("SELECT dateDiff('minute', a, b)", "SELECT (CAST(b AS Int64) - CAST(a AS Int64)) / 60000000"),
            ("SELECT dateDiff('hour',   a, b)", "SELECT (CAST(b AS Int64) - CAST(a AS Int64)) / 3600000000"),
            ("SELECT dateDiff('day',    a, b)", "SELECT (CAST(b AS Int64) - CAST(a AS Int64)) / 86400000000"),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(self.ch(sql), expected)

    def test_datediff_no_extra_nullif_nesting(self):
        # With SAFE_DIVISION=True the generator must not wrap the result in
        # an additional NULLIF on top of any existing protection.
        self.assertEqual(
            self.ch("SELECT x / (dateDiff('day', a, b) + 1)"),
            "SELECT x / ((CAST(b AS Int64) - CAST(a AS Int64)) / 86400000000 + 1)",
        )

    # --- COUNT() without arguments ------------------------------------------

    def test_count_no_args_becomes_count_star(self):
        self.assertEqual(self.ch("SELECT count()"),   "SELECT COUNT(*)")
        self.assertEqual(self.ch("SELECT count(*)"),  "SELECT COUNT(*)")

    # --- GROUP BY: no outer parentheses around item list --------------------

    def test_group_by_no_outer_parens(self):
        self.assertEqual(
            self.ch("SELECT category, count() FROM t GROUP BY category"),
            "SELECT category, COUNT(*) FROM `t` GROUP BY category AS category",
        )
        self.assertEqual(
            self.ch("SELECT a, b, count() FROM t GROUP BY a, b"),
            "SELECT a, b, COUNT(*) FROM `t` GROUP BY a AS a, b AS b",
        )

    def test_group_by_alias_ydb_roundtrip(self):
        self.assertEqual(
            parse_one("SELECT v, COUNT(*) FROM `t` GROUP BY v AS v", dialect="ydb").sql(dialect="ydb"),
            "SELECT v, COUNT(*) FROM `t` GROUP BY v AS v",
        )

    def test_group_by_column_gets_ydb_alias(self):
        self.assertEqual(
            parse_one("SELECT v, COUNT(*) FROM `t` GROUP BY v", dialect="ydb").sql(dialect="ydb"),
            "SELECT v, COUNT(*) FROM `t` GROUP BY v AS v",
        )

    def test_group_by_positional_constant_is_removed(self):
        self.assertEqual(
            self.ch("SELECT 1, count() FROM t GROUP BY 1"),
            "SELECT 1, COUNT(*) FROM `t`",
        )

    # --- ARRAY JOIN / arrayJoin ---------------------------------------------

    def test_array_join_to_flatten_by(self):
        self.assertEqual(
            self.ch("SELECT * FROM t ARRAY JOIN vals"),
            "SELECT * FROM `t` FLATTEN BY vals",
        )

    def test_array_join_alias_to_flatten_by_alias(self):
        self.assertEqual(
            self.ch("SELECT * FROM t ARRAY JOIN vals AS v"),
            "SELECT * FROM `t` FLATTEN BY vals AS v",
        )

    def test_array_join_function_to_flatten_by(self):
        self.assertEqual(
            self.ch("SELECT id, arrayJoin(vals) AS v FROM t"),
            "SELECT id, v FROM `t` FLATTEN BY vals AS v",
        )

    def test_multi_array_join_is_not_flatten_by(self):
        with self.assertRaises(UnsupportedError):
            self.ch("SELECT * FROM t ARRAY JOIN a, b")

    def test_left_array_join_is_not_flatten_by(self):
        with self.assertRaises(UnsupportedError):
            self.ch("SELECT * FROM t LEFT ARRAY JOIN vals AS v")


# ---------------------------------------------------------------------------
# PostgreSQL → YDB transpilation
# ---------------------------------------------------------------------------

class TestYDBFromPostgres(unittest.TestCase):
    """Tests for source-dialect → YDB transpilation of FLATTEN analogues."""

    maxDiff = None

    def pg(self, sql: str) -> str:
        return parse_one(sql, dialect="postgres").sql(dialect="ydb")

    def test_unnest_join_to_flatten_by(self):
        self.assertEqual(
            self.pg("SELECT * FROM t CROSS JOIN LATERAL unnest(vals)"),
            "SELECT * FROM `t` FLATTEN BY vals",
        )

    def test_unnest_comma_join_to_flatten_by(self):
        self.assertEqual(
            self.pg("SELECT * FROM t, unnest(vals) AS v"),
            "SELECT * FROM `t` FLATTEN BY vals AS v",
        )

    def test_unnest_join_alias_to_flatten_by_alias(self):
        self.assertEqual(
            self.pg("SELECT id, v FROM t CROSS JOIN LATERAL unnest(vals) AS v"),
            "SELECT id, v FROM `t` FLATTEN BY vals AS v",
        )

    def test_unnest_join_column_alias_to_flatten_by_alias(self):
        self.assertEqual(
            self.pg("SELECT id, item FROM t CROSS JOIN LATERAL unnest(vals) AS v(item)"),
            "SELECT id, item FROM `t` FLATTEN BY vals AS item",
        )

    def test_independent_unnest_keeps_cross_join(self):
        self.assertIn(
            "CROSS JOIN",
            self.pg("SELECT * FROM t CROSS JOIN LATERAL unnest(ARRAY[1, 2]) AS v"),
        )
