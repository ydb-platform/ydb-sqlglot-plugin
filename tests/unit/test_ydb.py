import unittest

from sqlglot import ErrorLevel, UnsupportedError, parse_one
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
        # Implicit cross join requires PRAGMA AnsiImplicitCrossJoin
        self.validate_identity(
            "SELECT * FROM `a`, `b`",
            write_sql="PRAGMA AnsiImplicitCrossJoin;\nSELECT * FROM `a`, `b`",
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

    def test_window_functions(self):
        cases = [
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM `table`",
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) FROM `table`",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.validate_identity(sql)

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
            "CREATE TABLE `table` (id Uint64 NOT NULL, name Utf8, created_at Timestamp, PRIMARY KEY(`id`))\n"
            "PARTITION BY HASH (`id`);",
        )

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

    def test_array_any(self):
        self.assertEqual(
            ydb("SELECT * FROM TABLE WHERE ARRAY_ANY(arr, x -> x)"),
            "SELECT * FROM `TABLE` WHERE ListHasItems(ListFilter(($x) -> {RETURN $x}))",
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
            "ON a.id = _u_0._u_1 WHERE NOT (_u_0._u_1 IS NULL)",
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
            "ON b.id = _u_0._u_1 WHERE TRUE AND NOT (_u_0._u_1 IS NULL) GROUP BY a_id AS a_id) AS _u_2 "
            "ON a.id = _u_2._u_3 WHERE NOT (_u_2._u_3 IS NULL)",
        )

    def test_unnest_uncorrelated_in_subquery(self):
        sql = "SELECT a.id FROM a WHERE a.id IN (SELECT b.a_id FROM b WHERE b.value > 10)"
        self.assertEqual(
            ydb(sql),
            "SELECT a.id AS id FROM `a` LEFT JOIN "
            "(SELECT b.a_id AS a_id FROM `b` WHERE b.value > 10 GROUP BY a_id AS a_id) AS _u_0 "
            "ON a.id = _u_0.a_id WHERE NOT (_u_0.a_id IS NULL)",
        )

    def test_unnest_correlated_in_subquery(self):
        sql = "SELECT * FROM x WHERE x.a IN (SELECT y.a FROM y WHERE y.b = x.b)"
        self.assertEqual(
            ydb(sql),
            "SELECT * FROM `x` LEFT JOIN "
            "(SELECT y.a AS a, y.b AS _u_1 FROM `y` WHERE TRUE GROUP BY b AS b) AS _u_0 "
            "ON x.b = _u_0._u_1 WHERE ListHasItems(($_x, $p_0)->(ListFilter($_x, ($_x) -> {RETURN $_x = $p_0}))(a, x.a))",
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
            "WHERE x.a > ListHasItems(($_x, $p_0)->(ListFilter($_x, ($_x) -> {RETURN $p_0 > $_x}))(a, x.a))",
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
            "PRAGMA AnsiImplicitCrossJoin;\n"
            "SELECT CASE WHEN a = 1 THEN _u_1._col0 ELSE 0 END AS val FROM `t1`, "
            "(SELECT MAX(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1",
        )

    def test_case_with_subquery_in_else(self):
        sql = "SELECT CASE WHEN a = 1 THEN 100 ELSE (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a) END as val FROM t1"
        self.assertEqual(
            ydb(sql),
            "PRAGMA AnsiImplicitCrossJoin;\n"
            "SELECT CASE WHEN a = 1 THEN 100 ELSE _u_1._col0 END AS val FROM `t1`, "
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

    # --- $varname -----------------------------------------------------------

    def test_dollar_variable_in_expr(self):
        self.validate_identity("$x + 1")

    def test_dollar_variable_as_table(self):
        self.validate_identity("SELECT * FROM $t AS t")

    def test_dollar_variable_in_select(self):
        self.validate_identity("SELECT $limit FROM `table`")

    # --- Module::Function() -------------------------------------------------

    def test_module_function_simple(self):
        self.validate_identity("DateTime::GetYear(ts)")

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

    # --- DECLARE $p AS Type -------------------------------------------------

    def test_declare_utf8(self):
        self.validate_identity("DECLARE $name AS Utf8")

    def test_declare_timestamp(self):
        self.validate_identity("DECLARE $ts AS Timestamp")

    def test_declare_uint64(self):
        self.validate_identity("DECLARE $id AS Uint64")

    # --- FLATTEN [LIST|DICT] BY ---------------------------------------------

    def test_flatten_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN BY col")

    def test_flatten_list_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN LIST BY col")

    def test_flatten_dict_by(self):
        self.validate_identity("SELECT * FROM `t` FLATTEN DICT BY col")

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

    # --- ASSUME ORDER BY ----------------------------------------------------

    def test_assume_order_by(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id")

    def test_assume_order_by_desc(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id DESC")

    def test_assume_order_by_multi_col(self):
        self.validate_identity("SELECT * FROM `t` ASSUME ORDER BY id, name")

    def test_regular_order_by_unchanged(self):
        self.validate_identity("SELECT * FROM `t` ORDER BY id")

    # --- Named expressions $name = expr -------------------------------------

    def test_named_expression_subquery(self):
        self.validate_identity("$t = (SELECT 1)")

    def test_named_expression_from_table(self):
        self.validate_identity("$t = (SELECT * FROM `table`)")


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
