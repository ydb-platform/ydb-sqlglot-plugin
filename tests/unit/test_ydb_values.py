import unittest

from sqlglot import parse_one


class TestYDBValues(unittest.TestCase):
    """Conformance tests for YDB's documented VALUES table constructor."""

    def ydb(self, sql: str, read: str = "ydb") -> str:
        return parse_one(sql, dialect=read).sql(dialect="ydb")

    def test_top_level_values(self):
        self.assertEqual(
            self.ydb("VALUES (1,2), (3,4)"),
            "VALUES (1, 2), (3, 4)",
        )

    def test_values_after_from(self):
        self.assertEqual(
            self.ydb("SELECT * FROM (VALUES (1,2), (3,4))"),
            "SELECT * FROM (VALUES (1, 2), (3, 4))",
        )

    def test_values_alias_with_column_names(self):
        self.assertEqual(
            self.ydb("SELECT * FROM (VALUES (1,2), (3,4)) AS t(x,y)"),
            "SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(x, y)",
        )

    def test_values_rows_accept_expressions(self):
        self.assertEqual(
            self.ydb("VALUES (1 + 2, 'first'), (3 * 4, 'second')"),
            "VALUES (1 + 2, 'first'), (3 * 4, 'second')",
        )

    def test_postgres_values_transpile_to_ydb(self):
        cases = [
            (
                "VALUES (1,2), (3,4)",
                "VALUES (1, 2), (3, 4)",
            ),
            (
                "SELECT * FROM (VALUES (1,2), (3,4))",
                "SELECT * FROM (VALUES (1, 2), (3, 4))",
            ),
            (
                "SELECT * FROM (VALUES (1,2), (3,4)) AS t(x,y)",
                "SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(x, y)",
            ),
        ]

        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(self.ydb(sql, read="postgres"), expected)


if __name__ == "__main__":
    unittest.main()
