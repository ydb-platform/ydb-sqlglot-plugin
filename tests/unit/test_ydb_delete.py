import unittest

from sqlglot import ParseError, parse, parse_one


def render_statements(sql: str) -> str:
    return ";\n".join(statement.sql(dialect="ydb") for statement in parse(sql, dialect="ydb") if statement is not None)


class TestYDBDelete(unittest.TestCase):
    def test_delete_where_doc_example(self):
        self.assertEqual(
            parse_one(
                'DELETE FROM my_table WHERE Key1 == 1 AND Key2 >= "One"',
                dialect="ydb",
            ).sql(dialect="ydb"),
            "DELETE FROM `my_table` WHERE Key1 = 1 AND Key2 >= 'One'",
        )

    def test_delete_on_doc_example(self):
        sql = (
            "$to_delete = (\n"
            '    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete" LIMIT 100\n'
            ");\n\n"
            "DELETE FROM my_table ON\n"
            "SELECT * FROM $to_delete;"
        )

        self.assertEqual(
            render_statements(sql),
            "$to_delete = (SELECT Key, SubKey FROM `my_table` "
            "WHERE Value = 'ToDelete' LIMIT 100);\n"
            "DELETE FROM `my_table` ON SELECT * FROM $to_delete",
        )

    def test_delete_on_roundtrip_is_stable(self):
        sql = "DELETE FROM `my_table` ON SELECT Key, SubKey FROM $to_delete"
        self.assertEqual(render_statements(render_statements(sql)), render_statements(sql))

    def test_delete_on_requires_query(self):
        with self.assertRaises(ParseError):
            parse_one("DELETE FROM my_table ON", dialect="ydb")

    def test_delete_returning_all_doc_example(self):
        self.assertEqual(
            parse_one(
                "DELETE FROM orders WHERE status = 'cancelled' RETURNING *",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "DELETE FROM `orders` WHERE status = 'cancelled' RETURNING *",
        )

    def test_delete_returning_columns_doc_example(self):
        self.assertEqual(
            parse_one(
                "DELETE FROM orders WHERE status = 'cancelled' RETURNING order_id, order_date",
                dialect="ydb",
            ).sql(dialect="ydb"),
            "DELETE FROM `orders` WHERE status = 'cancelled' RETURNING order_id, order_date",
        )

    def test_postgres_delete_transpilation(self):
        cases = [
            (
                "DELETE FROM orders WHERE status = 'cancelled'",
                "DELETE FROM `orders` WHERE status = 'cancelled'",
            ),
            (
                "DELETE FROM orders WHERE status = 'cancelled' RETURNING *",
                "DELETE FROM `orders` WHERE status = 'cancelled' RETURNING *",
            ),
            (
                "DELETE FROM orders WHERE status = 'cancelled' RETURNING order_id, order_date",
                "DELETE FROM `orders` WHERE status = 'cancelled' RETURNING order_id, order_date",
            ),
        ]

        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(
                    parse_one(sql, dialect="postgres").sql(dialect="ydb"),
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
