import unittest

from sqlglot import exp, parse


def generate_ydb_batch(sql: str) -> str:
    return ";\n".join(
        expression.sql(dialect="ydb") for expression in parse(sql, dialect="ydb") if expression is not None
    )


class TestYDBReplace(unittest.TestCase):
    def test_replace_into_values_doc_example(self):
        sql = """REPLACE INTO my_table (Key1, Key2, Value2) VALUES
    (1u, "One", 101),
    (2u, "Two", 102);
COMMIT;"""

        statements = parse(sql, dialect="ydb")

        self.assertIsInstance(statements[0], exp.Insert)
        self.assertTrue(statements[0].meta.get("ydb_replace"))
        self.assertEqual(
            "REPLACE INTO `my_table` (Key1, Key2, Value2) VALUES (1u, 'One', 101), (2u, 'Two', 102);\nCOMMIT",
            generate_ydb_batch(sql),
        )

    def test_replace_into_select_doc_example(self):
        sql = """REPLACE INTO my_table
SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
FROM my_table1;
COMMIT;"""

        statements = parse(sql, dialect="ydb")

        self.assertIsInstance(statements[0], exp.Insert)
        self.assertTrue(statements[0].meta.get("ydb_replace"))
        self.assertEqual(
            "REPLACE INTO `my_table` SELECT Key AS Key1, 'Empty' AS Key2, Value AS Value1 FROM `my_table1`;\nCOMMIT",
            generate_ydb_batch(sql),
        )


if __name__ == "__main__":
    unittest.main()
