"""Execute generated INTO RESULT statements against YDB."""

from sqlglot import parse_one


def test_into_result_labels_execute(ydb_pool):
    cases = (
        ("SELECT 1 AS value INTO RESULT result", 1),
        ("SELECT 2 AS value INTO RESULT `Result name`", 2),
    )

    for source, expected in cases:
        yql = parse_one(source, dialect="ydb").sql(dialect="ydb")
        result = ydb_pool.execute_with_retries(yql)

        assert result[0].rows[0]["value"] == expected
