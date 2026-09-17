import socket

import pytest
import ydb

YDB_ENDPOINT = "grpc://localhost:2136"
YDB_DATABASE = "/local"


def _ydb_reachable() -> bool:
    try:
        s = socket.create_connection(("localhost", 2136), timeout=1)
        s.close()
        return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def ydb_driver():
    """Session-scoped YDB driver, skipped when local YDB is unavailable."""
    if not _ydb_reachable():
        pytest.skip("YDB not available — run: docker compose up -d")

    driver = ydb.Driver(
        endpoint=YDB_ENDPOINT,
        database=YDB_DATABASE,
        credentials=ydb.AnonymousCredentials(),
    )
    driver.wait(timeout=10, fail_fast=True)
    yield driver
    driver.stop()


@pytest.fixture(scope="session")
def ydb_pool(ydb_driver):
    """Session-scoped YDB QuerySessionPool."""
    with ydb.QuerySessionPool(ydb_driver) as pool:
        yield pool
