import random
import string

from pytest import fixture
from yoyo.backends import SQLiteBackend
from yoyo.connections import parse_uri


@fixture(scope="session")
def s3_files() -> list[str]:
    return [
        "create-table.sql",
        "create-table.rollback.sql",
        "post-apply-create-table.sql",
    ]


@fixture(scope="session")
def yoyo_migrations_table_name():
    return "_yoyo_migrations"


@fixture(scope="session")
def sqlite_backend(yoyo_migrations_table_name):
    uri = parse_uri("sqlite:///:memory:")
    backend = SQLiteBackend(
        uri,
        yoyo_migrations_table_name,
    )
    backend.init_database()
    return backend


@fixture(scope="function")
def table_name():
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(5))


@fixture(scope="function")
def create_table_sql(table_name):
    return f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER)"