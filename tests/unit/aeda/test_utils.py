from pathlib import Path

import pytest

from aeda import utils


def test_get_db_connection_string():
    t_filename = Path("tests/fixtures/databases.ini")
    t_section = "mysql-source-test"
    connection_string = utils.get_db_connection_string(t_section, filename=t_filename)

    assert isinstance(connection_string, dict)


def test_get_connection_parameters():
    t_filename = Path("tests/fixtures/databases.ini")
    t_section = "mysql-source-test"

    db_engine_expected = "mysql"
    host_expected = "localhost"
    schema_expected = "world"
    catalog_expected = "def"

    (
        t_db_engine,
        t_server_name,
        t_catalog_name,
        t_schema_name,
    ) = utils.get_connection_parameters(t_section, filename=t_filename)

    assert isinstance(t_db_engine, str)
    assert t_db_engine == db_engine_expected
    assert t_server_name == host_expected
    assert t_catalog_name == catalog_expected
    assert t_schema_name == schema_expected


@pytest.mark.parametrize(
    "query",
    [
        ("columns", "mysql"),
        ("columns", "postgres"),
        ("columns", "mssqlserver"),
        ("columns", "mariadb"),
    ],
)
def test_get_query(query):
    t_type, t_engine = query
    t_query = utils.get_query(t_type, t_engine)
    assert isinstance(t_query, str)
