import pytest

from dagster._utils import file_relative_path
from dagster._utils.test.mysql_instance import TestMySQLInstance


@pytest.fixture(scope="session")
def hostname(conn_string):  # pylint: disable=redefined-outer-name, unused-argument
    return TestMySQLInstance.get_hostname()


@pytest.fixture(scope="session")
def conn_string():  # pylint: disable=redefined-outer-name, unused-argument
    with TestMySQLInstance.docker_service_up_or_skip(
        file_relative_path(__file__, "docker-compose.yml"), "test-mysql-db"
    ) as conn_str:
        yield conn_str
