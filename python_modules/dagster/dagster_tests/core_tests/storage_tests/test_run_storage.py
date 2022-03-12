import tempfile
from contextlib import contextmanager

import pytest
from dagster_tests.core_tests.storage_tests.utils.run_storage import TestRunStorage

from dagster._core.storage.runs import InMemoryRunStorage, SqliteRunStorage


@contextmanager
def create_sqlite_run_storage():
    with tempfile.TemporaryDirectory() as tempdir:
        yield SqliteRunStorage.from_local(tempdir)


@contextmanager
def create_non_bucket_sqlite_run_storage():
    with tempfile.TemporaryDirectory() as tempdir:
        yield NonBucketQuerySqliteRunStorage.from_local(tempdir)


class NonBucketQuerySqliteRunStorage(SqliteRunStorage):
    def supports_bucket_query(self):
        return False


@contextmanager
def create_in_memory_storage():
    yield InMemoryRunStorage()


class TestSqliteImplementation(TestRunStorage):
    __test__ = True

    @pytest.fixture(name="storage", params=[create_sqlite_run_storage])
    def run_storage(self, request):
        with request.param() as s:
            yield s


class TestNonBucketQuerySqliteImplementation(TestRunStorage):
    __test__ = True

    @pytest.fixture(name="storage", params=[create_non_bucket_sqlite_run_storage])
    def run_storage(self, request):
        with request.param() as s:
            yield s


class TestInMemoryImplementation(TestRunStorage):
    __test__ = True

    @pytest.fixture(name="storage", params=[create_in_memory_storage])
    def run_storage(self, request):
        with request.param() as s:
            yield s

    def test_storage_telemetry(self, storage):
        pass
