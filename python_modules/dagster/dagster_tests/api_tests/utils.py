import sys
from contextlib import contextmanager

from dagster import file_relative_path
from dagster._core.host_representation import (
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
    PipelineHandle,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import PythonFileTarget


@contextmanager
def get_bar_workspace(instance):
    with WorkspaceProcessContext(
        instance,
        PythonFileTarget(
            python_file=file_relative_path(__file__, "api_tests_repo.py"),
            attribute="bar_repo",
            working_directory=None,
            location_name="bar_repo_location",
        ),
    ) as workspace_process_context:
        yield workspace_process_context.create_request_context()


@contextmanager
def get_bar_repo_repository_location():
    loadable_target_origin = LoadableTargetOrigin(
        executable_path=sys.executable,
        python_file=file_relative_path(__file__, "api_tests_repo.py"),
        attribute="bar_repo",
    )
    location_name = "bar_repo_location"

    origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(loadable_target_origin, location_name)

    with origin.create_single_location() as location:
        yield location


@contextmanager
def get_bar_repo_handle():
    with get_bar_repo_repository_location() as location:
        yield location.get_repository("bar_repo").handle


@contextmanager
def get_foo_pipeline_handle():
    with get_bar_repo_handle() as repo_handle:
        yield PipelineHandle("foo", repo_handle)


@contextmanager
def get_foo_external_pipeline():
    with get_bar_repo_repository_location() as location:
        yield location.get_repository("bar_repo").get_full_external_pipeline("foo")
