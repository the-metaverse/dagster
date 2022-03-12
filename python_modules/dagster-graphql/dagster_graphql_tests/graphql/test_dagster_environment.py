import sys

from dagster._core.host_representation import ManagedGrpcPythonEnvRepositoryLocationOrigin
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._utils import file_relative_path


def test_dagster_out_of_process_location():
    with ManagedGrpcPythonEnvRepositoryLocationOrigin(
        location_name="test_location",
        loadable_target_origin=LoadableTargetOrigin(
            executable_path=sys.executable,
            python_file=file_relative_path(__file__, "setup.py"),
            attribute="test_repo",
        ),
    ).create_single_location() as env:
        assert env.get_repository("test_repo")
