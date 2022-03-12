from dagster._api.snapshot_schedule import sync_get_external_schedule_execution_data_ephemeral_grpc
from dagster._core.definitions.schedule_definition import ScheduleExecutionData
from dagster._core.test_utils import instance_for_test
from dagster._seven import get_current_datetime_in_utc

from .utils import get_bar_repo_handle


def test_external_schedule_execution_data_api_grpc():
    with instance_for_test() as instance:
        with get_bar_repo_handle() as repository_handle:
            execution_data = sync_get_external_schedule_execution_data_ephemeral_grpc(
                instance,
                repository_handle,
                "foo_schedule",
                None,
            )
            assert isinstance(execution_data, ScheduleExecutionData)
            assert len(execution_data.run_requests) == 1
            to_launch = execution_data.run_requests[0]
            assert to_launch.run_config == {"fizz": "buzz"}
            assert to_launch.tags == {"dagster/schedule_name": "foo_schedule"}


def test_external_schedule_execution_data_api_never_execute_grpc():
    with instance_for_test() as instance:
        with get_bar_repo_handle() as repository_handle:
            execution_data = sync_get_external_schedule_execution_data_ephemeral_grpc(
                instance,
                repository_handle,
                "foo_schedule_never_execute",
                None,
            )
            assert isinstance(execution_data, ScheduleExecutionData)
            assert len(execution_data.run_requests) == 0


def test_include_execution_time_grpc():
    with instance_for_test() as instance:
        with get_bar_repo_handle() as repository_handle:
            execution_time = get_current_datetime_in_utc()
            execution_data = sync_get_external_schedule_execution_data_ephemeral_grpc(
                instance,
                repository_handle,
                "foo_schedule_echo_time",
                execution_time,
            )

            assert isinstance(execution_data, ScheduleExecutionData)
            assert len(execution_data.run_requests) == 1
            to_launch = execution_data.run_requests[0]
            assert to_launch.run_config == {"passed_in_time": execution_time.isoformat()}
            assert to_launch.tags == {"dagster/schedule_name": "foo_schedule_echo_time"}
