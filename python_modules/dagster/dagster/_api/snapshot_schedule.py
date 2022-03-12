from typing import TYPE_CHECKING, Any

from dagster import check
from dagster.core.definitions.schedule_definition import ScheduleExecutionData
from dagster.core.errors import DagsterUserCodeProcessError
from dagster.core.host_representation.external_data import ExternalScheduleExecutionErrorData
from dagster.core.host_representation.handle import RepositoryHandle
from dagster.core.instance import DagsterInstance
from dagster.grpc.types import ExternalScheduleExecutionArgs
from dagster.serdes import deserialize_as
from dagster.seven.compat.pendulum import PendulumDateTime

if TYPE_CHECKING:
    from dagster.grpc.client import DagsterGrpcClient


def sync_get_external_schedule_execution_data_ephemeral_grpc(
    instance: DagsterInstance,
    repository_handle: RepositoryHandle,
    schedule_name: str,
    scheduled_execution_time: Any,
):
    from dagster.grpc.client import ephemeral_grpc_api_client

    origin = repository_handle.get_external_origin()
    with ephemeral_grpc_api_client(
        origin.repository_location_origin.loadable_target_origin
    ) as api_client:
        return sync_get_external_schedule_execution_data_grpc(
            api_client,
            instance,
            repository_handle,
            schedule_name,
            scheduled_execution_time,
        )


def sync_get_external_schedule_execution_data_grpc(
    api_client: "DagsterGrpcClient",
    instance: DagsterInstance,
    repository_handle: RepositoryHandle,
    schedule_name: str,
    scheduled_execution_time: Any,
) -> ScheduleExecutionData:
    check.inst_param(repository_handle, "repository_handle", RepositoryHandle)
    check.str_param(schedule_name, "schedule_name")
    check.opt_inst_param(scheduled_execution_time, "scheduled_execution_time", PendulumDateTime)

    origin = repository_handle.get_external_origin()
    result = deserialize_as(
        api_client.external_schedule_execution(
            external_schedule_execution_args=ExternalScheduleExecutionArgs(
                repository_origin=origin,
                instance_ref=instance.get_ref(),
                schedule_name=schedule_name,
                scheduled_execution_timestamp=scheduled_execution_time.timestamp()
                if scheduled_execution_time
                else None,
                scheduled_execution_timezone=scheduled_execution_time.timezone.name
                if scheduled_execution_time
                else None,
            )
        ),
        (ScheduleExecutionData, ExternalScheduleExecutionErrorData),
    )
    if isinstance(result, ExternalScheduleExecutionErrorData):
        raise DagsterUserCodeProcessError.from_error_info(result.error)

    return result
