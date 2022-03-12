import time

from dagster import Bool, Field, check, seven
from dagster._core.errors import (
    DagsterInvariantViolationError,
    DagsterLaunchFailedError,
    DagsterUserCodeUnreachableError,
)
from dagster._core.host_representation.grpc_server_registry import ProcessGrpcServerRegistry
from dagster._core.host_representation.repository_location import GrpcServerRepositoryLocation
from dagster._core.storage.pipeline_run import PipelineRun
from dagster._core.storage.tags import GRPC_INFO_TAG
from dagster._grpc.client import DagsterGrpcClient
from dagster._grpc.types import (
    CanCancelExecutionRequest,
    CancelExecutionRequest,
    ExecuteExternalPipelineArgs,
    StartRunResult,
)
from dagster._serdes import ConfigurableClass, deserialize_as, deserialize_json_to_dagster_namedtuple
from dagster._utils import merge_dicts

from .base import LaunchRunContext, RunLauncher


class DefaultRunLauncher(RunLauncher, ConfigurableClass):
    """Launches runs against running GRPC servers."""

    def __init__(self, inst_data=None, wait_for_processes=False):
        self._inst_data = inst_data

        # Whether to wait for any processes that were used to launch runs to finish
        # before disposing of this launcher. Primarily useful for test cleanup where
        # we want to make sure that resources used by the test are cleaned up before
        # the test ends.
        self._wait_for_processes = check.bool_param(wait_for_processes, "wait_for_processes")

        self._run_ids = set()

        self._locations_to_wait_for = []

        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {"wait_for_processes": Field(Bool, is_required=False)}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DefaultRunLauncher(
            inst_data=inst_data, wait_for_processes=config_value.get("wait_for_processes", False)
        )

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.pipeline_run

        check.inst_param(run, "run", PipelineRun)

        if not context.workspace:
            raise DagsterInvariantViolationError(
                "DefaultRunLauncher requires a workspace to be included in its LaunchRunContext"
            )

        external_pipeline_origin = check.not_none(run.external_pipeline_origin)
        repository_location = context.workspace.get_location(
            external_pipeline_origin.external_repository_origin.repository_location_origin.location_name
        )

        check.inst(
            repository_location,
            GrpcServerRepositoryLocation,
            "DefaultRunLauncher: Can't launch runs for pipeline not loaded from a GRPC server",
        )

        self._instance.add_run_tags(
            run.run_id,
            {
                GRPC_INFO_TAG: seven.json.dumps(
                    merge_dicts(
                        {"host": repository_location.host},
                        (
                            {"port": repository_location.port}
                            if repository_location.port
                            else {"socket": repository_location.socket}
                        ),
                        ({"use_ssl": True} if repository_location.use_ssl else {}),
                    )
                )
            },
        )

        res = deserialize_as(
            repository_location.client.start_run(
                ExecuteExternalPipelineArgs(
                    pipeline_origin=external_pipeline_origin,
                    pipeline_run_id=run.run_id,
                    instance_ref=self._instance.get_ref(),
                )
            ),
            StartRunResult,
        )
        if not res.success:
            raise (
                DagsterLaunchFailedError(
                    res.message, serializable_error_info=res.serializable_error_info
                )
            )

        self._run_ids.add(run.run_id)

        if self._wait_for_processes:
            self._locations_to_wait_for.append(repository_location)

    def _get_grpc_client_for_termination(self, run_id):
        if not self._instance:
            return None

        run = self._instance.get_run_by_id(run_id)
        if not run or run.is_finished:
            return None

        tags = run.tags

        if GRPC_INFO_TAG not in tags:
            return None

        grpc_info = seven.json.loads(tags.get(GRPC_INFO_TAG))

        return DagsterGrpcClient(
            port=grpc_info.get("port"),
            socket=grpc_info.get("socket"),
            host=grpc_info.get("host"),
            use_ssl=bool(grpc_info.get("use_ssl", False)),
        )

    def can_terminate(self, run_id):
        check.str_param(run_id, "run_id")

        client = self._get_grpc_client_for_termination(run_id)
        if not client:
            return False

        try:
            res = deserialize_json_to_dagster_namedtuple(
                client.can_cancel_execution(CanCancelExecutionRequest(run_id=run_id), timeout=5)
            )
        except DagsterUserCodeUnreachableError:
            # Server that created the run may no longer exist
            return False

        return res.can_cancel

    def terminate(self, run_id):
        check.str_param(run_id, "run_id")
        if not self._instance:
            return False

        run = self._instance.get_run_by_id(run_id)
        if not run:
            return False

        client = self._get_grpc_client_for_termination(run_id)

        if not client:
            self._instance.report_engine_event(
                message="Unable to get grpc client to send termination request to.",
                pipeline_run=run,
                cls=self.__class__,
            )
            return False

        self._instance.report_run_canceling(run)
        res = deserialize_json_to_dagster_namedtuple(
            client.cancel_execution(CancelExecutionRequest(run_id=run_id))
        )
        return res.success

    def join(self, timeout=30):
        # If this hasn't been initialized at all, we can just do a noop
        if not self._instance:
            return

        total_time = 0
        interval = 0.01

        while True:
            active_run_ids = [
                run_id
                for run_id in self._run_ids
                if (
                    self._instance.get_run_by_id(run_id)
                    and not self._instance.get_run_by_id(run_id).is_finished
                )
            ]

            if len(active_run_ids) == 0:
                return

            if total_time >= timeout:
                raise Exception(
                    "Timed out waiting for these runs to finish: {active_run_ids}".format(
                        active_run_ids=repr(active_run_ids)
                    )
                )

            total_time += interval
            time.sleep(interval)
            interval = interval * 2

    def dispose(self):
        if not self._wait_for_processes:
            return

        for location in self._locations_to_wait_for:
            if isinstance(location, GrpcServerRepositoryLocation) and isinstance(
                location.grpc_server_registry, ProcessGrpcServerRegistry
            ):
                location.grpc_server_registry.wait_for_processes()
