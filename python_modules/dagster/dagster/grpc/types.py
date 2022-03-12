from typing import Dict, List, NamedTuple, Optional

from dagster import check
from dagster._core.code_pointer import CodePointer
from dagster._core.execution.plan.state import KnownExecutionState
from dagster._core.execution.retries import RetryMode
from dagster._core.host_representation.origin import (
    ExternalPipelineOrigin,
    ExternalRepositoryOrigin,
    RepositoryLocationOrigin,
)
from dagster._core.instance.ref import InstanceRef
from dagster._core.origin import PipelinePythonOrigin, get_python_environment_entry_point
from dagster.serdes import serialize_dagster_namedtuple, whitelist_for_serdes
from dagster.utils import frozenlist
from dagster.utils.error import SerializableErrorInfo


@whitelist_for_serdes
class ExecutionPlanSnapshotArgs(
    NamedTuple(
        "_ExecutionPlanSnapshotArgs",
        [
            ("pipeline_origin", ExternalPipelineOrigin),
            ("solid_selection", List[str]),
            ("run_config", Dict[str, object]),
            ("mode", str),
            ("step_keys_to_execute", Optional[List[str]]),
            ("pipeline_snapshot_id", str),
            ("known_state", Optional[KnownExecutionState]),
            ("instance_ref", Optional[InstanceRef]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: ExternalPipelineOrigin,
        solid_selection: List[str],
        run_config: Dict[str, object],
        mode: str,
        step_keys_to_execute: Optional[List[str]],
        pipeline_snapshot_id: str,
        known_state: Optional[KnownExecutionState] = None,
        instance_ref: Optional[InstanceRef] = None,
    ):
        return super(ExecutionPlanSnapshotArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", ExternalPipelineOrigin
            ),
            solid_selection=check.opt_list_param(solid_selection, "solid_selection", of_type=str),
            run_config=check.dict_param(run_config, "run_config", key_type=str),
            mode=check.str_param(mode, "mode"),
            step_keys_to_execute=check.opt_nullable_list_param(
                step_keys_to_execute, "step_keys_to_execute", of_type=str
            ),
            pipeline_snapshot_id=check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id"),
            known_state=check.opt_inst_param(known_state, "known_state", KnownExecutionState),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
        )


def _get_entry_point(origin: PipelinePythonOrigin):
    return (
        origin.repository_origin.entry_point
        if origin.repository_origin.entry_point
        else get_python_environment_entry_point(origin.executable_path)
    )


@whitelist_for_serdes
class ExecuteRunArgs(
    NamedTuple(
        "_ExecuteRunArgs",
        [
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
            ("set_exit_code_on_failure", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
        set_exit_code_on_failure: Optional[bool] = None,
    ):
        return super(ExecuteRunArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                PipelinePythonOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            set_exit_code_on_failure=(
                True
                if check.opt_bool_param(set_exit_code_on_failure, "set_exit_code_on_failure")
                == True
                else None
            ),  # for back-compat
        )

    def get_command_args(self) -> List[str]:
        return _get_entry_point(self.pipeline_origin) + [
            "api",
            "execute_run",
            serialize_dagster_namedtuple(self),
        ]


@whitelist_for_serdes
class ResumeRunArgs(
    NamedTuple(
        "_ResumeRunArgs",
        [
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
            ("set_exit_code_on_failure", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
        set_exit_code_on_failure: Optional[bool] = None,
    ):
        return super(ResumeRunArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                PipelinePythonOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            set_exit_code_on_failure=(
                True
                if check.opt_bool_param(set_exit_code_on_failure, "set_exit_code_on_failure")
                == True
                else None
            ),  # for back-compat
        )

    def get_command_args(self) -> List[str]:
        return _get_entry_point(self.pipeline_origin) + [
            "api",
            "resume_run",
            serialize_dagster_namedtuple(self),
        ]


@whitelist_for_serdes
class ExecuteExternalPipelineArgs(
    NamedTuple(
        "_ExecuteExternalPipelineArgs",
        [
            ("pipeline_origin", ExternalPipelineOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: ExternalPipelineOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
    ):
        return super(ExecuteExternalPipelineArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                ExternalPipelineOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
        )


@whitelist_for_serdes
class ExecuteStepArgs(
    NamedTuple(
        "_ExecuteStepArgs",
        [
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("step_keys_to_execute", Optional[List[str]]),
            ("instance_ref", Optional[InstanceRef]),
            ("retry_mode", Optional[RetryMode]),
            ("known_state", Optional[KnownExecutionState]),
            ("should_verify_step", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        step_keys_to_execute: Optional[List[str]],
        instance_ref: Optional[InstanceRef] = None,
        retry_mode: Optional[RetryMode] = None,
        known_state: Optional[KnownExecutionState] = None,
        should_verify_step: Optional[bool] = None,
    ):
        return super(ExecuteStepArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", PipelinePythonOrigin
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            step_keys_to_execute=check.opt_nullable_list_param(
                step_keys_to_execute, "step_keys_to_execute", of_type=str
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            retry_mode=check.opt_inst_param(retry_mode, "retry_mode", RetryMode),
            known_state=check.opt_inst_param(known_state, "known_state", KnownExecutionState),
            should_verify_step=check.opt_bool_param(
                should_verify_step, "should_verify_step", False
            ),
        )

    def get_command_args(self) -> List[str]:
        return _get_entry_point(self.pipeline_origin) + [
            "api",
            "execute_step",
            serialize_dagster_namedtuple(self),
        ]


@whitelist_for_serdes
class LoadableRepositorySymbol(
    NamedTuple("_LoadableRepositorySymbol", [("repository_name", str), ("attribute", str)])
):
    def __new__(cls, repository_name: str, attribute: str):
        return super(LoadableRepositorySymbol, cls).__new__(
            cls,
            repository_name=check.str_param(repository_name, "repository_name"),
            attribute=check.str_param(attribute, "attribute"),
        )


@whitelist_for_serdes
class ListRepositoriesResponse(
    NamedTuple(
        "_ListRepositoriesResponse",
        [
            ("repository_symbols", List[LoadableRepositorySymbol]),
            ("executable_path", Optional[str]),
            ("repository_code_pointer_dict", Dict[str, CodePointer]),
            ("entry_point", Optional[List[str]]),
        ],
    )
):
    def __new__(
        cls,
        repository_symbols,
        executable_path=None,
        repository_code_pointer_dict=None,
        entry_point=None,
    ):
        return super(ListRepositoriesResponse, cls).__new__(
            cls,
            repository_symbols=check.list_param(
                repository_symbols, "repository_symbols", of_type=LoadableRepositorySymbol
            ),
            executable_path=check.opt_str_param(executable_path, "executable_path"),
            repository_code_pointer_dict=check.opt_dict_param(
                repository_code_pointer_dict,
                "repository_code_pointer_dict",
                key_type=str,
                value_type=CodePointer,
            ),
            entry_point=(
                frozenlist(check.list_param(entry_point, "entry_point", of_type=str))
                if entry_point != None
                else None
            ),
        )


@whitelist_for_serdes
class ListRepositoriesInput(
    NamedTuple(
        "_ListRepositoriesInput",
        [
            ("module_name", Optional[str]),
            ("python_file", Optional[str]),
            ("working_directory", Optional[str]),
            ("attribute", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        module_name: Optional[str],
        python_file: Optional[str],
        working_directory: Optional[str],
        attribute: Optional[str],
    ):
        check.invariant(not (module_name and python_file), "Must set only one")
        check.invariant(module_name or python_file, "Must set at least one")
        return super(ListRepositoriesInput, cls).__new__(
            cls,
            module_name=check.opt_str_param(module_name, "module_name"),
            python_file=check.opt_str_param(python_file, "python_file"),
            working_directory=check.opt_str_param(working_directory, "working_directory"),
            attribute=check.opt_str_param(attribute, "attribute"),
        )


@whitelist_for_serdes
class PartitionArgs(
    NamedTuple(
        "_PartitionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("partition_set_name", str),
            ("partition_name", str),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        partition_set_name: str,
        partition_name: str,
    ):
        return super(PartitionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin,
                "repository_origin",
                ExternalRepositoryOrigin,
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
            partition_name=check.str_param(partition_name, "partition_name"),
        )


@whitelist_for_serdes
class PartitionNamesArgs(
    NamedTuple(
        "_PartitionNamesArgs",
        [("repository_origin", ExternalRepositoryOrigin), ("partition_set_name", str)],
    )
):
    def __new__(cls, repository_origin: ExternalRepositoryOrigin, partition_set_name: str):
        return super(PartitionNamesArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
        )


@whitelist_for_serdes
class PartitionSetExecutionParamArgs(
    NamedTuple(
        "_PartitionSetExecutionParamArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("partition_set_name", str),
            ("partition_names", List[str]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        partition_set_name: str,
        partition_names: List[str],
    ):
        return super(PartitionSetExecutionParamArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
            partition_names=check.list_param(partition_names, "partition_names", of_type=str),
        )


@whitelist_for_serdes
class PipelineSubsetSnapshotArgs(
    NamedTuple(
        "_PipelineSubsetSnapshotArgs",
        [("pipeline_origin", ExternalPipelineOrigin), ("solid_selection", Optional[List[str]])],
    )
):
    def __new__(cls, pipeline_origin: ExternalPipelineOrigin, solid_selection: List[str]):
        return super(PipelineSubsetSnapshotArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", ExternalPipelineOrigin
            ),
            solid_selection=check.list_param(solid_selection, "solid_selection", of_type=str)
            if solid_selection
            else None,
        )


@whitelist_for_serdes
class NotebookPathArgs(
    NamedTuple(
        "_NotebookPathArgs",
        [("repository_location_origin", RepositoryLocationOrigin), ("notebook_path", str)],
    )
):
    def __new__(cls, repository_location_origin: RepositoryLocationOrigin, notebook_path: str):
        return super(NotebookPathArgs, cls).__new__(
            cls,
            repository_location_origin=check.inst_param(
                repository_location_origin, "repository_location_origin", RepositoryLocationOrigin
            ),
            notebook_path=check.str_param(notebook_path, "notebook_path"),
        )


@whitelist_for_serdes
class ExternalScheduleExecutionArgs(
    NamedTuple(
        "_ExternalScheduleExecutionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", Optional[InstanceRef]),
            ("schedule_name", str),
            ("scheduled_execution_timestamp", Optional[float]),
            ("scheduled_execution_timezone", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        instance_ref: Optional[InstanceRef],
        schedule_name: str,
        scheduled_execution_timestamp: Optional[float] = None,
        scheduled_execution_timezone: Optional[str] = None,
    ):
        return super(ExternalScheduleExecutionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            schedule_name=check.str_param(schedule_name, "schedule_name"),
            scheduled_execution_timestamp=check.opt_float_param(
                scheduled_execution_timestamp, "scheduled_execution_timestamp"
            ),
            scheduled_execution_timezone=check.opt_str_param(
                scheduled_execution_timezone,
                "scheduled_execution_timezone",
            ),
        )


@whitelist_for_serdes
class SensorExecutionArgs(
    NamedTuple(
        "_SensorExecutionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", Optional[InstanceRef]),
            ("sensor_name", str),
            ("last_completion_time", Optional[float]),
            ("last_run_key", Optional[str]),
            ("cursor", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        instance_ref: Optional[InstanceRef],
        sensor_name: str,
        last_completion_time: Optional[float],
        last_run_key: Optional[str],
        cursor: Optional[str],
    ):
        return super(SensorExecutionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            sensor_name=check.str_param(sensor_name, "sensor_name"),
            last_completion_time=check.opt_float_param(
                last_completion_time, "last_completion_time"
            ),
            last_run_key=check.opt_str_param(last_run_key, "last_run_key"),
            cursor=check.opt_str_param(cursor, "cursor"),
        )


@whitelist_for_serdes
class ExternalJobArgs(
    NamedTuple(
        "_ExternalJobArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", InstanceRef),
            ("name", str),
        ],
    )
):
    def __new__(
        cls, repository_origin: ExternalRepositoryOrigin, instance_ref: InstanceRef, name: str
    ):
        return super(ExternalJobArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.inst_param(instance_ref, "instance_ref", InstanceRef),
            name=check.str_param(name, "name"),
        )


@whitelist_for_serdes
class ShutdownServerResult(
    NamedTuple(
        "_ShutdownServerResult",
        [("success", bool), ("serializable_error_info", Optional[SerializableErrorInfo])],
    )
):
    def __new__(cls, success: bool, serializable_error_info: Optional[SerializableErrorInfo]):
        return super(ShutdownServerResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class CancelExecutionRequest(NamedTuple("_CancelExecutionRequest", [("run_id", str)])):
    def __new__(cls, run_id: str):
        return super(CancelExecutionRequest, cls).__new__(
            cls,
            run_id=check.str_param(run_id, "run_id"),
        )


@whitelist_for_serdes
class CancelExecutionResult(
    NamedTuple(
        "_CancelExecutionResult",
        [
            ("success", bool),
            ("message", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        success: bool,
        message: Optional[str],
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(CancelExecutionResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            message=check.opt_str_param(message, "message"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class CanCancelExecutionRequest(NamedTuple("_CanCancelExecutionRequest", [("run_id", str)])):
    def __new__(cls, run_id: str):
        return super(CanCancelExecutionRequest, cls).__new__(
            cls,
            run_id=check.str_param(run_id, "run_id"),
        )


@whitelist_for_serdes
class CanCancelExecutionResult(NamedTuple("_CancelExecutionResult", [("can_cancel", bool)])):
    def __new__(cls, can_cancel: bool):
        return super(CanCancelExecutionResult, cls).__new__(
            cls,
            can_cancel=check.bool_param(can_cancel, "can_cancel"),
        )


@whitelist_for_serdes
class StartRunResult(
    NamedTuple(
        "_StartRunResult",
        [
            ("success", bool),
            ("message", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        success: bool,
        message: Optional[str],
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(StartRunResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            message=check.opt_str_param(message, "message"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class GetCurrentImageResult(
    NamedTuple(
        "_GetCurrentImageResult",
        [
            ("current_image", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls, current_image: Optional[str], serializable_error_info: Optional[SerializableErrorInfo]
    ):
        return super(GetCurrentImageResult, cls).__new__(
            cls,
            current_image=check.opt_str_param(current_image, "current_image"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )
