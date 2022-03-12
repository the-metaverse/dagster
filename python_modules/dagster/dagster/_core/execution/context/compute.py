from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Mapping, Optional

from dagster import _check as check
from dagster._core.definitions.dependency import Node, NodeHandle
from dagster._core.definitions.events import (
    AssetMaterialization,
    AssetObservation,
    ExpectationResult,
    Materialization,
    UserEvent,
)
from dagster._core.definitions.mode import ModeDefinition
from dagster._core.definitions.pipeline_definition import PipelineDefinition
from dagster._core.definitions.solid_definition import SolidDefinition
from dagster._core.definitions.step_launcher import StepLauncher
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.errors import DagsterInvalidPropertyError
from dagster._core.events import DagsterEvent
from dagster._core.instance import DagsterInstance
from dagster._core.log_manager import DagsterLogManager
from dagster._core.storage.pipeline_run import PipelineRun
from dagster._utils.forked_pdb import ForkedPdb

from .system import StepExecutionContext


class AbstractComputeExecutionContext(ABC):  # pylint: disable=no-init
    """Base class for solid context implemented by SolidExecutionContext and DagstermillExecutionContext"""

    @abstractmethod
    def has_tag(self, key) -> bool:
        """Implement this method to check if a logging tag is set."""

    @abstractmethod
    def get_tag(self, key: str) -> Optional[str]:
        """Implement this method to get a logging tag."""

    @property
    @abstractmethod
    def run_id(self) -> str:
        """The run id for the context."""

    @property
    @abstractmethod
    def solid_def(self) -> SolidDefinition:
        """The solid definition corresponding to the execution step being executed."""

    @property
    @abstractmethod
    def solid(self) -> Node:
        """The solid corresponding to the execution step being executed."""

    @property
    @abstractmethod
    def pipeline_def(self) -> PipelineDefinition:
        """The pipeline being executed."""

    @property
    @abstractmethod
    def pipeline_run(self) -> PipelineRun:
        """The PipelineRun object corresponding to the execution."""

    @property
    @abstractmethod
    def resources(self) -> Any:
        """Resources available in the execution context."""

    @property
    @abstractmethod
    def log(self) -> DagsterLogManager:
        """The log manager available in the execution context."""

    @property
    @abstractmethod
    def solid_config(self) -> Any:
        """The parsed config specific to this solid."""

    @property
    def op_config(self) -> Any:
        return self.solid_config


class SolidExecutionContext(AbstractComputeExecutionContext):
    """The ``context`` object that can be made available as the first argument to a solid's compute
    function.

    The context object provides system information such as resources, config, and logging to a
    solid's compute function. Users should not instantiate this object directly.

    Example:

    .. code-block:: python

        @solid
        def hello_world(context: SolidExecutionContext):
            context.log.info("Hello, world!")

    """

    __slots__ = ["_step_execution_context"]

    def __init__(self, step_execution_context: StepExecutionContext):
        self._step_execution_context = check.inst_param(
            step_execution_context,
            "step_execution_context",
            StepExecutionContext,
        )
        self._pdb: Optional[ForkedPdb] = None
        self._events: List[DagsterEvent] = []
        self._output_metadata: Dict[str, Any] = {}

    @property
    def solid_config(self) -> Any:
        return self._step_execution_context.op_config

    @property
    def pipeline_run(self) -> PipelineRun:
        """PipelineRun: The current pipeline run"""
        return self._step_execution_context.pipeline_run

    @property
    def instance(self) -> DagsterInstance:
        """DagsterInstance: The current Dagster instance"""
        return self._step_execution_context.instance

    @property
    def pdb(self) -> ForkedPdb:
        """dagster._utils.forked_pdb.ForkedPdb: Gives access to pdb debugging from within the op.

        Example:

        .. code-block:: python

            @op
            def debug(context):
                context.pdb.set_trace()

        """
        if self._pdb is None:
            self._pdb = ForkedPdb()

        return self._pdb

    @property
    def file_manager(self):
        """Deprecated access to the file manager.

        :meta private:
        """
        raise DagsterInvalidPropertyError(
            "You have attempted to access the file manager which has been moved to resources in 0.10.0. "
            "Please access it via `context.resources.file_manager` instead."
        )

    @property
    def resources(self) -> Any:
        """Resources: The currently available resources."""
        return self._step_execution_context.resources

    @property
    def step_launcher(self) -> Optional[StepLauncher]:
        """Optional[StepLauncher]: The current step launcher, if any."""
        return self._step_execution_context.step_launcher

    @property
    def run_id(self) -> str:
        """str: The id of the current execution's run."""
        return self._step_execution_context.run_id

    @property
    def run_config(self) -> dict:
        """dict: The run config for the current execution."""
        return self._step_execution_context.run_config

    @property
    def pipeline_def(self) -> PipelineDefinition:
        """PipelineDefinition: The currently executing pipeline."""
        return self._step_execution_context.pipeline_def

    @property
    def pipeline_name(self) -> str:
        """str: The name of the currently executing pipeline."""
        return self._step_execution_context.pipeline_name

    @property
    def mode_def(self) -> ModeDefinition:
        """ModeDefinition: The mode of the current execution."""
        return self._step_execution_context.mode_def

    @property
    def log(self) -> DagsterLogManager:
        """DagsterLogManager: The log manager available in the execution context."""
        return self._step_execution_context.log

    @property
    def solid_handle(self) -> NodeHandle:
        """NodeHandle: The current solid's handle.

        :meta private:
        """
        return self._step_execution_context.solid_handle

    @property
    def solid(self) -> Node:
        """Solid: The current solid object.

        :meta private:

        """
        return self._step_execution_context.pipeline_def.get_solid(self.solid_handle)

    @property
    def solid_def(self) -> SolidDefinition:
        """SolidDefinition: The current solid definition."""
        return self._step_execution_context.pipeline_def.get_solid(self.solid_handle).definition

    @property
    def has_partition_key(self) -> bool:
        """Whether the current run is a partitioned run"""
        return self._step_execution_context.has_partition_key

    @property
    def partition_key(self) -> str:
        """The partition key for the current run.

        Raises an error if the current run is not a partitioned run.
        """
        return self._step_execution_context.partition_key

    def output_asset_partition_key(self, output_name: str = "result") -> str:
        """Returns the asset partition key for the given output. Defaults to "result", which is the
        name of the default output.
        """
        return self._step_execution_context.asset_partition_key_for_output(output_name)

    def output_asset_partitions_time_window(self, output_name: str = "result") -> TimeWindow:
        """The time window for the partitions of the output asset.

        Raises an error if either of the following are true:
        - The output asset has no partitioning.
        - The output asset is not partitioned with a TimeWindowPartitionsDefinition.
        """
        return self._step_execution_context.asset_partitions_time_window_for_output(output_name)

    def has_tag(self, key: str) -> bool:
        """Check if a logging tag is set.

        Args:
            key (str): The tag to check.

        Returns:
            bool: Whether the tag is set.
        """
        return self._step_execution_context.has_tag(key)

    def get_tag(self, key: str) -> Optional[str]:
        """Get a logging tag.

        Args:
            key (tag): The tag to get.

        Returns:
            Optional[str]: The value of the tag, if present.
        """
        return self._step_execution_context.get_tag(key)

    def has_events(self) -> bool:
        return bool(self._events)

    def consume_events(self) -> Iterator[DagsterEvent]:
        """Pops and yields all user-generated events that have been recorded from this context.

        If consume_events has not yet been called, this will yield all logged events since the beginning of the op's computation. If consume_events has been called, it will yield all events since the last time consume_events was called. Designed for internal use. Users should never need to invoke this method.
        """
        events = self._events
        self._events = []
        yield from events

    def log_event(self, event: UserEvent) -> None:
        """Log an AssetMaterialization, AssetObservation, or ExpectationResult from within the body of an op.

        Events logged with this method will appear in the list of DagsterEvents, as well as the event log.

        Args:
            event (Union[AssetMaterialization, Materialization, AssetObservation, ExpectationResult]): The event to log.

        **Examples:**

        .. code-block:: python

            from dagster import op, AssetMaterialization

            @op
            def log_materialization(context):
                context.log_event(AssetMaterialization("foo"))
        """

        if isinstance(event, (AssetMaterialization, Materialization)):
            self._events.append(
                DagsterEvent.asset_materialization(
                    self._step_execution_context,
                    event,
                    self._step_execution_context.get_input_lineage(),
                )
            )
        elif isinstance(event, AssetObservation):
            self._events.append(DagsterEvent.asset_observation(self._step_execution_context, event))
        elif isinstance(event, ExpectationResult):
            self._events.append(
                DagsterEvent.step_expectation_result(self._step_execution_context, event)
            )
        else:
            check.failed("Unexpected event {event}".format(event=event))

    def add_output_metadata(
        self,
        metadata: Mapping[str, Any],
        output_name: Optional[str] = None,
        mapping_key: Optional[str] = None,
    ) -> None:
        """Add metadata to one of the outputs of an op.

        This can only be used once per output in the body of an op. Using this method with the same output_name more than once within an op will result in an error.

        Args:
            metadata (Mapping[str, Any]): The metadata to attach to the output
            output_name (Optional[str]): The name of the output to attach metadata to. If there is only one output on the op, then this argument does not need to be provided. The metadata will automatically be attached to the only output.

        **Examples:**

        .. code-block:: python

            from dagster import Out, op
            from typing import Tuple

            @op
            def add_metadata(context):
                context.add_output_metadata({"foo", "bar"})
                return 5 # Since the default output is called "result", metadata will be attached to the output "result".

            @op(out={"a": Out(), "b": Out()})
            def add_metadata_two_outputs(context) -> Tuple[str, int]:
                context.add_output_metadata({"foo": "bar"}, output_name="b")
                context.add_output_metadata({"baz": "bat"}, output_name="a")

                return ("dog", 5)

        """
        metadata = check.dict_param(metadata, "metadata", key_type=str)
        output_name = check.opt_str_param(output_name, "output_name")
        mapping_key = check.opt_str_param(mapping_key, "mapping_key")

        self._step_execution_context.add_output_metadata(
            metadata=metadata, output_name=output_name, mapping_key=mapping_key
        )

    def get_output_metadata(
        self, output_name: str, mapping_key: Optional[str] = None
    ) -> Optional[Mapping[str, Any]]:
        return self._step_execution_context.get_output_metadata(
            output_name=output_name, mapping_key=mapping_key
        )

    def get_step_execution_context(self) -> StepExecutionContext:
        """Allows advanced users (e.g. framework authors) to punch through to the underlying
        step execution context.

        :meta private:

        Returns:
            StepExecutionContext: The underlying system context.
        """
        return self._step_execution_context

    @property
    def retry_number(self) -> int:
        """
        Which retry attempt is currently executing i.e. 0 for initial attempt, 1 for first retry, etc.
        """

        return self._step_execution_context.previous_attempt_count

    def describe_op(self):
        return self._step_execution_context.describe_op()

    def get_mapping_key(self) -> Optional[str]:
        """
        Which mapping_key this execution is for if downstream of a DynamicOutput, otherwise None.
        """
        return self._step_execution_context.step.get_mapping_key()


class OpExecutionContext(SolidExecutionContext):
    pass
