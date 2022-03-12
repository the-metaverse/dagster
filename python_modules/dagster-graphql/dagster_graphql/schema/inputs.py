import graphene
import pendulum

from dagster._core.storage.pipeline_run import PipelineRunStatus, RunsFilter

from .pipelines.status import GrapheneRunStatus
from .runs import GrapheneRunConfigData
from .util import non_null_list


class GrapheneAssetKeyInput(graphene.InputObjectType):
    path = non_null_list(graphene.String)

    class Meta:
        name = "AssetKeyInput"


class GrapheneExecutionTag(graphene.InputObjectType):
    key = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.String)

    class Meta:
        name = "ExecutionTag"


class GrapheneRunsFilter(graphene.InputObjectType):
    runIds = graphene.List(graphene.String)
    pipelineName = graphene.Field(graphene.String)
    tags = graphene.List(graphene.NonNull(GrapheneExecutionTag))
    statuses = graphene.List(graphene.NonNull(GrapheneRunStatus))
    snapshotId = graphene.Field(graphene.String)
    updatedAfter = graphene.Field(graphene.Float)
    createdBefore = graphene.Field(graphene.Float)
    mode = graphene.Field(graphene.String)

    class Meta:
        description = """This type represents a filter on Dagster runs."""
        name = "RunsFilter"

    def to_selector(self):
        if self.tags:
            # We are wrapping self.tags in a list because graphene.List is not marked as iterable
            tags = {tag["key"]: tag["value"] for tag in list(self.tags)}
        else:
            tags = None

        if self.statuses:
            statuses = [
                PipelineRunStatus[status]
                for status in self.statuses  # pylint: disable=not-an-iterable
            ]
        else:
            statuses = None

        updated_after = pendulum.from_timestamp(self.updatedAfter) if self.updatedAfter else None
        created_before = pendulum.from_timestamp(self.createdBefore) if self.createdBefore else None

        return RunsFilter(
            run_ids=self.runIds,
            pipeline_name=self.pipelineName,
            tags=tags,
            statuses=statuses,
            snapshot_id=self.snapshotId,
            updated_after=updated_after,
            mode=self.mode,
            created_before=created_before,
        )


class GrapheneStepOutputHandle(graphene.InputObjectType):
    stepKey = graphene.NonNull(graphene.String)
    outputName = graphene.NonNull(graphene.String)

    class Meta:
        name = "StepOutputHandle"


class GraphenePipelineSelector(graphene.InputObjectType):
    pipelineName = graphene.NonNull(graphene.String)
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))

    class Meta:
        description = """This type represents the fields necessary to identify a
        pipeline or pipeline subset."""
        name = "PipelineSelector"


class GrapheneGraphSelector(graphene.InputObjectType):
    graphName = graphene.NonNull(graphene.String)
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)

    class Meta:
        description = """This type represents the fields necessary to identify a
        graph"""
        name = "GraphSelector"


class GrapheneJobOrPipelineSelector(graphene.InputObjectType):
    pipelineName = graphene.String()
    jobName = graphene.String()
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))

    class Meta:
        description = """This type represents the fields necessary to identify a job or pipeline"""
        name = "JobOrPipelineSelector"


class GrapheneRepositorySelector(graphene.InputObjectType):
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)

    class Meta:
        description = """This type represents the fields necessary to identify a repository."""
        name = "RepositorySelector"


class GraphenePartitionSetSelector(graphene.InputObjectType):
    partitionSetName = graphene.NonNull(graphene.String)
    repositorySelector = graphene.NonNull(GrapheneRepositorySelector)

    class Meta:
        description = """This type represents the fields necessary to identify a
        pipeline or pipeline subset."""
        name = "PartitionSetSelector"


class GrapheneLaunchBackfillParams(graphene.InputObjectType):
    selector = graphene.NonNull(GraphenePartitionSetSelector)
    partitionNames = non_null_list(graphene.String)
    reexecutionSteps = graphene.List(graphene.NonNull(graphene.String))
    fromFailure = graphene.Boolean()
    tags = graphene.List(graphene.NonNull(GrapheneExecutionTag))
    forceSynchronousSubmission = graphene.Boolean()

    class Meta:
        name = "LaunchBackfillParams"


class GrapheneSensorSelector(graphene.InputObjectType):
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    sensorName = graphene.NonNull(graphene.String)

    class Meta:
        description = """This type represents the fields necessary to identify a sensor."""
        name = "SensorSelector"


class GrapheneScheduleSelector(graphene.InputObjectType):
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    scheduleName = graphene.NonNull(graphene.String)

    class Meta:
        description = """This type represents the fields necessary to identify a schedule."""
        name = "ScheduleSelector"


class GrapheneExecutionMetadata(graphene.InputObjectType):
    runId = graphene.String()
    tags = graphene.List(graphene.NonNull(GrapheneExecutionTag))
    rootRunId = graphene.String(
        description="""The ID of the run at the root of the run group. All partial /
        full re-executions should use the first run as the rootRunID so they are
        grouped together."""
    )
    parentRunId = graphene.String(
        description="""The ID of the run serving as the parent within the run group.
        For the first re-execution, this will be the same as the `rootRunId`. For
        subsequent runs, the root or a previous re-execution could be the parent run."""
    )

    class Meta:
        name = "ExecutionMetadata"


class GrapheneExecutionParams(graphene.InputObjectType):
    selector = graphene.NonNull(
        GrapheneJobOrPipelineSelector,
        description="""Defines the job / pipeline and solid subset that should be executed.
        All subsequent executions in the same run group (for example, a single-step
        re-execution) are scoped to the original run's selector and solid
        subset.""",
    )
    runConfigData = graphene.Field(GrapheneRunConfigData)
    mode = graphene.Field(graphene.String)
    executionMetadata = graphene.Field(
        GrapheneExecutionMetadata,
        description="""Defines run tags and parent / root relationships.\n\nNote: To
        'restart from failure', provide a `parentRunId` and pass the
        'dagster/is_resume_retry' tag. Dagster's automatic step key selection will
        override any stepKeys provided.""",
    )
    stepKeys = graphene.Field(
        graphene.List(graphene.NonNull(graphene.String)),
        description="""Defines step keys to execute within the execution plan defined
        by the pipeline `selector`. To execute the entire execution plan, you can omit
        this parameter, provide an empty array, or provide every step name.""",
    )
    preset = graphene.Field(graphene.String)

    class Meta:
        name = "ExecutionParams"


class GrapheneMarshalledInput(graphene.InputObjectType):
    input_name = graphene.NonNull(graphene.String)
    key = graphene.NonNull(graphene.String)

    class Meta:
        name = "MarshalledInput"


class GrapheneMarshalledOutput(graphene.InputObjectType):
    output_name = graphene.NonNull(graphene.String)
    key = graphene.NonNull(graphene.String)

    class Meta:
        name = "MarshalledOutput"


class GrapheneStepExecution(graphene.InputObjectType):
    stepKey = graphene.NonNull(graphene.String)
    marshalledInputs = graphene.List(graphene.NonNull(GrapheneMarshalledInput))
    marshalledOutputs = graphene.List(graphene.NonNull(GrapheneMarshalledOutput))

    class Meta:
        name = "StepExecution"


class GrapheneInstigationSelector(graphene.InputObjectType):
    class Meta:
        name = "InstigationSelector"
        description = (
            """This type represents the fields necessary to identify a schedule or sensor."""
        )

    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    name = graphene.NonNull(graphene.String)


types = [
    GrapheneAssetKeyInput,
    GrapheneExecutionMetadata,
    GrapheneExecutionParams,
    GrapheneExecutionTag,
    GrapheneInstigationSelector,
    GrapheneMarshalledInput,
    GrapheneMarshalledOutput,
    GrapheneLaunchBackfillParams,
    GraphenePartitionSetSelector,
    GrapheneRunsFilter,
    GraphenePipelineSelector,
    GrapheneRepositorySelector,
    GrapheneScheduleSelector,
    GrapheneSensorSelector,
    GrapheneStepExecution,
    GrapheneStepOutputHandle,
]
