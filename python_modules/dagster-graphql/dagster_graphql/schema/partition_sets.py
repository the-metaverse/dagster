import graphene
from dagster_graphql.implementation.fetch_partition_sets import (
    get_partition_by_name,
    get_partition_config,
    get_partition_set_partition_statuses,
    get_partition_tags,
    get_partitions,
)
from dagster_graphql.implementation.fetch_runs import get_runs

from dagster import check
from dagster._core.host_representation import ExternalPartitionSet, RepositoryHandle
from dagster._core.storage.pipeline_run import RunsFilter
from dagster._core.storage.tags import PARTITION_NAME_TAG, PARTITION_SET_TAG
from dagster.utils import merge_dicts

from .errors import (
    GraphenePartitionSetNotFoundError,
    GraphenePipelineNotFoundError,
    GraphenePythonError,
)
from .inputs import GrapheneRunsFilter
from .pipelines.pipeline import GrapheneRun
from .pipelines.status import GrapheneRunStatus
from .repository_origin import GrapheneRepositoryOrigin
from .tags import GraphenePipelineTag
from .util import non_null_list


class GraphenePartitionTags(graphene.ObjectType):
    results = non_null_list(GraphenePipelineTag)

    class Meta:
        name = "PartitionTags"


class GraphenePartitionRunConfig(graphene.ObjectType):
    yaml = graphene.NonNull(graphene.String)

    class Meta:
        name = "PartitionRunConfig"


class GraphenePartitionRunConfigOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitionRunConfig, GraphenePythonError)
        name = "PartitionRunConfigOrError"


class GraphenePartitionStatus(graphene.ObjectType):
    id = graphene.NonNull(graphene.String)
    partitionName = graphene.NonNull(graphene.String)
    runStatus = graphene.Field(GrapheneRunStatus)

    class Meta:
        name = "PartitionStatus"


class GraphenePartitionStatuses(graphene.ObjectType):
    results = non_null_list(GraphenePartitionStatus)

    class Meta:
        name = "PartitionStatuses"


class GraphenePartitionStatusesOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitionStatuses, GraphenePythonError)
        name = "PartitionStatusesOrError"


class GraphenePartitionTagsOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitionTags, GraphenePythonError)
        name = "PartitionTagsOrError"


class GraphenePartition(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    partition_set_name = graphene.NonNull(graphene.String)
    solid_selection = graphene.List(graphene.NonNull(graphene.String))
    mode = graphene.NonNull(graphene.String)
    runConfigOrError = graphene.NonNull(GraphenePartitionRunConfigOrError)
    tagsOrError = graphene.NonNull(GraphenePartitionTagsOrError)
    runs = graphene.Field(
        non_null_list(GrapheneRun),
        filter=graphene.Argument(GrapheneRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
    )
    status = graphene.Field(GrapheneRunStatus)

    class Meta:
        name = "Partition"

    def __init__(self, external_repository_handle, external_partition_set, partition_name):
        self._external_repository_handle = check.inst_param(
            external_repository_handle, "external_respository_handle", RepositoryHandle
        )
        self._external_partition_set = check.inst_param(
            external_partition_set, "external_partition_set", ExternalPartitionSet
        )
        self._partition_name = check.str_param(partition_name, "partition_name")

        super().__init__(
            name=partition_name,
            partition_set_name=external_partition_set.name,
            solid_selection=external_partition_set.solid_selection,
            mode=external_partition_set.mode,
        )

    def resolve_runConfigOrError(self, graphene_info):
        return get_partition_config(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set.name,
            self._partition_name,
        )

    def resolve_tagsOrError(self, graphene_info):
        return get_partition_tags(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set.name,
            self._partition_name,
        )

    def resolve_runs(self, graphene_info, **kwargs):
        filters = kwargs.get("filter")
        partition_tags = {
            PARTITION_SET_TAG: self._external_partition_set.name,
            PARTITION_NAME_TAG: self._partition_name,
        }
        if filters is not None:
            filters = filters.to_selector()
            runs_filter = RunsFilter(
                run_ids=filters.run_ids,
                pipeline_name=filters.job_name,
                statuses=filters.statuses,
                tags=merge_dicts(filters.tags, partition_tags),
            )
        else:
            runs_filter = RunsFilter(tags=partition_tags)

        return get_runs(
            graphene_info, runs_filter, cursor=kwargs.get("cursor"), limit=kwargs.get("limit")
        )


class GraphenePartitions(graphene.ObjectType):
    results = non_null_list(GraphenePartition)

    class Meta:
        name = "Partitions"


class GraphenePartitionsOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitions, GraphenePythonError)
        name = "PartitionsOrError"


class GraphenePartitionSet(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    pipeline_name = graphene.NonNull(graphene.String)
    solid_selection = graphene.List(graphene.NonNull(graphene.String))
    mode = graphene.NonNull(graphene.String)
    partitionsOrError = graphene.Field(
        graphene.NonNull(GraphenePartitionsOrError),
        cursor=graphene.String(),
        limit=graphene.Int(),
        reverse=graphene.Boolean(),
    )
    partition = graphene.Field(GraphenePartition, partition_name=graphene.NonNull(graphene.String))
    partitionStatusesOrError = graphene.NonNull(GraphenePartitionStatusesOrError)
    repositoryOrigin = graphene.NonNull(GrapheneRepositoryOrigin)

    class Meta:
        name = "PartitionSet"

    def __init__(self, external_repository_handle, external_partition_set):
        self._external_repository_handle = check.inst_param(
            external_repository_handle, "external_respository_handle", RepositoryHandle
        )
        self._external_partition_set = check.inst_param(
            external_partition_set, "external_partition_set", ExternalPartitionSet
        )

        super().__init__(
            name=external_partition_set.name,
            pipeline_name=external_partition_set.pipeline_name,
            solid_selection=external_partition_set.solid_selection,
            mode=external_partition_set.mode,
        )

    def resolve_id(self, _graphene_info):
        return self._external_partition_set.get_external_origin_id()

    def resolve_partitionsOrError(self, graphene_info, **kwargs):
        return get_partitions(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set,
            cursor=kwargs.get("cursor"),
            limit=kwargs.get("limit"),
            reverse=kwargs.get("reverse"),
        )

    def resolve_partition(self, graphene_info, partition_name):
        return get_partition_by_name(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set,
            partition_name,
        )

    def resolve_partitionStatusesOrError(self, graphene_info):
        return get_partition_set_partition_statuses(
            graphene_info, self._external_repository_handle, self._external_partition_set.name
        )

    def resolve_repositoryOrigin(self, _):
        origin = self._external_partition_set.get_external_origin().external_repository_origin
        return GrapheneRepositoryOrigin(origin)


class GraphenePartitionSetOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitionSet, GraphenePartitionSetNotFoundError, GraphenePythonError)
        name = "PartitionSetOrError"


class GraphenePartitionSets(graphene.ObjectType):
    results = non_null_list(GraphenePartitionSet)

    class Meta:
        name = "PartitionSets"


class GraphenePartitionSetsOrError(graphene.Union):
    class Meta:
        types = (GraphenePartitionSets, GraphenePipelineNotFoundError, GraphenePythonError)
        name = "PartitionSetsOrError"


types = [
    GraphenePartition,
    GraphenePartitionRunConfig,
    GraphenePartitionRunConfigOrError,
    GraphenePartitions,
    GraphenePartitionSet,
    GraphenePartitionSetOrError,
    GraphenePartitionSets,
    GraphenePartitionSetsOrError,
    GraphenePartitionsOrError,
    GraphenePartitionStatus,
    GraphenePartitionStatuses,
    GraphenePartitionStatusesOrError,
    GraphenePartitionTags,
    GraphenePartitionTagsOrError,
]
