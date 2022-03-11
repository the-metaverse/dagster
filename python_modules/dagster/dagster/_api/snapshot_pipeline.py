from typing import TYPE_CHECKING, List, Optional

from dagster import check
from dagster._core.errors import DagsterUserCodeProcessError
from dagster._core.host_representation.external_data import ExternalPipelineSubsetResult
from dagster._core.host_representation.origin import ExternalPipelineOrigin
from dagster._grpc.types import PipelineSubsetSnapshotArgs
from dagster.serdes import deserialize_as

if TYPE_CHECKING:
    from dagster._grpc.client import DagsterGrpcClient


def sync_get_external_pipeline_subset_grpc(
    api_client: "DagsterGrpcClient",
    pipeline_origin: ExternalPipelineOrigin,
    solid_selection: Optional[List[str]] = None,
) -> ExternalPipelineSubsetResult:
    from dagster._grpc.client import DagsterGrpcClient

    check.inst_param(api_client, "api_client", DagsterGrpcClient)
    pipeline_origin = check.inst_param(pipeline_origin, "pipeline_origin", ExternalPipelineOrigin)
    solid_selection = check.opt_list_param(solid_selection, "solid_selection", of_type=str)

    result = deserialize_as(
        api_client.external_pipeline_subset(
            pipeline_subset_snapshot_args=PipelineSubsetSnapshotArgs(
                pipeline_origin=pipeline_origin, solid_selection=solid_selection
            ),
        ),
        ExternalPipelineSubsetResult,
    )

    if result.error:
        raise DagsterUserCodeProcessError.from_error_info(result.error)

    return result
