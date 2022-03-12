from dagster import _check as check
from dagster._core.host_representation import RepositoryLocation
from dagster._core.host_representation.origin import ExternalPipelineOrigin
from dagster._core.host_representation.selector import PipelineSelector


def external_pipeline_from_location(repo_location, external_pipeline_origin, solid_selection):
    check.inst_param(repo_location, "repository_location", RepositoryLocation)
    check.inst_param(external_pipeline_origin, "external_pipeline_origin", ExternalPipelineOrigin)

    repo_name = external_pipeline_origin.external_repository_origin.repository_name
    pipeline_name = external_pipeline_origin.pipeline_name

    check.invariant(
        repo_location.has_repository(repo_name),
        "Could not find repository {repo_name} in location {repo_location_name}".format(
            repo_name=repo_name, repo_location_name=repo_location.name
        ),
    )
    external_repo = repo_location.get_repository(repo_name)

    pipeline_selector = PipelineSelector(
        location_name=repo_location.name,
        repository_name=external_repo.name,
        pipeline_name=pipeline_name,
        solid_selection=solid_selection,
    )

    return repo_location.get_external_pipeline(pipeline_selector)
