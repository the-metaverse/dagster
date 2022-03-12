from dagster import check
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.host_representation import InstigationSelector
from dagster._core.scheduler.instigation import InstigatorStatus

from .utils import capture_error


@capture_error
def get_unloadable_instigator_states_or_error(graphene_info, instigator_type=None):
    from ..schema.instigation import GrapheneInstigationState, GrapheneInstigationStates

    check.opt_inst_param(instigator_type, "instigator_type", InstigatorType)
    instigator_states = graphene_info.context.instance.all_instigator_state(
        instigator_type=instigator_type
    )
    external_instigators = [
        instigator
        for repository_location in graphene_info.context.repository_locations
        for repository in repository_location.get_repositories().values()
        for instigator in repository.get_external_schedules() + repository.get_external_sensors()
    ]

    instigator_origin_ids = {
        instigator.get_external_origin_id() for instigator in external_instigators
    }

    unloadable_states = [
        instigator_state
        for instigator_state in instigator_states
        if instigator_state.instigator_origin_id not in instigator_origin_ids
        and instigator_state.status == InstigatorStatus.RUNNING
    ]

    return GrapheneInstigationStates(
        results=[
            GrapheneInstigationState(instigator_state=instigator_state)
            for instigator_state in unloadable_states
        ]
    )


@capture_error
def get_instigator_state_or_error(graphene_info, selector):
    from ..schema.instigation import GrapheneInstigationState

    check.inst_param(selector, "selector", InstigationSelector)
    location = graphene_info.context.get_repository_location(selector.location_name)
    repository = location.get_repository(selector.repository_name)

    if repository.has_external_sensor(selector.name):
        external_sensor = repository.get_external_sensor(selector.name)
        stored_state = graphene_info.context.instance.get_instigator_state(
            external_sensor.get_external_origin_id()
        )
        current_state = external_sensor.get_current_instigator_state(stored_state)
    elif repository.has_external_schedule(selector.name):
        external_schedule = repository.get_external_schedule(selector.name)
        stored_state = graphene_info.context.instance.get_instigator_state(
            external_schedule.get_external_origin_id()
        )
        current_state = external_schedule.get_current_instigator_state(stored_state)
    else:
        check.failed(f"Could not find a definition for {selector.name}")

    return GrapheneInstigationState(current_state)
