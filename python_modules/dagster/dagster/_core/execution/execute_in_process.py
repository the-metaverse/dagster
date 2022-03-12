from typing import Any, Dict, Optional

from dagster._core.definitions import NodeDefinition, PipelineDefinition
from dagster._core.definitions.pipeline_base import InMemoryPipeline
from dagster._core.execution.plan.outputs import StepOutputHandle
from dagster._core.instance import DagsterInstance

from .api import (
    ExecuteRunWithPlanIterable,
    create_execution_plan,
    ephemeral_instance_if_missing,
    pipeline_execution_iterator,
)
from .context_creation_pipeline import (
    PlanOrchestrationContextManager,
    orchestration_context_event_generator,
)
from .execute_in_process_result import ExecuteInProcessResult


def core_execute_in_process(
    node: NodeDefinition,
    run_config: Dict[str, Any],
    ephemeral_pipeline: PipelineDefinition,
    instance: Optional[DagsterInstance],
    output_capturing_enabled: bool,
    raise_on_error: bool,
    run_tags: Optional[Dict[str, Any]] = None,
) -> ExecuteInProcessResult:
    pipeline_def = ephemeral_pipeline
    mode_def = pipeline_def.get_mode_definition()
    pipeline = InMemoryPipeline(pipeline_def)

    execution_plan = create_execution_plan(
        pipeline,
        run_config=run_config,
        mode=mode_def.name,
        instance_ref=instance.get_ref() if instance and instance.is_persistent else None,
    )

    output_capture: Dict[StepOutputHandle, Any] = {}

    with ephemeral_instance_if_missing(instance) as execute_instance:
        pipeline_run = execute_instance.create_run_for_pipeline(
            pipeline_def=pipeline_def,
            run_config=run_config,
            mode=mode_def.name,
            tags={**pipeline_def.tags, **(run_tags or {})},
        )
        run_id = pipeline_run.run_id

        execute_run_iterable = ExecuteRunWithPlanIterable(
            execution_plan=execution_plan,
            iterator=pipeline_execution_iterator,
            execution_context_manager=PlanOrchestrationContextManager(
                context_event_generator=orchestration_context_event_generator,
                pipeline=pipeline,
                execution_plan=execution_plan,
                pipeline_run=pipeline_run,
                instance=execute_instance,
                run_config=run_config,
                executor_defs=None,
                output_capture=output_capture if output_capturing_enabled else None,
                raise_on_error=raise_on_error,
            ),
        )

        event_list = []

        for event in execute_run_iterable:
            event_list.append(event)

            if event.is_pipeline_event:
                execute_instance.handle_run_event(run_id, event)

    return ExecuteInProcessResult(
        node, event_list, execute_instance.get_run_by_id(run_id), output_capture
    )
