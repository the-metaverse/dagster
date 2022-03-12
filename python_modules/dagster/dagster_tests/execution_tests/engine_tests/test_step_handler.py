from dagster import pipeline
from dagster._core.definitions.reconstructable import reconstructable
from dagster._core.executor.step_delegating import StepHandlerContext
from dagster._core.test_utils import create_run_for_test, instance_for_test
from dagster._grpc.types import ExecuteStepArgs


@pipeline
def foo_pipline():
    pass


def test_step_handler_context():
    recon_pipeline = reconstructable(foo_pipline)
    with instance_for_test() as instance:
        run = create_run_for_test(instance)

        args = ExecuteStepArgs(
            pipeline_origin=recon_pipeline.get_python_origin(),
            pipeline_run_id=run.run_id,
            step_keys_to_execute=run.step_keys_to_execute,
            instance_ref=None,
        )
        ctx = StepHandlerContext(
            instance=instance,
            execute_step_args=args,
            step_tags={},
            pipeline_run=run,
        )

        assert ctx.execute_step_args == args
        assert ctx.pipeline_run == run
