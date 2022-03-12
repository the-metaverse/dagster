from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION, SUBSCRIPTION_QUERY
from dagster_graphql.schema import create_schema
from dagster_graphql.test.utils import execute_dagster_graphql, infer_pipeline_selector
from graphql import graphql
from graphql.execution.executors.sync import SyncExecutor

from dagster._cli.workspace import get_workspace_process_context_from_kwargs
from dagster._core.test_utils import instance_for_test
from dagster._utils import file_relative_path


def test_execute_hammer_through_dagit():
    with instance_for_test() as instance:
        with get_workspace_process_context_from_kwargs(
            instance,
            version="",
            read_only=False,
            kwargs={
                "python_file": file_relative_path(
                    __file__, "../../../dagster-test/dagster_test/toys/hammer.py"
                ),
                "attribute": "hammer_pipeline",
            },
        ) as workspace_process_context:
            context = workspace_process_context.create_request_context()
            selector = infer_pipeline_selector(context, "hammer_pipeline")
            executor = SyncExecutor()

            variables = {
                "executionParams": {
                    "runConfigData": {
                        "execution": {"dask": {"config": {"cluster": {"local": {}}}}},
                    },
                    "selector": selector,
                    "mode": "default",
                }
            }

            start_pipeline_result = graphql(
                request_string=LAUNCH_PIPELINE_EXECUTION_MUTATION,
                schema=create_schema(),
                context=context,
                variables=variables,
                executor=executor,
            )

            if start_pipeline_result.errors:
                raise Exception("{}".format(start_pipeline_result.errors))

            run_id = start_pipeline_result.data["launchPipelineExecution"]["run"]["runId"]

            context.instance.run_launcher.join(timeout=60)

            subscription = execute_dagster_graphql(
                context, SUBSCRIPTION_QUERY, variables={"runId": run_id}
            )

            subscribe_results = []
            subscription.subscribe(subscribe_results.append)

            messages = [
                x["__typename"] for x in subscribe_results[0].data["pipelineRunLogs"]["messages"]
            ]

            assert "RunStartEvent" in messages
            assert "RunSuccessEvent" in messages
