from typing import Callable, Optional

from dagster._core.definitions import OpDefinition, failure_hook
from dagster._core.execution.context.hook import HookContext


def _default_summary_fn(context: HookContext) -> str:
    if isinstance(context.op, OpDefinition):
        return f"Op {context.op.name} on job {context.job_name} failed!"
    else:
        return f"Solid {context.solid.name} on pipeline {context.pipeline_name} failed!"


def _dedup_key_fn(context: HookContext) -> str:
    return f"{context.job_name}|{context.op.name}"


def _source_fn(context: HookContext):
    return f"{context.job_name}"


def pagerduty_on_failure(
    severity: str,
    summary_fn: Callable[[HookContext], str] = _default_summary_fn,
    dagit_base_url: Optional[str] = None,
):
    """Create a hook on step failure events that will trigger a PagerDuty alert.

    Args:
        severity (str): How impacted the affected system is. Displayed to users in lists and
            influences the priority of any created incidents. Must be one of {info, warning, error, critical}
        summary_fn (Optional(Callable[[HookContext], str])): Function which takes in the HookContext
            outputs a summary of the issue.
        dagit_base_url: (Optional[str]): The base url of your Dagit instance. Specify this to allow
            alerts to include deeplinks to the specific job/pipeline run that triggered the hook.

    Examples:
        .. code-block:: python
            @op
            def my_op(context):
                pass

            @job(
                resource_defs={"pagerduty": pagerduty_resource},
                hooks={pagerduty_on_failure("info", dagit_base_url="http://localhost:3000")},
            )
            def my_job():
                my_op()

        .. code-block:: python

            def my_summary_fn(context: HookContext) -> str:
                return f"Op {context.op.name} failed!"

            @op
            def my_op(context):
                pass

            @job(resource_defs={"pagerduty": pagerduty_resource})
            def my_job():
                my_op.with_hooks(hook_defs={pagerduty_on_failure(severity="critical", summary_fn=my_summary_fn)})

    """

    @failure_hook(required_resource_keys={"pagerduty"})
    def _hook(context: HookContext):
        custom_details = {}
        if dagit_base_url:
            custom_details = {
                "dagit url": "{base_url}/instance/runs/{run_id}".format(
                    base_url=dagit_base_url, run_id=context.run_id
                )
            }
        context.resources.pagerduty.EventV2_create(   # type: ignore
            summary=summary_fn(context),
            source=_source_fn(context),
            severity=severity,
            dedup_key=_dedup_key_fn(context),
            custom_details=custom_details,
        )

    return _hook
