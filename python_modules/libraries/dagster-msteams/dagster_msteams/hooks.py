from typing import Callable, Optional

from dagster_msteams.card import Card

from dagster._core.definitions import failure_hook, success_hook
from dagster._core.execution.context.hook import HookContext


def _default_status_message(context: HookContext, status: str) -> str:
    return "Solid {solid_name} on pipeline {pipeline_name} {status}!\nRun ID: {run_id}".format(
        solid_name=context.solid.name,
        pipeline_name=context.pipeline_name,
        run_id=context.run_id,
        status=status,
    )


def _default_failure_message(context: HookContext) -> str:
    return _default_status_message(context, status="failed")


def _default_success_message(context: HookContext) -> str:
    return _default_status_message(context, status="succeeded")


def teams_on_failure(
    message_fn: Callable[[HookContext], str] = _default_failure_message,
    dagit_base_url: Optional[str] = None,
):
    """Create a hook on step failure events that will message the given MS Teams webhook URL.

    Args:
        message_fn (Optional(Callable[[HookContext], str])): Function which takes in the
            HookContext outputs the message you want to send.
        dagit_base_url: (Optional[str]): The base url of your Dagit instance. Specify this
            to allow messages to include deeplinks to the specific pipeline run that triggered
            the hook.

    Examples:
        .. code-block:: python

            @teams_on_failure(dagit_base_url="http://localhost:3000")
            @pipeline(...)
            def my_pipeline():
                pass

        .. code-block:: python

            def my_message_fn(context: HookContext) -> str:
                return "Solid {solid_name} failed!".format(
                    solid_name=context.solid
                )

            @solid
            def a_solid(context):
                pass

            @pipeline(...)
            def my_pipeline():
                a_solid.with_hooks(hook_defs={teams_on_failure("#foo", my_message_fn)})

    """

    @failure_hook(required_resource_keys={"msteams"})
    def _hook(context: HookContext):
        text = message_fn(context)
        if dagit_base_url:
            text += "<a href='{base_url}/instance/runs/{run_id}'>View in Dagit</a>".format(
                base_url=dagit_base_url,
                run_id=context.run_id,
            )
        card = Card()
        card.add_attachment(text_message=text)
        context.resources.msteams.post_message(payload=card.payload)

    return _hook


def teams_on_success(
    message_fn: Callable[[HookContext], str] = _default_success_message,
    dagit_base_url: Optional[str] = None,
):
    """Create a hook on step success events that will message the given MS Teams webhook URL.

    Args:
        message_fn (Optional(Callable[[HookContext], str])): Function which takes in the
            HookContext outputs the message you want to send.
        dagit_base_url: (Optional[str]): The base url of your Dagit instance. Specify this
            to allow messages to include deeplinks to the specific pipeline run that triggered
            the hook.

    Examples:
        .. code-block:: python

            @teams_on_success(dagit_base_url="http://localhost:3000")
            @pipeline(...)
            def my_pipeline():
                pass

        .. code-block:: python

            def my_message_fn(context: HookContext) -> str:
                return "Solid {solid_name} failed!".format(
                    solid_name=context.solid
                )

            @solid
            def a_solid(context):
                pass

            @pipeline(...)
            def my_pipeline():
                a_solid.with_hooks(hook_defs={teams_on_success("#foo", my_message_fn)})

    """

    @success_hook(required_resource_keys={"msteams"})
    def _hook(context: HookContext):
        text = message_fn(context)
        if dagit_base_url:
            text += "<a href='{base_url}/instance/runs/{run_id}'>View in Dagit</a>".format(
                base_url=dagit_base_url,
                run_id=context.run_id,
            )
        card = Card()
        card.add_attachment(text_message=text)
        context.resources.msteams.post_message(payload=card.payload)

    return _hook
