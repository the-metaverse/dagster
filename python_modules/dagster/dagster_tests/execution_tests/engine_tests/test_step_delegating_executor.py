import subprocess
from typing import List

from dagster import executor, pipeline, reconstructable, solid
from dagster._config.field_utils import Permissive
from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
from dagster._core.definitions.mode import ModeDefinition
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.execution.api import execute_pipeline
from dagster._core.execution.retries import RetryMode
from dagster._core.executor.step_delegating import StepDelegatingExecutor, StepHandler
from dagster._core.storage.fs_io_manager import fs_io_manager
from dagster._core.test_utils import instance_for_test


class TestStepHandler(StepHandler):
    # This step handler waits for all processes to exit, because windows tests flake when processes
    # are left alive when the test ends. Non-test step handlers should not keep their own state in memory.
    processes = []  # type: ignore
    launch_step_count = 0  # type: ignore
    saw_baz_solid = False
    check_step_health_count = 0  # type: ignore
    terminate_step_count = 0  # type: ignore
    verify_step_count = 0  # type: ignore

    @property
    def name(self):
        return "TestStepHandler"

    def launch_step(self, step_handler_context):
        if step_handler_context.execute_step_args.should_verify_step:
            TestStepHandler.verify_step_count += 1
        if step_handler_context.execute_step_args.step_keys_to_execute[0] == "baz_solid":
            TestStepHandler.saw_baz_solid = True
            assert step_handler_context.step_tags["baz_solid"] == {"foo": "bar"}

        TestStepHandler.launch_step_count += 1
        print("TestStepHandler Launching Step!")  # pylint: disable=print-call
        TestStepHandler.processes.append(
            subprocess.Popen(step_handler_context.execute_step_args.get_command_args())
        )
        return []

    def check_step_health(self, step_handler_context) -> List[DagsterEvent]:
        TestStepHandler.check_step_health_count += 1
        return []

    def terminate_step(self, step_handler_context):
        TestStepHandler.terminate_step_count += 1
        raise NotImplementedError()

    @classmethod
    def reset(cls):
        cls.processes = []
        cls.launch_step_count = 0
        cls.check_step_health_count = 0
        cls.terminate_step_count = 0
        cls.verify_step_count = 0

    @classmethod
    def wait_for_processes(cls):
        for p in cls.processes:
            p.wait(timeout=5)


@executor(
    name="test_step_delegating_executor",
    requirements=multiple_process_executor_requirements(),
    config_schema=Permissive(),
)
def test_step_delegating_executor(exc_init):
    return StepDelegatingExecutor(
        TestStepHandler(),
        retries=RetryMode.DISABLED,
        sleep_seconds=exc_init.executor_config.get("sleep_seconds"),
        check_step_health_interval_seconds=exc_init.executor_config.get(
            "check_step_health_interval_seconds"
        ),
    )


@solid
def bar_solid(_):
    return "bar"


@solid(tags={"foo": "bar"})
def baz_solid(_, bar):
    return bar * 2


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[test_step_delegating_executor],
            resource_defs={"io_manager": fs_io_manager},
        )
    ]
)
def foo_pipline():
    baz_solid(bar_solid())
    bar_solid()


def test_execute():
    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(foo_pipline),
            instance=instance,
            run_config={"execution": {"test_step_delegating_executor": {"config": {}}}},
        )
        TestStepHandler.wait_for_processes()

    assert any(
        [
            "Starting execution with step handler TestStepHandler" in event
            for event in result.event_list
        ]
    )
    assert any(["STEP_START" in event for event in result.event_list])
    assert result.success
    assert TestStepHandler.saw_baz_solid
    assert TestStepHandler.verify_step_count == 0


def test_skip_execute():
    from .test_jobs import define_dynamic_skipping_job

    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(define_dynamic_skipping_job),
            instance=instance,
        )
        TestStepHandler.wait_for_processes()

    assert result.success


def test_dynamic_execute():
    from .test_jobs import define_dynamic_job

    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(define_dynamic_job),
            instance=instance,
        )
        TestStepHandler.wait_for_processes()

    assert result.success
    assert (
        len(
            [
                e
                for e in result.event_list
                if e.event_type_value == DagsterEventType.STEP_START.value
            ]
        )
        == 11
    )


def test_skipping():
    from .test_jobs import define_skpping_job

    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(define_skpping_job),
            instance=instance,
        )
        TestStepHandler.wait_for_processes()

    assert result.success


def test_execute_intervals():
    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(foo_pipline),
            instance=instance,
            run_config={
                "execution": {
                    "test_step_delegating_executor": {
                        "config": {"check_step_health_interval_seconds": 60}
                    }
                }
            },
        )
        TestStepHandler.wait_for_processes()

    assert result.success
    assert TestStepHandler.launch_step_count == 3
    assert TestStepHandler.terminate_step_count == 0
    # pipeline should complete before 60s
    assert TestStepHandler.check_step_health_count == 0

    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(foo_pipline),
            instance=instance,
            run_config={
                "execution": {
                    "test_step_delegating_executor": {
                        "config": {"check_step_health_interval_seconds": 0}
                    }
                }
            },
        )
        TestStepHandler.wait_for_processes()

    assert result.success
    assert TestStepHandler.launch_step_count == 3
    assert TestStepHandler.terminate_step_count == 0
    # every step should get checked at least once
    assert TestStepHandler.check_step_health_count >= 3


@executor(
    name="test_step_delegating_executor_verify_step",
    requirements=multiple_process_executor_requirements(),
    config_schema=Permissive(),
)
def test_step_delegating_executor_verify_step(exc_init):
    return StepDelegatingExecutor(
        TestStepHandler(),
        retries=RetryMode.DISABLED,
        sleep_seconds=exc_init.executor_config.get("sleep_seconds"),
        check_step_health_interval_seconds=exc_init.executor_config.get(
            "check_step_health_interval_seconds"
        ),
        should_verify_step=True,
    )


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[test_step_delegating_executor_verify_step],
            resource_defs={"io_manager": fs_io_manager},
        )
    ]
)
def foo_pipline_verify_step():
    baz_solid(bar_solid())
    bar_solid()


def test_execute_verify_step():
    TestStepHandler.reset()
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(foo_pipline_verify_step),
            instance=instance,
            run_config={"execution": {"test_step_delegating_executor_verify_step": {"config": {}}}},
        )
        TestStepHandler.wait_for_processes()

    assert any(
        [
            "Starting execution with step handler TestStepHandler" in event
            for event in result.event_list
        ]
    )
    assert result.success
    assert TestStepHandler.verify_step_count == 3
