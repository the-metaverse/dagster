import os
import random
import string
import tempfile
import time
from contextlib import contextmanager

import pendulum
import pytest

from dagster import (
    Any,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    Field,
    Output,
    graph,
    pipeline,
    pipeline_failure_sensor,
    repository,
    run_failure_sensor,
    solid,
)
from dagster._core.definitions.decorators.sensor import asset_sensor, sensor
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.definitions.run_status_sensor_definition import run_status_sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus, RunRequest, SkipReason
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.events.log import EventLogEntry
from dagster._core.execution.api import execute_pipeline
from dagster._core.host_representation import ExternalInstigatorOrigin, ExternalRepositoryOrigin
from dagster._core.instance import DagsterInstance
from dagster._core.scheduler.instigation import InstigatorState, InstigatorStatus, TickStatus
from dagster._core.storage.event_log.base import EventRecordsFilter
from dagster._core.storage.pipeline_run import PipelineRunStatus
from dagster._core.test_utils import (
    create_test_daemon_workspace,
    get_logger_output_from_capfd,
    instance_for_test,
)
from dagster._core.workspace.load_target import PythonFileTarget
from dagster.daemon import get_default_daemon_logger
from dagster.daemon.sensor import execute_sensor_iteration, execute_sensor_iteration_loop
from dagster.seven.compat.pendulum import create_pendulum_time, to_timezone


@solid
def the_solid(_):
    return 1


@pipeline
def the_pipeline():
    the_solid()


@graph()
def the_graph():
    the_solid()


the_job = the_graph.to_job()


@solid(config_schema=Field(Any))
def config_solid(_):
    return 1


@pipeline
def config_pipeline():
    config_solid()


@graph()
def config_graph():
    config_solid()


@solid
def foo_solid():
    yield AssetMaterialization(asset_key=AssetKey("foo"))
    yield Output(1)


@pipeline
def foo_pipeline():
    foo_solid()


@solid
def foo_observation_solid():
    yield AssetObservation(asset_key=AssetKey("foo"), metadata={"text": "FOO"})
    yield Output(5)


@pipeline
def foo_observation_pipeline():
    foo_observation_solid()


@solid
def hanging_solid():
    start_time = time.time()
    while True:
        if time.time() - start_time > 10:
            return
        time.sleep(0.5)


@pipeline
def hanging_pipeline():
    hanging_solid()


@solid
def failure_solid():
    raise Exception("womp womp")


@pipeline
def failure_pipeline():
    failure_solid()


@graph()
def failure_graph():
    failure_solid()


failure_job = failure_graph.to_job()


@sensor(pipeline_name="the_pipeline")
def simple_sensor(context):
    if not context.last_completion_time or not int(context.last_completion_time) % 2:
        return SkipReason()

    return RunRequest(run_key=None, run_config={}, tags={})


@sensor(pipeline_name="the_pipeline")
def always_on_sensor(_context):
    return RunRequest(run_key=None, run_config={}, tags={})


@sensor(pipeline_name="the_pipeline")
def run_key_sensor(_context):
    return RunRequest(run_key="only_once", run_config={}, tags={})


@sensor(pipeline_name="the_pipeline")
def error_sensor(context):
    context.update_cursor("the exception below should keep this from being persisted")
    raise Exception("womp womp")


@sensor(pipeline_name="the_pipeline")
def wrong_config_sensor(_context):
    return RunRequest(run_key="bad_config_key", run_config={"bad_key": "bad_val"}, tags={})


@sensor(pipeline_name="the_pipeline", minimum_interval_seconds=60)
def custom_interval_sensor(_context):
    return SkipReason()


@sensor(pipeline_name="the_pipeline")
def skip_cursor_sensor(context):
    if not context.cursor:
        cursor = 1
    else:
        cursor = int(context.cursor) + 1

    context.update_cursor(str(cursor))
    return SkipReason()


@sensor(pipeline_name="the_pipeline")
def run_cursor_sensor(context):
    if not context.cursor:
        cursor = 1
    else:
        cursor = int(context.cursor) + 1

    context.update_cursor(str(cursor))
    return RunRequest(run_key=None, run_config={}, tags={})


def _random_string(length):
    return "".join(random.choice(string.ascii_lowercase) for x in range(length))


@sensor(pipeline_name="config_pipeline")
def large_sensor(_context):
    # create a gRPC response payload larger than the limit (4194304)
    REQUEST_COUNT = 25
    REQUEST_TAG_COUNT = 5000
    REQUEST_CONFIG_COUNT = 100

    for _ in range(REQUEST_COUNT):
        tags_garbage = {_random_string(10): _random_string(20) for i in range(REQUEST_TAG_COUNT)}
        config_garbage = {
            _random_string(10): _random_string(20) for i in range(REQUEST_CONFIG_COUNT)
        }
        config = {"solids": {"config_solid": {"config": {"foo": config_garbage}}}}
        yield RunRequest(run_key=None, run_config=config, tags=tags_garbage)


@asset_sensor(pipeline_name="the_pipeline", asset_key=AssetKey("foo"))
def asset_foo_sensor(context, _event):
    return RunRequest(run_key=context.cursor, run_config={})


@asset_sensor(asset_key=AssetKey("foo"), job=the_job)
def asset_job_sensor(context, _event):
    return RunRequest(run_key=context.cursor, run_config={})


@pipeline_failure_sensor
def my_pipeline_failure_sensor(context):
    assert isinstance(context.instance, DagsterInstance)


@run_failure_sensor(job_selection=[failure_job])
def my_run_failure_sensor_filtered(context):
    assert isinstance(context.instance, DagsterInstance)


@run_failure_sensor()
def my_run_failure_sensor_that_itself_fails(context):
    raise Exception("How meta")


@run_status_sensor(pipeline_run_status=PipelineRunStatus.SUCCESS)
def my_pipeline_success_sensor(context):
    assert isinstance(context.instance, DagsterInstance)


@run_status_sensor(pipeline_run_status=PipelineRunStatus.STARTED)
def my_pipeline_started_sensor(context):
    assert isinstance(context.instance, DagsterInstance)


config_job = config_graph.to_job()


@sensor(jobs=[the_job, config_job])
def two_job_sensor(context):
    counter = int(context.cursor) if context.cursor else 0
    if counter % 2 == 0:
        yield RunRequest(run_key=str(counter), job_name=the_job.name)
    else:
        yield RunRequest(
            run_key=str(counter),
            job_name=config_job.name,
            run_config={"solids": {"config_solid": {"config": {"foo": "blah"}}}},
        )
    context.update_cursor(str(counter + 1))


@sensor()
def bad_request_untargeted(_ctx):
    yield RunRequest(run_key=None, job_name="should_fail")


@sensor(job=the_job)
def bad_request_mismatch(_ctx):
    yield RunRequest(run_key=None, job_name="config_pipeline")


@sensor(jobs=[the_job, config_job])
def bad_request_unspecified(_ctx):
    yield RunRequest(run_key=None)


@repository
def the_repo():
    return [
        the_pipeline,
        the_job,
        config_pipeline,
        config_graph,
        foo_pipeline,
        large_sensor,
        simple_sensor,
        error_sensor,
        wrong_config_sensor,
        always_on_sensor,
        run_key_sensor,
        custom_interval_sensor,
        skip_cursor_sensor,
        run_cursor_sensor,
        asset_foo_sensor,
        asset_job_sensor,
        my_pipeline_failure_sensor,
        my_run_failure_sensor_filtered,
        my_run_failure_sensor_that_itself_fails,
        my_pipeline_success_sensor,
        my_pipeline_started_sensor,
        failure_pipeline,
        failure_job,
        hanging_pipeline,
        two_job_sensor,
        bad_request_untargeted,
        bad_request_mismatch,
        bad_request_unspecified,
    ]


@pipeline
def the_other_pipeline():
    the_solid()


@repository
def the_other_repo():
    return [
        the_other_pipeline,
    ]


@sensor(pipeline_name="the_pipeline", default_status=DefaultSensorStatus.RUNNING)
def always_running_sensor(context):
    if not context.last_completion_time or not int(context.last_completion_time) % 2:
        return SkipReason()

    return RunRequest(run_key=None, run_config={}, tags={})


@sensor(pipeline_name="the_pipeline", default_status=DefaultSensorStatus.STOPPED)
def never_running_sensor(context):
    if not context.last_completion_time or not int(context.last_completion_time) % 2:
        return SkipReason()

    return RunRequest(run_key=None, run_config={}, tags={})


@repository
def the_status_in_code_repo():
    return [
        the_pipeline,
        always_running_sensor,
        never_running_sensor,
    ]


@contextmanager
def instance_with_sensors(overrides=None, attribute="the_repo"):
    with instance_for_test(overrides) as instance:
        with create_test_daemon_workspace(workspace_load_target(attribute)) as workspace:
            yield (
                instance,
                workspace,
                next(
                    iter(workspace.get_workspace_snapshot().values())
                ).repository_location.get_repository(attribute),
            )


def workspace_load_target(attribute="the_repo"):
    return PythonFileTarget(
        python_file=__file__,
        attribute=attribute,
        working_directory=os.path.dirname(__file__),
        location_name="test_location",
    )


def evaluate_sensors(instance, workspace):
    list(
        execute_sensor_iteration(
            instance,
            get_default_daemon_logger("SensorDaemon"),
            workspace,
        )
    )


def validate_tick(
    tick,
    external_sensor,
    expected_datetime,
    expected_status,
    expected_run_ids=None,
    expected_error=None,
):
    tick_data = tick.tick_data
    assert tick_data.instigator_origin_id == external_sensor.get_external_origin_id()
    assert tick_data.instigator_name == external_sensor.name
    assert tick_data.instigator_type == InstigatorType.SENSOR
    assert tick_data.status == expected_status
    assert tick_data.timestamp == expected_datetime.timestamp()
    if expected_run_ids is not None:
        assert set(tick_data.run_ids) == set(expected_run_ids)
    if expected_error:
        assert expected_error in str(tick_data.error)


def validate_run_started(run, expected_success=True):
    if expected_success:
        assert (
            run.status == PipelineRunStatus.STARTED
            or run.status == PipelineRunStatus.SUCCESS
            or run.status == PipelineRunStatus.STARTING
        )
    else:
        assert run.status == PipelineRunStatus.FAILURE


def wait_for_all_runs_to_start(instance, timeout=10):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for runs to start")
        time.sleep(0.5)

        not_started_runs = [
            run for run in instance.get_runs() if run.status == PipelineRunStatus.NOT_STARTED
        ]

        if len(not_started_runs) == 0:
            break


def wait_for_all_runs_to_finish(instance, timeout=10):
    start_time = time.time()
    FINISHED_STATES = [
        PipelineRunStatus.SUCCESS,
        PipelineRunStatus.FAILURE,
        PipelineRunStatus.CANCELED,
    ]
    while True:
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for runs to start")
        time.sleep(0.5)

        not_finished_runs = [
            run for run in instance.get_runs() if run.status not in FINISHED_STATES
        ]

        if len(not_finished_runs) == 0:
            break


def test_simple_sensor(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("simple_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            assert (
                get_logger_output_from_capfd(capfd, "dagster.daemon.SensorDaemon")
                == """2019-02-27 17:59:59 -0600 - dagster.daemon.SensorDaemon - INFO - Checking for new runs for sensor: simple_sensor
2019-02-27 17:59:59 -0600 - dagster.daemon.SensorDaemon - INFO - No run requests returned for simple_sensor, skipping"""
            )

            freeze_datetime = freeze_datetime.add(seconds=30)

        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            wait_for_all_runs_to_start(instance)
            assert instance.get_runs_count() == 1
            run = instance.get_runs()[0]
            validate_run_started(run)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 2

            expected_datetime = create_pendulum_time(
                year=2019, month=2, day=28, hour=0, minute=0, second=29
            )
            validate_tick(
                ticks[0],
                external_sensor,
                expected_datetime,
                TickStatus.SUCCESS,
                [run.run_id],
            )

            assert (
                get_logger_output_from_capfd(capfd, "dagster.daemon.SensorDaemon")
                == """2019-02-27 18:00:29 -0600 - dagster.daemon.SensorDaemon - INFO - Checking for new runs for sensor: simple_sensor
2019-02-27 18:00:29 -0600 - dagster.daemon.SensorDaemon - INFO - Launching run for simple_sensor
2019-02-27 18:00:29 -0600 - dagster.daemon.SensorDaemon - INFO - Completed launch of run {run_id} for simple_sensor""".format(
                    run_id=run.run_id
                )
            )


def test_bad_load_sensor_repository(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("simple_sensor")

            valid_origin = external_sensor.get_external_origin()

            # Swap out a new repository name
            invalid_repo_origin = ExternalInstigatorOrigin(
                ExternalRepositoryOrigin(
                    valid_origin.external_repository_origin.repository_location_origin,
                    "invalid_repo_name",
                ),
                valid_origin.instigator_name,
            )

            instance.add_instigator_state(
                InstigatorState(
                    invalid_repo_origin, InstigatorType.SENSOR, InstigatorStatus.RUNNING
                )
            )

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(invalid_repo_origin.get_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(invalid_repo_origin.get_id())
            assert len(ticks) == 0

            captured = capfd.readouterr()
            assert (
                "Could not find repository invalid_repo_name in location test_location to run sensor simple_sensor"
                in captured.out
            )


def test_bad_load_sensor(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("simple_sensor")

            valid_origin = external_sensor.get_external_origin()

            # Swap out a new repository name
            invalid_repo_origin = ExternalInstigatorOrigin(
                valid_origin.external_repository_origin,
                "invalid_sensor",
            )

            instance.add_instigator_state(
                InstigatorState(
                    invalid_repo_origin, InstigatorType.SENSOR, InstigatorStatus.RUNNING
                )
            )

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(invalid_repo_origin.get_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(invalid_repo_origin.get_id())
            assert len(ticks) == 0

            captured = capfd.readouterr()
            assert "Could not find sensor invalid_sensor in repository the_repo." in captured.out


def test_error_sensor(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("error_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )

            state = instance.get_instigator_state(external_sensor.get_external_origin_id())
            assert state.instigator_data is None

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                [],
                "Error occurred during the execution of evaluation_fn for sensor error_sensor",
            )

            captured = capfd.readouterr()
            assert (
                "Error occurred during the execution of evaluation_fn for sensor error_sensor"
            ) in captured.out

            # Tick updated the sensor's last tick time, but not its cursor (due to the failure)
            state = instance.get_instigator_state(external_sensor.get_external_origin_id())
            assert state.instigator_data.cursor is None
            assert state.instigator_data.last_tick_timestamp == freeze_datetime.timestamp()


def test_wrong_config_sensor(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(
            year=2019,
            month=2,
            day=27,
            hour=23,
            minute=59,
            second=59,
        ),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("wrong_config_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1

            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                [],
                "Error in config for pipeline",
            )

            captured = capfd.readouterr()
            assert ("Error in config for pipeline") in captured.out

        freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):
            # Error repeats on subsequent ticks

            evaluate_sensors(instance, workspace)
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 2

            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                [],
                "Error in config for pipeline",
            )

            captured = capfd.readouterr()
            assert ("Error in config for pipeline") in captured.out


def test_launch_failure(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors(
        overrides={
            "run_launcher": {
                "module": "dagster._core.test_utils",
                "class": "ExplodingRunLauncher",
            },
        },
    ) as (instance, workspace, external_repo):
        with pendulum.test(freeze_datetime):

            external_sensor = external_repo.get_external_sensor("always_on_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 1
            run = instance.get_runs()[0]
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
                [run.run_id],
            )

            captured = capfd.readouterr()
            assert (
                "Run {run_id} created successfully but failed to launch:".format(run_id=run.run_id)
            ) in captured.out

            assert "The entire purpose of this is to throw on launch" in captured.out


def test_launch_once(capfd):
    freeze_datetime = to_timezone(
        create_pendulum_time(
            year=2019,
            month=2,
            day=27,
            hour=23,
            minute=59,
            second=59,
            tz="UTC",
        ),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):

            external_sensor = external_repo.get_external_sensor("run_key_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )
            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)
            wait_for_all_runs_to_start(instance)

            assert instance.get_runs_count() == 1
            run = instance.get_runs()[0]
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
                expected_run_ids=[run.run_id],
            )

        # run again (after 30 seconds), to ensure that the run key maintains idempotence
        freeze_datetime = freeze_datetime.add(seconds=30)
        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            assert instance.get_runs_count() == 1
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )
            captured = capfd.readouterr()
            assert (
                'Skipping 1 run for sensor run_key_sensor already completed with run keys: ["only_once"]'
                in captured.out
            )

            launched_run = instance.get_runs()[0]

            # Manually create a new run with the same tags
            execute_pipeline(
                the_pipeline,
                run_config=launched_run.run_config,
                tags=launched_run.tags,
                instance=instance,
            )

            # Sensor loop still executes
        freeze_datetime = freeze_datetime.add(seconds=30)
        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())

            assert len(ticks) == 3
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )


def test_custom_interval_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=28, tz="UTC"), "US/Central"
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("custom_interval_sensor")
            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(ticks[0], external_sensor, freeze_datetime, TickStatus.SKIPPED)

            freeze_datetime = freeze_datetime.add(seconds=30)

        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            # no additional tick created after 30 seconds
            assert len(ticks) == 1

            freeze_datetime = freeze_datetime.add(seconds=30)

        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 2

            expected_datetime = create_pendulum_time(year=2019, month=2, day=28, hour=0, minute=1)
            validate_tick(ticks[0], external_sensor, expected_datetime, TickStatus.SKIPPED)


def test_custom_interval_sensor_with_offset(monkeypatch):
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=28, tz="UTC"), "US/Central"
    )

    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        pendulum.set_test_now(pendulum.now().add(seconds=s))

    monkeypatch.setattr(time, "sleep", fake_sleep)

    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):

            # 60 second custom interval
            external_sensor = external_repo.get_external_sensor("custom_interval_sensor")

            instance.add_instigator_state(
                InstigatorState(
                    external_sensor.get_external_origin(),
                    InstigatorType.SENSOR,
                    InstigatorStatus.RUNNING,
                )
            )

            # create a tick
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1

            # calling for another iteration should not generate another tick because time has not
            # advanced
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1

            # call the sensor_iteration_loop, which should loop, and call the monkeypatched sleep
            # to advance 30 seconds
            list(
                execute_sensor_iteration_loop(
                    instance,
                    workspace,
                    get_default_daemon_logger("dagster.daemon.SensorDaemon"),
                    until=freeze_datetime.add(seconds=65).timestamp(),
                )
            )

            assert pendulum.now() == freeze_datetime.add(seconds=65)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 2
            assert sum(sleeps) == 65


def _get_unloadable_sensor_origin():
    load_target = workspace_load_target()
    return ExternalRepositoryOrigin(
        load_target.create_origins()[0], "fake_repository"
    ).get_instigator_origin("doesnt_exist")


def test_sensor_start_stop():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("always_on_sensor")
            external_origin_id = external_sensor.get_external_origin_id()
            instance.start_sensor(external_sensor)

            assert instance.get_runs_count() == 0
            ticks = instance.get_ticks(external_origin_id)
            assert len(ticks) == 0

            evaluate_sensors(instance, workspace)

            assert instance.get_runs_count() == 1
            run = instance.get_runs()[0]
            ticks = instance.get_ticks(external_origin_id)
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
                [run.run_id],
            )

            freeze_datetime = freeze_datetime.add(seconds=15)

        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            # no new ticks, no new runs, we are below the 30 second min interval
            assert instance.get_runs_count() == 1
            ticks = instance.get_ticks(external_origin_id)
            assert len(ticks) == 1

            # stop / start
            instance.stop_sensor(external_origin_id, external_sensor)
            instance.start_sensor(external_sensor)

            evaluate_sensors(instance, workspace)
            # no new ticks, no new runs, we are below the 30 second min interval
            assert instance.get_runs_count() == 1
            ticks = instance.get_ticks(external_origin_id)
            assert len(ticks) == 1

            freeze_datetime = freeze_datetime.add(seconds=16)

        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)
            # should have new tick, new run, we are after the 30 second min interval
            assert instance.get_runs_count() == 2
            ticks = instance.get_ticks(external_origin_id)
            assert len(ticks) == 2


def test_large_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            external_sensor = external_repo.get_external_sensor("large_sensor")
            instance.start_sensor(external_sensor)
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(external_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                external_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )


def test_cursor_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            skip_sensor = external_repo.get_external_sensor("skip_cursor_sensor")
            run_sensor = external_repo.get_external_sensor("run_cursor_sensor")
            instance.start_sensor(skip_sensor)
            instance.start_sensor(run_sensor)
            evaluate_sensors(instance, workspace)

            skip_ticks = instance.get_ticks(skip_sensor.get_external_origin_id())
            assert len(skip_ticks) == 1
            validate_tick(
                skip_ticks[0],
                skip_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )
            assert skip_ticks[0].cursor == "1"

            run_ticks = instance.get_ticks(run_sensor.get_external_origin_id())
            assert len(run_ticks) == 1
            validate_tick(
                run_ticks[0],
                run_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            assert run_ticks[0].cursor == "1"

        freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):
            evaluate_sensors(instance, workspace)

            skip_ticks = instance.get_ticks(skip_sensor.get_external_origin_id())
            assert len(skip_ticks) == 2
            validate_tick(
                skip_ticks[0],
                skip_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )
            assert skip_ticks[0].cursor == "2"

            run_ticks = instance.get_ticks(run_sensor.get_external_origin_id())
            assert len(run_ticks) == 2
            validate_tick(
                run_ticks[0],
                run_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            assert run_ticks[0].cursor == "2"


def test_asset_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            foo_sensor = external_repo.get_external_sensor("asset_foo_sensor")
            instance.start_sensor(foo_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(foo_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                foo_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):

            # should generate the foo asset
            execute_pipeline(foo_pipeline, instance=instance)

            # should fire the asset sensor
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(foo_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                foo_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            run = instance.get_runs()[0]
            assert run.run_config == {}
            assert run.tags
            assert run.tags.get("dagster/sensor_name") == "asset_foo_sensor"


def test_asset_job_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            job_sensor = external_repo.get_external_sensor("asset_job_sensor")
            instance.start_sensor(job_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):

            # should generate the foo asset
            execute_pipeline(foo_pipeline, instance=instance)

            # should fire the asset sensor
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            run = instance.get_runs()[0]
            assert run.run_config == {}
            assert run.tags
            assert run.tags.get("dagster/sensor_name") == "asset_job_sensor"


def test_asset_sensor_not_triggered_on_observation():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            foo_sensor = external_repo.get_external_sensor("asset_foo_sensor")
            instance.start_sensor(foo_sensor)

            # generates the foo asset observation
            execute_pipeline(foo_observation_pipeline, instance=instance)

            # observation should not fire the asset sensor
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(foo_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                foo_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):

            # should generate the foo asset
            execute_pipeline(foo_pipeline, instance=instance)

            # materialization should fire the asset sensor
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(foo_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                foo_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            run = instance.get_runs()[0]
            assert run.run_config == {}
            assert run.tags
            assert run.tags.get("dagster/sensor_name") == "asset_foo_sensor"


def test_pipeline_failure_sensor():
    freeze_datetime = pendulum.now()
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            failure_sensor = external_repo.get_external_sensor("my_pipeline_failure_sensor")
            instance.start_sensor(failure_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
            time.sleep(1)

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("failure_pipeline")
            run = instance.create_run_for_pipeline(
                failure_pipeline,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.FAILURE
            freeze_datetime = freeze_datetime.add(seconds=60)

        with pendulum.test(freeze_datetime):

            # should fire the failure sensor
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )


def test_run_failure_sensor_that_fails():
    freeze_datetime = pendulum.now()
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            failure_sensor = external_repo.get_external_sensor(
                "my_run_failure_sensor_that_itself_fails"
            )
            instance.start_sensor(failure_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
            time.sleep(1)

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("failure_pipeline")
            run = instance.create_run_for_pipeline(
                failure_pipeline,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.FAILURE
            freeze_datetime = freeze_datetime.add(seconds=60)

        with pendulum.test(freeze_datetime):

            # should fire the failure sensor and fail
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                expected_error="How meta",
            )

        # Next tick skips again
        freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):
            # should fire the failure sensor and fail
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 3
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )


def test_run_failure_sensor_filtered():
    freeze_datetime = pendulum.now()
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            failure_sensor = external_repo.get_external_sensor("my_run_failure_sensor_filtered")
            instance.start_sensor(failure_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
            time.sleep(1)

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("failure_pipeline")
            run = instance.create_run_for_pipeline(
                failure_pipeline,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.FAILURE
            freeze_datetime = freeze_datetime.add(seconds=60)

        with pendulum.test(freeze_datetime):

            # should not fire the failure sensor (filtered to failure job)
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
            time.sleep(1)

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("failure_graph")
            run = instance.create_run_for_pipeline(
                failure_job,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.FAILURE

            freeze_datetime = freeze_datetime.add(seconds=60)

        with pendulum.test(freeze_datetime):

            # should not fire the failure sensor (filtered to failure job)
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
            assert len(ticks) == 3
            validate_tick(
                ticks[0],
                failure_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )


def test_run_status_sensor(capfd):
    freeze_datetime = pendulum.now()
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            success_sensor = external_repo.get_external_sensor("my_pipeline_success_sensor")
            instance.start_sensor(success_sensor)

            started_sensor = external_repo.get_external_sensor("my_pipeline_started_sensor")
            instance.start_sensor(started_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(success_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                success_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            freeze_datetime = freeze_datetime.add(seconds=60)
            time.sleep(1)

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("failure_pipeline")
            run = instance.create_run_for_pipeline(
                failure_pipeline,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.FAILURE
            freeze_datetime = freeze_datetime.add(seconds=60)

        with pendulum.test(freeze_datetime):

            # should not fire the success sensor, should fire the started sensro
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(success_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                success_sensor,
                freeze_datetime,
                TickStatus.SKIPPED,
            )

            ticks = instance.get_ticks(started_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                started_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )

        with pendulum.test(freeze_datetime):
            external_pipeline = external_repo.get_full_external_pipeline("foo_pipeline")
            run = instance.create_run_for_pipeline(
                foo_pipeline,
                external_pipeline_origin=external_pipeline.get_external_origin(),
                pipeline_code_origin=external_pipeline.get_python_origin(),
            )
            instance.submit_run(run.run_id, workspace)
            wait_for_all_runs_to_finish(instance)
            run = instance.get_runs()[0]
            assert run.status == PipelineRunStatus.SUCCESS
            freeze_datetime = freeze_datetime.add(seconds=60)

        capfd.readouterr()

        with pendulum.test(freeze_datetime):

            # should fire the success sensor and the started sensor
            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(success_sensor.get_external_origin_id())
            assert len(ticks) == 3
            validate_tick(
                ticks[0],
                success_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )

            ticks = instance.get_ticks(started_sensor.get_external_origin_id())
            assert len(ticks) == 3
            validate_tick(
                ticks[0],
                started_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )

            captured = capfd.readouterr()
            assert (
                'Sensor "my_pipeline_started_sensor" acted on run status STARTED of run'
                in captured.out
            )
            assert (
                'Sensor "my_pipeline_success_sensor" acted on run status SUCCESS of run'
                in captured.out
            )


def sqlite_storage_config_fn(temp_dir):
    # non-run sharded storage
    return {
        "run_storage": {
            "module": "dagster._core.storage.runs",
            "class": "SqliteRunStorage",
            "config": {"base_dir": temp_dir},
        },
        "event_log_storage": {
            "module": "dagster._core.storage.event_log",
            "class": "SqliteEventLogStorage",
            "config": {"base_dir": temp_dir},
        },
    }


def default_storage_config_fn(_):
    # run sharded storage
    return {}


def sql_event_log_storage_config_fn(temp_dir):
    return {
        "event_log_storage": {
            "module": "dagster._core.storage.event_log",
            "class": "ConsolidatedSqliteEventLogStorage",
            "config": {"base_dir": temp_dir},
        },
    }


@pytest.mark.parametrize(
    "storage_config_fn",
    [default_storage_config_fn, sqlite_storage_config_fn],
)
def test_run_status_sensor_interleave(storage_config_fn):
    freeze_datetime = pendulum.now()
    with tempfile.TemporaryDirectory() as temp_dir:

        with instance_with_sensors(overrides=storage_config_fn(temp_dir)) as (
            instance,
            workspace,
            external_repo,
        ):
            # start sensor
            with pendulum.test(freeze_datetime):
                failure_sensor = external_repo.get_external_sensor("my_pipeline_failure_sensor")
                instance.start_sensor(failure_sensor)

                evaluate_sensors(instance, workspace)

                ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
                assert len(ticks) == 1
                validate_tick(
                    ticks[0],
                    failure_sensor,
                    freeze_datetime,
                    TickStatus.SKIPPED,
                )

                freeze_datetime = freeze_datetime.add(seconds=60)
                time.sleep(1)

            with pendulum.test(freeze_datetime):
                external_pipeline = external_repo.get_full_external_pipeline("hanging_pipeline")
                # start run 1
                run1 = instance.create_run_for_pipeline(
                    hanging_pipeline,
                    external_pipeline_origin=external_pipeline.get_external_origin(),
                    pipeline_code_origin=external_pipeline.get_python_origin(),
                )
                instance.submit_run(run1.run_id, workspace)
                freeze_datetime = freeze_datetime.add(seconds=60)
                # start run 2
                run2 = instance.create_run_for_pipeline(
                    hanging_pipeline,
                    external_pipeline_origin=external_pipeline.get_external_origin(),
                    pipeline_code_origin=external_pipeline.get_python_origin(),
                )
                instance.submit_run(run2.run_id, workspace)
                freeze_datetime = freeze_datetime.add(seconds=60)
                # fail run 2
                instance.report_run_failed(run2)
                freeze_datetime = freeze_datetime.add(seconds=60)
                run = instance.get_runs()[0]
                assert run.status == PipelineRunStatus.FAILURE
                assert run.run_id == run2.run_id

            # check sensor
            with pendulum.test(freeze_datetime):

                # should fire for run 2
                evaluate_sensors(instance, workspace)

                ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
                assert len(ticks) == 2
                validate_tick(
                    ticks[0],
                    failure_sensor,
                    freeze_datetime,
                    TickStatus.SUCCESS,
                )
                assert len(ticks[0].origin_run_ids) == 1
                assert ticks[0].origin_run_ids[0] == run2.run_id

            # fail run 1
            with pendulum.test(freeze_datetime):
                # fail run 2
                instance.report_run_failed(run1)
                freeze_datetime = freeze_datetime.add(seconds=60)
                time.sleep(1)

            # check sensor
            with pendulum.test(freeze_datetime):

                # should fire for run 1
                evaluate_sensors(instance, workspace)

                ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
                assert len(ticks) == 3
                validate_tick(
                    ticks[0],
                    failure_sensor,
                    freeze_datetime,
                    TickStatus.SUCCESS,
                )
                assert len(ticks[0].origin_run_ids) == 1
                assert ticks[0].origin_run_ids[0] == run1.run_id


@pytest.mark.parametrize("storage_config_fn", [sql_event_log_storage_config_fn])
def test_pipeline_failure_sensor_empty_run_records(storage_config_fn):
    freeze_datetime = pendulum.now()
    with tempfile.TemporaryDirectory() as temp_dir:

        with instance_with_sensors(overrides=storage_config_fn(temp_dir)) as (
            instance,
            workspace,
            external_repo,
        ):

            with pendulum.test(freeze_datetime):
                failure_sensor = external_repo.get_external_sensor("my_pipeline_failure_sensor")
                instance.start_sensor(failure_sensor)

                evaluate_sensors(instance, workspace)

                ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
                assert len(ticks) == 1
                validate_tick(
                    ticks[0],
                    failure_sensor,
                    freeze_datetime,
                    TickStatus.SKIPPED,
                )

                freeze_datetime = freeze_datetime.add(seconds=60)
                time.sleep(1)

            with pendulum.test(freeze_datetime):
                # create a mismatch between event storage and run storage
                instance.event_log_storage.store_event(
                    EventLogEntry(
                        error_info=None,
                        level="debug",
                        user_message="",
                        run_id="fake_run_id",
                        timestamp=time.time(),
                        dagster_event=DagsterEvent(
                            DagsterEventType.PIPELINE_FAILURE.value,
                            "foo",
                        ),
                    )
                )
                runs = instance.get_runs()
                assert len(runs) == 0
                failure_events = instance.get_event_records(
                    EventRecordsFilter(event_type=DagsterEventType.PIPELINE_FAILURE)
                )
                assert len(failure_events) == 1
                freeze_datetime = freeze_datetime.add(seconds=60)

            with pendulum.test(freeze_datetime):
                # shouldn't fire the failure sensor due to the mismatch
                evaluate_sensors(instance, workspace)

                ticks = instance.get_ticks(failure_sensor.get_external_origin_id())
                assert len(ticks) == 2
                validate_tick(
                    ticks[0],
                    failure_sensor,
                    freeze_datetime,
                    TickStatus.SKIPPED,
                )


def test_multi_job_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            job_sensor = external_repo.get_external_sensor("two_job_sensor")
            instance.start_sensor(job_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )

            run = instance.get_runs()[0]
            assert run.run_config == {}
            assert run.tags.get("dagster/sensor_name") == "two_job_sensor"
            assert run.pipeline_name == "the_graph"

            freeze_datetime = freeze_datetime.add(seconds=60)
        with pendulum.test(freeze_datetime):

            # should fire the asset sensor
            evaluate_sensors(instance, workspace)
            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 2
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.SUCCESS,
            )
            run = instance.get_runs()[0]
            assert run.run_config == {"solids": {"config_solid": {"config": {"foo": "blah"}}}}
            assert run.tags
            assert run.tags.get("dagster/sensor_name") == "two_job_sensor"
            assert run.pipeline_name == "config_graph"


def test_bad_run_request_untargeted():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            job_sensor = external_repo.get_external_sensor("bad_request_untargeted")
            instance.start_sensor(job_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                None,
                (
                    "Error in sensor bad_request_untargeted: Sensor evaluation function returned a "
                    "RunRequest for a sensor lacking a specified target (pipeline_name, job, or "
                    "jobs)."
                ),
            )


def test_bad_run_request_mismatch():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            job_sensor = external_repo.get_external_sensor("bad_request_mismatch")
            instance.start_sensor(job_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                None,
                (
                    "Error in sensor bad_request_mismatch: Sensor returned a RunRequest with "
                    "job_name config_pipeline. Expected one of: ['the_graph']"
                ),
            )


def test_bad_run_request_unspecified():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, tz="UTC"),
        "US/Central",
    )
    with instance_with_sensors() as (
        instance,
        workspace,
        external_repo,
    ):
        with pendulum.test(freeze_datetime):
            job_sensor = external_repo.get_external_sensor("bad_request_unspecified")
            instance.start_sensor(job_sensor)

            evaluate_sensors(instance, workspace)

            ticks = instance.get_ticks(job_sensor.get_external_origin_id())
            assert len(ticks) == 1
            validate_tick(
                ticks[0],
                job_sensor,
                freeze_datetime,
                TickStatus.FAILURE,
                None,
                (
                    "Error in sensor bad_request_unspecified: Sensor returned a RunRequest that "
                    "did not specify job_name for the requested run. Expected one of: "
                    "['the_graph', 'config_graph']"
                ),
            )


def test_status_in_code_sensor():
    freeze_datetime = to_timezone(
        create_pendulum_time(year=2019, month=2, day=27, hour=23, minute=59, second=59, tz="UTC"),
        "US/Central",
    )
    with instance_for_test() as instance:
        with create_test_daemon_workspace(
            workspace_load_target(attribute="the_status_in_code_repo")
        ) as workspace:
            external_repo = next(
                iter(workspace.get_workspace_snapshot().values())
            ).repository_location.get_repository("the_status_in_code_repo")

            with pendulum.test(freeze_datetime):

                running_sensor = external_repo.get_external_sensor("always_running_sensor")
                not_running_sensor = external_repo.get_external_sensor("never_running_sensor")

                always_running_origin = running_sensor.get_external_origin()
                never_running_origin = not_running_sensor.get_external_origin()

                assert instance.get_runs_count() == 0
                assert len(instance.get_ticks(always_running_origin.get_id())) == 0
                assert len(instance.get_ticks(never_running_origin.get_id())) == 0

                assert len(instance.all_instigator_state()) == 0

                evaluate_sensors(instance, workspace)

                assert instance.get_runs_count() == 0

                assert len(instance.all_instigator_state()) == 1
                instigator_state = instance.get_instigator_state(always_running_origin.get_id())
                assert instigator_state.status == InstigatorStatus.AUTOMATICALLY_RUNNING

                ticks = instance.get_ticks(running_sensor.get_external_origin_id())
                assert len(ticks) == 1
                validate_tick(
                    ticks[0],
                    running_sensor,
                    freeze_datetime,
                    TickStatus.SKIPPED,
                )

                assert len(instance.get_ticks(never_running_origin.get_id())) == 0

            freeze_datetime = freeze_datetime.add(seconds=30)
            with pendulum.test(freeze_datetime):
                evaluate_sensors(instance, workspace)
                wait_for_all_runs_to_start(instance)
                assert instance.get_runs_count() == 1
                run = instance.get_runs()[0]
                validate_run_started(run)
                ticks = instance.get_ticks(running_sensor.get_external_origin_id())
                assert len(ticks) == 2

                expected_datetime = create_pendulum_time(
                    year=2019, month=2, day=28, hour=0, minute=0, second=29
                )
                validate_tick(
                    ticks[0],
                    running_sensor,
                    expected_datetime,
                    TickStatus.SUCCESS,
                    [run.run_id],
                )

                assert len(instance.get_ticks(never_running_origin.get_id())) == 0
