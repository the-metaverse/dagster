import os
import random
import string
import time
from collections import defaultdict
from contextlib import contextmanager

import pendulum
import pytest

from dagster import (
    Any,
    Field,
    ModeDefinition,
    daily_partitioned_config,
    fs_io_manager,
    graph,
    pipeline,
    repository,
    solid,
)
from dagster._core.definitions import Partition, PartitionSetDefinition
from dagster._core.definitions.reconstructable import ReconstructableRepository
from dagster._core.execution.api import execute_pipeline
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.host_representation import (
    ExternalRepositoryOrigin,
    InProcessRepositoryLocationOrigin,
)
from dagster._core.storage.pipeline_run import PipelineRunStatus, RunsFilter
from dagster._core.storage.tags import BACKFILL_ID_TAG, PARTITION_NAME_TAG, PARTITION_SET_TAG
from dagster._core.test_utils import create_test_daemon_workspace, instance_for_test
from dagster._core.workspace.load_target import PythonFileTarget
from dagster._daemon import get_default_daemon_logger
from dagster._daemon.backfill import execute_backfill_iteration
from dagster._seven import IS_WINDOWS, get_system_temp_directory
from dagster._utils import touch_file
from dagster._utils.error import SerializableErrorInfo

default_mode_def = ModeDefinition(resource_defs={"io_manager": fs_io_manager})


def _failure_flag_file():
    return os.path.join(get_system_temp_directory(), "conditionally_fail")


def _step_events(instance, run):
    events_by_step = defaultdict(set)
    logs = instance.all_logs(run.run_id)
    for record in logs:
        if not record.is_dagster_event or not record.step_key:
            continue
        events_by_step[record.step_key] = record.dagster_event.event_type_value
    return events_by_step


@solid
def always_succeed(_):
    return 1


@graph()
def comp_always_succeed():
    always_succeed()


@daily_partitioned_config(start_date="2021-05-05")
def my_config(_start, _end):
    return {}


always_succeed_job = comp_always_succeed.to_job(config=my_config)


@solid
def fail_solid(_):
    raise Exception("blah")


@solid
def conditionally_fail(_, _input):
    if os.path.isfile(_failure_flag_file()):
        raise Exception("blah")

    return 1


@solid
def after_failure(_, _input):
    return 1


@pipeline(mode_defs=[default_mode_def])
def the_pipeline():
    always_succeed()


@pipeline(mode_defs=[default_mode_def])
def conditional_failure_pipeline():
    after_failure(conditionally_fail(always_succeed()))


@pipeline(mode_defs=[default_mode_def])
def partial_pipeline():
    always_succeed.alias("step_one")()
    always_succeed.alias("step_two")()
    always_succeed.alias("step_three")()


@pipeline(mode_defs=[default_mode_def])
def parallel_failure_pipeline():
    fail_solid.alias("fail_one")()
    fail_solid.alias("fail_two")()
    fail_solid.alias("fail_three")()
    always_succeed.alias("success_four")()


@solid(config_schema=Field(Any))
def config_solid(_):
    return 1


@pipeline(mode_defs=[default_mode_def])
def config_pipeline():
    config_solid()


simple_partition_set = PartitionSetDefinition(
    name="simple_partition_set",
    pipeline_name="the_pipeline",
    partition_fn=lambda: [Partition("one"), Partition("two"), Partition("three")],
)

conditionally_fail_partition_set = PartitionSetDefinition(
    name="conditionally_fail_partition_set",
    pipeline_name="conditional_failure_pipeline",
    partition_fn=lambda: [Partition("one"), Partition("two"), Partition("three")],
)

partial_partition_set = PartitionSetDefinition(
    name="partial_partition_set",
    pipeline_name="partial_pipeline",
    partition_fn=lambda: [Partition("one"), Partition("two"), Partition("three")],
)

parallel_failure_partition_set = PartitionSetDefinition(
    name="parallel_failure_partition_set",
    pipeline_name="parallel_failure_pipeline",
    partition_fn=lambda: [Partition("one"), Partition("two"), Partition("three")],
)


def _large_partition_config(_):
    REQUEST_CONFIG_COUNT = 50000

    def _random_string(length):
        return "".join(random.choice(string.ascii_lowercase) for x in range(length))

    return {
        "solids": {
            "config_solid": {
                "config": {
                    "foo": {
                        _random_string(10): _random_string(20) for i in range(REQUEST_CONFIG_COUNT)
                    }
                }
            }
        }
    }


large_partition_set = PartitionSetDefinition(
    name="large_partition_set",
    pipeline_name="config_pipeline",
    partition_fn=lambda: [Partition("one"), Partition("two"), Partition("three")],
    run_config_fn_for_partition=_large_partition_config,
)


def _unloadable_partition_set_origin():
    working_directory = os.path.dirname(__file__)
    recon_repo = ReconstructableRepository.for_file(__file__, "doesnt_exist", working_directory)
    return ExternalRepositoryOrigin(
        InProcessRepositoryLocationOrigin(recon_repo), "fake_repository"
    ).get_partition_set_origin("doesnt_exist")


@repository
def the_repo():
    return [
        the_pipeline,
        conditional_failure_pipeline,
        partial_pipeline,
        config_pipeline,
        simple_partition_set,
        conditionally_fail_partition_set,
        partial_partition_set,
        large_partition_set,
        always_succeed_job,
        parallel_failure_partition_set,
        parallel_failure_pipeline,
    ]


@contextmanager
def default_repo():
    load_target = workspace_load_target()
    origin = load_target.create_origins()[0]
    with origin.create_single_location() as location:
        yield location.get_repository("the_repo")


def workspace_load_target():
    return PythonFileTarget(
        python_file=__file__,
        attribute=None,
        working_directory=os.path.dirname(__file__),
        location_name="test_location",
    )


@contextmanager
def instance_for_context(external_repo_context, overrides=None):
    with instance_for_test(overrides) as instance:
        with create_test_daemon_workspace(
            workspace_load_target=workspace_load_target()
        ) as workspace:
            with external_repo_context() as external_repo:
                yield (instance, workspace, external_repo)


def step_did_not_run(instance, run, step_name):
    step_events = _step_events(instance, run)[step_name]
    return len(step_events) == 0


def step_succeeded(instance, run, step_name):
    step_events = _step_events(instance, run)[step_name]
    return "STEP_SUCCESS" in step_events


def step_failed(instance, run, step_name):
    step_events = _step_events(instance, run)[step_name]
    return "STEP_FAILURE" in step_events


def wait_for_all_runs_to_start(instance, timeout=10):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for runs to start")
        time.sleep(0.5)

        pending_states = [
            PipelineRunStatus.NOT_STARTED,
            PipelineRunStatus.STARTING,
            PipelineRunStatus.STARTED,
        ]
        pending_runs = [run for run in instance.get_runs() if run.status in pending_states]

        if len(pending_runs) == 0:
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


def test_simple_backfill():
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        external_partition_set = external_repo.get_external_partition_set("simple_partition_set")
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="simple",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0

        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )

        assert instance.get_runs_count() == 3
        runs = instance.get_runs()
        three, two, one = runs
        assert one.tags[BACKFILL_ID_TAG] == "simple"
        assert one.tags[PARTITION_NAME_TAG] == "one"
        assert two.tags[BACKFILL_ID_TAG] == "simple"
        assert two.tags[PARTITION_NAME_TAG] == "two"
        assert three.tags[BACKFILL_ID_TAG] == "simple"
        assert three.tags[PARTITION_NAME_TAG] == "three"


def test_failure_backfill():
    output_file = _failure_flag_file()
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        external_partition_set = external_repo.get_external_partition_set(
            "conditionally_fail_partition_set"
        )
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="shouldfail",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0

        try:
            touch_file(output_file)
            list(
                execute_backfill_iteration(
                    instance, workspace, get_default_daemon_logger("BackfillDaemon")
                )
            )
            wait_for_all_runs_to_start(instance)
        finally:
            os.remove(output_file)

        assert instance.get_runs_count() == 3
        runs = instance.get_runs()
        three, two, one = runs
        assert one.tags[BACKFILL_ID_TAG] == "shouldfail"
        assert one.tags[PARTITION_NAME_TAG] == "one"
        assert one.status == PipelineRunStatus.FAILURE
        assert step_succeeded(instance, one, "always_succeed")
        assert step_failed(instance, one, "conditionally_fail")
        assert step_did_not_run(instance, one, "after_failure")

        assert two.tags[BACKFILL_ID_TAG] == "shouldfail"
        assert two.tags[PARTITION_NAME_TAG] == "two"
        assert two.status == PipelineRunStatus.FAILURE
        assert step_succeeded(instance, two, "always_succeed")
        assert step_failed(instance, two, "conditionally_fail")
        assert step_did_not_run(instance, two, "after_failure")

        assert three.tags[BACKFILL_ID_TAG] == "shouldfail"
        assert three.tags[PARTITION_NAME_TAG] == "three"
        assert three.status == PipelineRunStatus.FAILURE
        assert step_succeeded(instance, three, "always_succeed")
        assert step_failed(instance, three, "conditionally_fail")
        assert step_did_not_run(instance, three, "after_failure")

        instance.add_backfill(
            PartitionBackfill(
                backfill_id="fromfailure",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=True,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )

        assert not os.path.isfile(_failure_flag_file())
        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )
        wait_for_all_runs_to_start(instance)

        assert instance.get_runs_count() == 6
        from_failure_filter = RunsFilter(tags={BACKFILL_ID_TAG: "fromfailure"})
        assert instance.get_runs_count(filters=from_failure_filter) == 3

        runs = instance.get_runs(filters=from_failure_filter)
        three, two, one = runs

        assert one.tags[BACKFILL_ID_TAG] == "fromfailure"
        assert one.tags[PARTITION_NAME_TAG] == "one"
        assert one.status == PipelineRunStatus.SUCCESS
        assert step_did_not_run(instance, one, "always_succeed")
        assert step_succeeded(instance, one, "conditionally_fail")
        assert step_succeeded(instance, one, "after_failure")

        assert two.tags[BACKFILL_ID_TAG] == "fromfailure"
        assert two.tags[PARTITION_NAME_TAG] == "two"
        assert two.status == PipelineRunStatus.SUCCESS
        assert step_did_not_run(instance, one, "always_succeed")
        assert step_succeeded(instance, one, "conditionally_fail")
        assert step_succeeded(instance, one, "after_failure")

        assert three.tags[BACKFILL_ID_TAG] == "fromfailure"
        assert three.tags[PARTITION_NAME_TAG] == "three"
        assert three.status == PipelineRunStatus.SUCCESS
        assert step_did_not_run(instance, one, "always_succeed")
        assert step_succeeded(instance, one, "conditionally_fail")
        assert step_succeeded(instance, one, "after_failure")


@pytest.mark.skipif(IS_WINDOWS, reason="flaky in windows")
def test_partial_backfill():
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        external_partition_set = external_repo.get_external_partition_set("partial_partition_set")

        # create full runs, where every step is executed
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="full",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0
        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )
        wait_for_all_runs_to_start(instance)

        assert instance.get_runs_count() == 3
        runs = instance.get_runs()
        three, two, one = runs

        assert one.tags[BACKFILL_ID_TAG] == "full"
        assert one.tags[PARTITION_NAME_TAG] == "one"
        assert one.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, one, "step_one")
        assert step_succeeded(instance, one, "step_two")
        assert step_succeeded(instance, one, "step_three")

        assert two.tags[BACKFILL_ID_TAG] == "full"
        assert two.tags[PARTITION_NAME_TAG] == "two"
        assert two.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, two, "step_one")
        assert step_succeeded(instance, two, "step_two")
        assert step_succeeded(instance, two, "step_three")

        assert three.tags[BACKFILL_ID_TAG] == "full"
        assert three.tags[PARTITION_NAME_TAG] == "three"
        assert three.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, three, "step_one")
        assert step_succeeded(instance, three, "step_two")
        assert step_succeeded(instance, three, "step_three")

        # delete one of the runs, the partial reexecution should still succeed because the steps
        # can be executed independently, require no input/output config
        instance.delete_run(one.run_id)
        assert instance.get_runs_count() == 2

        # create partial runs
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="partial",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=["step_one"],
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )
        wait_for_all_runs_to_start(instance)

        assert instance.get_runs_count() == 5
        partial_filter = RunsFilter(tags={BACKFILL_ID_TAG: "partial"})
        assert instance.get_runs_count(filters=partial_filter) == 3
        runs = instance.get_runs(filters=partial_filter)
        three, two, one = runs

        assert one.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, one, "step_one")
        assert step_did_not_run(instance, one, "step_two")
        assert step_did_not_run(instance, one, "step_three")

        assert two.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, two, "step_one")
        assert step_did_not_run(instance, two, "step_two")
        assert step_did_not_run(instance, two, "step_three")

        assert three.status == PipelineRunStatus.SUCCESS
        assert step_succeeded(instance, three, "step_one")
        assert step_did_not_run(instance, three, "step_two")
        assert step_did_not_run(instance, three, "step_three")


def test_large_backfill():
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        external_partition_set = external_repo.get_external_partition_set("large_partition_set")
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="simple",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0

        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )

        assert instance.get_runs_count() == 3


def test_unloadable_backfill():
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        _external_repo,
    ):
        unloadable_origin = _unloadable_partition_set_origin()
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="simple",
                partition_set_origin=unloadable_origin,
                status=BulkActionStatus.REQUESTED,
                partition_names=["one", "two", "three"],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0

        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )

        assert instance.get_runs_count() == 0
        backfill = instance.get_backfill("simple")
        assert backfill.status == BulkActionStatus.FAILED
        assert isinstance(backfill.error, SerializableErrorInfo)


def test_backfill_from_partitioned_job():
    partition_name_list = [
        partition.name for partition in my_config.partitions_def.get_partitions()
    ]
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        external_partition_set = external_repo.get_external_partition_set(
            "comp_always_succeed_partition_set"
        )
        instance.add_backfill(
            PartitionBackfill(
                backfill_id="partition_schedule_from_job",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=partition_name_list[:3],
                from_failure=False,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )
        assert instance.get_runs_count() == 0

        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )

        assert instance.get_runs_count() == 3
        runs = reversed(instance.get_runs())
        for idx, run in enumerate(runs):
            assert run.tags[BACKFILL_ID_TAG] == "partition_schedule_from_job"
            assert run.tags[PARTITION_NAME_TAG] == partition_name_list[idx]
            assert run.tags[PARTITION_SET_TAG] == "comp_always_succeed_partition_set"


def test_backfill_from_failure_for_subselection():
    with instance_for_context(default_repo) as (
        instance,
        workspace,
        external_repo,
    ):
        partition = parallel_failure_partition_set.get_partition("one")
        run_config = parallel_failure_partition_set.run_config_for_partition(partition)
        tags = parallel_failure_partition_set.tags_for_partition(partition)
        external_partition_set = external_repo.get_external_partition_set(
            "parallel_failure_partition_set"
        )

        execute_pipeline(
            parallel_failure_pipeline,
            run_config=run_config,
            tags=tags,
            instance=instance,
            solid_selection=["fail_three", "success_four"],
            raise_on_error=False,
        )

        assert instance.get_runs_count() == 1
        wait_for_all_runs_to_finish(instance)
        run = instance.get_runs()[0]
        assert run.status == PipelineRunStatus.FAILURE

        instance.add_backfill(
            PartitionBackfill(
                backfill_id="fromfailure",
                partition_set_origin=external_partition_set.get_external_origin(),
                status=BulkActionStatus.REQUESTED,
                partition_names=["one"],
                from_failure=True,
                reexecution_steps=None,
                tags=None,
                backfill_timestamp=pendulum.now().timestamp(),
            )
        )

        list(
            execute_backfill_iteration(
                instance, workspace, get_default_daemon_logger("BackfillDaemon")
            )
        )
        assert instance.get_runs_count() == 2
        run = instance.get_runs(limit=1)[0]
        assert run.solids_to_execute
        assert run.solid_selection
        assert len(run.solids_to_execute) == 2
        assert len(run.solid_selection) == 2
