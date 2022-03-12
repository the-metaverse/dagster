import re
import time
from collections import Counter
from contextlib import ExitStack

import mock
import pendulum
import pytest

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    DagsterInstance,
    Field,
    InputDefinition,
    ModeDefinition,
    Output,
    OutputDefinition,
    RetryRequested,
    op,
    pipeline,
    resource,
    seven,
    solid,
)
from dagster._core.definitions import ExpectationResult
from dagster._core.definitions.dependency import NodeHandle
from dagster._core.definitions.pipeline_base import InMemoryPipeline
from dagster._core.events import (
    DagsterEvent,
    DagsterEventType,
    EngineEventData,
    StepExpectationResultData,
    StepMaterializationData,
)
from dagster._core.events.log import EventLogEntry, construct_event_logger
from dagster._core.execution.api import execute_run
from dagster._core.execution.plan.handle import StepHandle
from dagster._core.execution.plan.objects import StepFailureData, StepSuccessData
from dagster._core.execution.stats import StepEventStatus
from dagster._core.storage.event_log import InMemoryEventLogStorage, SqlEventLogStorage
from dagster._core.storage.event_log.base import (
    EventLogRecord,
    EventRecordsFilter,
    RunShardedEventsCursor,
)
from dagster._core.storage.event_log.migration import (
    EVENT_LOG_DATA_MIGRATIONS,
    migrate_asset_key_data,
)
from dagster._core.storage.event_log.sqlite.sqlite_event_log import SqliteEventLogStorage
from dagster._core.test_utils import instance_for_test
from dagster._core.utils import make_new_run_id
from dagster._loggers import colored_console_logger
from dagster._serdes import deserialize_json_to_dagster_namedtuple
from dagster.utils import datetime_as_float

DEFAULT_RUN_ID = "foo"

TEST_TIMEOUT = 5

# py36 & 37 list.append not hashable
# pylint: disable=unnecessary-lambda


def create_test_event_log_record(message: str, run_id: str = DEFAULT_RUN_ID):
    return EventLogEntry(
        error_info=None,
        user_message=message,
        level="debug",
        run_id=run_id,
        timestamp=time.time(),
        dagster_event=DagsterEvent(
            DagsterEventType.ENGINE_EVENT.value,
            "nonce",
            event_specific_data=EngineEventData.in_process(999),
        ),
    )


def _stats_records(run_id):
    now = time.time()
    return [
        _event_record(run_id, "A", now - 325, DagsterEventType.STEP_START),
        _event_record(
            run_id,
            "A",
            now - 225,
            DagsterEventType.STEP_SUCCESS,
            StepSuccessData(duration_ms=100000.0),
        ),
        _event_record(run_id, "B", now - 225, DagsterEventType.STEP_START),
        _event_record(
            run_id,
            "B",
            now - 175,
            DagsterEventType.STEP_FAILURE,
            StepFailureData(error=None, user_failure_data=None),
        ),
        _event_record(run_id, "C", now - 175, DagsterEventType.STEP_START),
        _event_record(run_id, "C", now - 150, DagsterEventType.STEP_SKIPPED),
        _event_record(run_id, "D", now - 150, DagsterEventType.STEP_START),
        _event_record(
            run_id,
            "D",
            now - 125,
            DagsterEventType.ASSET_MATERIALIZATION,
            StepMaterializationData(AssetMaterialization(asset_key="mat_1")),
        ),
        _event_record(
            run_id,
            "D",
            now - 100,
            DagsterEventType.STEP_EXPECTATION_RESULT,
            StepExpectationResultData(ExpectationResult(success=True, label="exp 1")),
        ),
        _event_record(
            run_id,
            "D",
            now - 75,
            DagsterEventType.ASSET_MATERIALIZATION,
            StepMaterializationData(AssetMaterialization(asset_key="mat_2")),
        ),
        _event_record(
            run_id,
            "D",
            now - 50,
            DagsterEventType.STEP_EXPECTATION_RESULT,
            StepExpectationResultData(ExpectationResult(success=False, label="exp 2")),
        ),
        _event_record(
            run_id,
            "D",
            now - 25,
            DagsterEventType.ASSET_MATERIALIZATION,
            StepMaterializationData(AssetMaterialization(asset_key="mat_3")),
        ),
        _event_record(
            run_id,
            "D",
            now,
            DagsterEventType.STEP_SUCCESS,
            StepSuccessData(duration_ms=150000.0),
        ),
    ]


def _event_record(run_id, solid_name, timestamp, event_type, event_specific_data=None):
    pipeline_name = "pipeline_name"
    solid_handle = NodeHandle(solid_name, None)
    step_handle = StepHandle(solid_handle)
    return EventLogEntry(
        error_info=None,
        user_message="",
        level="debug",
        run_id=run_id,
        timestamp=timestamp,
        step_key=step_handle.to_key(),
        pipeline_name=pipeline_name,
        dagster_event=DagsterEvent(
            event_type.value,
            pipeline_name,
            solid_handle=solid_handle,
            step_handle=step_handle,
            event_specific_data=event_specific_data,
        ),
    )


def _mode_def(event_callback):
    @resource
    def foo_resource():
        time.sleep(0.1)
        return "foo"

    return ModeDefinition(
        resource_defs={"foo": foo_resource},
        logger_defs={
            "callback": construct_event_logger(event_callback),
            "console": colored_console_logger,
        },
    )


# This exists to create synthetic events to test the store
def _synthesize_events(solids_fn, run_id=None, check_success=True, instance=None, run_config=None):
    events = []

    def _append_event(event):
        events.append(event)

    @pipeline(mode_defs=[_mode_def(_append_event)])
    def a_pipe():
        solids_fn()

    with ExitStack() as stack:
        if not instance:
            instance = stack.enter_context(DagsterInstance.ephemeral())

        run_config = {
            **{"loggers": {"callback": {}, "console": {}}},
            **(run_config if run_config else {}),
        }

        pipeline_run = instance.create_run_for_pipeline(
            a_pipe, run_id=run_id, run_config=run_config
        )

        result = execute_run(InMemoryPipeline(a_pipe), pipeline_run, instance)

        if check_success:
            assert result.success

        return events, result


def _fetch_all_events(configured_storage, run_id=None):
    with configured_storage.run_connection(run_id=run_id) as conn:
        res = conn.execute("SELECT event from event_logs")
        return res.fetchall()


def _event_types(out_events):
    return list(map(lambda e: e.dagster_event.event_type if e.dagster_event else None, out_events))


@solid
def should_succeed(context):
    context.log.info("succeed")
    return "yay"


@solid
def solid_one(_):
    yield AssetMaterialization(asset_key=AssetKey("asset_1"))
    yield Output(1)


@solid
def solid_two(_):
    yield AssetMaterialization(asset_key=AssetKey("asset_2"))
    yield AssetMaterialization(asset_key=AssetKey(["path", "to", "asset_3"]))
    yield Output(1)


def one_solid():
    solid_one()


def two_solids():
    solid_one()
    solid_two()


def cursor_datetime_args():
    # parametrization function to test constructing run-sharded event log cursors, with both
    # timezone-aware and timezone-naive datetimes
    yield None
    yield pendulum.now()
    yield pendulum.now().naive()


class TestEventLogStorage:
    """
    You can extend this class to easily run these set of tests on any event log storage. When extending,
    you simply need to override the `event_log_storage` fixture and return your implementation of
    `EventLogStorage`.

    For example:

    ```
    class TestMyStorageImplementation(TestEventLogStorage):
        __test__ = True

        @pytest.fixture(scope='function', name='storage')
        def event_log_storage(self):  # pylint: disable=arguments-differ
            return MyStorageImplementation()
    ```
    """

    __test__ = False

    @pytest.fixture(name="storage", params=[])
    def event_log_storage(self, request):
        with request.param() as s:
            try:
                yield s
            finally:
                s.dispose()

    def test_init_log_storage(self, storage):
        if isinstance(storage, InMemoryEventLogStorage):
            assert not storage.is_persistent
        else:
            assert storage.is_persistent

    def test_log_storage_run_not_found(self, storage):
        assert storage.get_logs_for_run("bar") == []

    def can_wipe(self):
        # Whether the storage is allowed to wipe the event log
        return True

    def can_watch(self):
        # Whether the storage is allowed to watch the event log
        return True

    def test_event_log_storage_store_events_and_wipe(self, storage):
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=DEFAULT_RUN_ID,
                timestamp=time.time(),
                dagster_event=DagsterEvent(
                    DagsterEventType.ENGINE_EVENT.value,
                    "nonce",
                    event_specific_data=EngineEventData.in_process(999),
                ),
            )
        )
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 1
        assert storage.get_stats_for_run(DEFAULT_RUN_ID)

        if self.can_wipe():
            storage.wipe()
            assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0

    def test_event_log_storage_store_with_multiple_runs(self, storage):
        runs = ["foo", "bar", "baz"]
        for run_id in runs:
            assert len(storage.get_logs_for_run(run_id)) == 0
            storage.store_event(
                EventLogEntry(
                    error_info=None,
                    level="debug",
                    user_message="",
                    run_id=run_id,
                    timestamp=time.time(),
                    dagster_event=DagsterEvent(
                        DagsterEventType.STEP_SUCCESS.value,
                        "nonce",
                        event_specific_data=StepSuccessData(duration_ms=100.0),
                    ),
                )
            )

        for run_id in runs:
            assert len(storage.get_logs_for_run(run_id)) == 1
            assert storage.get_stats_for_run(run_id).steps_succeeded == 1

        if self.can_wipe():
            storage.wipe()
            for run_id in runs:
                assert len(storage.get_logs_for_run(run_id)) == 0

    def test_event_log_storage_watch(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        watched = []
        watcher = lambda x: watched.append(x)  # pylint: disable=unnecessary-lambda

        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0

        storage.store_event(create_test_event_log_record(str(1)))
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 1
        assert len(watched) == 0

        storage.watch(DEFAULT_RUN_ID, 0, watcher)

        storage.store_event(create_test_event_log_record(str(2)))
        storage.store_event(create_test_event_log_record(str(3)))
        storage.store_event(create_test_event_log_record(str(4)))

        attempts = 10
        while len(watched) < 3 and attempts > 0:
            time.sleep(0.5)
            attempts -= 1
        assert len(watched) == 3

        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 4

        storage.end_watch(DEFAULT_RUN_ID, watcher)
        time.sleep(0.3)  # this value scientifically selected from a range of attractive values
        storage.store_event(create_test_event_log_record(str(5)))

        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 5
        assert len(watched) == 3

        storage.delete_events(DEFAULT_RUN_ID)

        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0
        assert len(watched) == 3

        assert [int(evt.user_message) for evt in watched] == [2, 3, 4]

    def test_event_log_storage_pagination(self, storage):
        # interleave two runs events to ensure pagination is not affected by other runs
        storage.store_event(create_test_event_log_record("A"))
        storage.store_event(create_test_event_log_record(str(0), run_id="other_run"))
        storage.store_event(create_test_event_log_record("B"))
        storage.store_event(create_test_event_log_record(str(1), run_id="other_run"))
        storage.store_event(create_test_event_log_record("C"))
        storage.store_event(create_test_event_log_record(str(2), run_id="other_run"))
        storage.store_event(create_test_event_log_record("D"))

        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 4
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID, -1)) == 4
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID, 0)) == 3
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID, 1)) == 2
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID, 2)) == 1
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID, 3)) == 0

    def test_event_log_delete(self, storage):
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0
        storage.store_event(create_test_event_log_record(str(0)))
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 1
        assert storage.get_stats_for_run(DEFAULT_RUN_ID)
        storage.delete_events(DEFAULT_RUN_ID)
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0

    def test_event_log_get_stats_without_start_and_success(self, storage):
        # When an event log doesn't have a PIPELINE_START or PIPELINE_SUCCESS | PIPELINE_FAILURE event,
        # we want to ensure storage.get_stats_for_run(...) doesn't throw an error.
        assert len(storage.get_logs_for_run(DEFAULT_RUN_ID)) == 0
        assert storage.get_stats_for_run(DEFAULT_RUN_ID)

    def test_event_log_get_stats_for_run(self, storage):
        import math

        enqueued_time = time.time()
        launched_time = enqueued_time + 20
        start_time = launched_time + 50
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=DEFAULT_RUN_ID,
                timestamp=enqueued_time,
                dagster_event=DagsterEvent(
                    DagsterEventType.PIPELINE_ENQUEUED.value,
                    "nonce",
                ),
            )
        )
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=DEFAULT_RUN_ID,
                timestamp=launched_time,
                dagster_event=DagsterEvent(
                    DagsterEventType.PIPELINE_STARTING.value,
                    "nonce",
                ),
            )
        )
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=DEFAULT_RUN_ID,
                timestamp=start_time,
                dagster_event=DagsterEvent(
                    DagsterEventType.PIPELINE_START.value,
                    "nonce",
                ),
            )
        )
        assert math.isclose(storage.get_stats_for_run(DEFAULT_RUN_ID).enqueued_time, enqueued_time)
        assert math.isclose(storage.get_stats_for_run(DEFAULT_RUN_ID).launch_time, launched_time)
        assert math.isclose(storage.get_stats_for_run(DEFAULT_RUN_ID).start_time, start_time)

    def test_event_log_step_stats(self, storage):
        # When an event log doesn't have a PIPELINE_START or PIPELINE_SUCCESS | PIPELINE_FAILURE event,
        # we want to ensure storage.get_stats_for_run(...) doesn't throw an error.

        for record in _stats_records(run_id=DEFAULT_RUN_ID):
            storage.store_event(record)

        step_stats = storage.get_step_stats_for_run(DEFAULT_RUN_ID)
        assert len(step_stats) == 4

        a_stats = [stats for stats in step_stats if stats.step_key == "A"][0]
        assert a_stats.step_key == "A"
        assert a_stats.status.value == "SUCCESS"
        assert a_stats.end_time - a_stats.start_time == 100
        assert len(a_stats.attempts_list) == 1

        b_stats = [stats for stats in step_stats if stats.step_key == "B"][0]
        assert b_stats.step_key == "B"
        assert b_stats.status.value == "FAILURE"
        assert b_stats.end_time - b_stats.start_time == 50
        assert len(b_stats.attempts_list) == 1

        c_stats = [stats for stats in step_stats if stats.step_key == "C"][0]
        assert c_stats.step_key == "C"
        assert c_stats.status.value == "SKIPPED"
        assert c_stats.end_time - c_stats.start_time == 25
        assert len(c_stats.attempts_list) == 1

        d_stats = [stats for stats in step_stats if stats.step_key == "D"][0]
        assert d_stats.step_key == "D"
        assert d_stats.status.value == "SUCCESS"
        assert d_stats.end_time - d_stats.start_time == 150
        assert len(d_stats.materialization_events) == 3
        assert len(d_stats.expectation_results) == 2
        assert len(c_stats.attempts_list) == 1

    def test_secondary_index(self, storage):
        if not isinstance(storage, SqlEventLogStorage):
            pytest.skip("This test is for SQL-backed Event Log behavior")

        # test that newly initialized DBs will have the secondary indexes built
        for name in EVENT_LOG_DATA_MIGRATIONS.keys():
            assert storage.has_secondary_index(name)

        # test the generic API with garbage migration names
        assert not storage.has_secondary_index("_A")
        assert not storage.has_secondary_index("_B")
        storage.enable_secondary_index("_A")
        assert storage.has_secondary_index("_A")
        assert not storage.has_secondary_index("_B")
        storage.enable_secondary_index("_B")
        assert storage.has_secondary_index("_A")
        assert storage.has_secondary_index("_B")

    def test_basic_event_store(self, storage):
        if not isinstance(storage, SqlEventLogStorage):
            pytest.skip("This test is for SQL-backed Event Log behavior")

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, _result = _synthesize_events(_solids, run_id=DEFAULT_RUN_ID)

        for event in events:
            storage.store_event(event)

        rows = _fetch_all_events(storage, run_id=DEFAULT_RUN_ID)

        out_events = list(map(lambda r: deserialize_json_to_dagster_namedtuple(r[0]), rows))

        # messages can come out of order
        event_type_counts = Counter(_event_types(out_events))
        assert event_type_counts
        assert Counter(_event_types(out_events)) == Counter(_event_types(events))

    def test_basic_get_logs_for_run(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        out_events = storage.get_logs_for_run(result.run_id)

        assert _event_types(out_events) == _event_types(events)

    def test_get_logs_for_run_cursor_limit(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        out_events = []
        cursor = -1
        fuse = 0
        chunk_size = 2
        while fuse < 50:
            fuse += 1
            # fetch in batches w/ limit & cursor
            chunk = storage.get_logs_for_run(result.run_id, cursor=cursor, limit=chunk_size)
            if not chunk:
                break
            assert len(chunk) <= chunk_size
            out_events += chunk
            cursor += len(chunk)

        assert _event_types(out_events) == _event_types(events)

    def test_wipe_sql_backed_event_log(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        out_events = storage.get_logs_for_run(result.run_id)

        assert _event_types(out_events) == _event_types(events)

        if self.can_wipe():
            storage.wipe()

            assert storage.get_logs_for_run(result.run_id) == []

    def test_delete_sql_backed_event_log(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        out_events = storage.get_logs_for_run(result.run_id)

        assert _event_types(out_events) == _event_types(events)

        storage.delete_events(result.run_id)

        assert storage.get_logs_for_run(result.run_id) == []

    def test_get_logs_for_run_of_type(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        assert _event_types(
            storage.get_logs_for_run(result.run_id, of_type=DagsterEventType.PIPELINE_SUCCESS)
        ) == [DagsterEventType.PIPELINE_SUCCESS]

        assert _event_types(
            storage.get_logs_for_run(result.run_id, of_type=DagsterEventType.STEP_SUCCESS)
        ) == [DagsterEventType.STEP_SUCCESS]

        assert _event_types(
            storage.get_logs_for_run(
                result.run_id,
                of_type={DagsterEventType.STEP_SUCCESS, DagsterEventType.PIPELINE_SUCCESS},
            )
        ) == [DagsterEventType.STEP_SUCCESS, DagsterEventType.PIPELINE_SUCCESS]

    def test_basic_get_logs_for_run_cursor(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events, result = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        assert _event_types(storage.get_logs_for_run(result.run_id, cursor=-1)) == _event_types(
            events
        )

    def test_basic_get_logs_for_run_multiple_runs(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events_one, result_one = _synthesize_events(_solids)
        for event in events_one:
            storage.store_event(event)

        events_two, result_two = _synthesize_events(_solids)
        for event in events_two:
            storage.store_event(event)

        out_events_one = storage.get_logs_for_run(result_one.run_id)
        assert len(out_events_one) == len(events_one)

        assert set(_event_types(out_events_one)) == set(_event_types(events_one))

        assert set(map(lambda e: e.run_id, out_events_one)) == {result_one.run_id}

        stats_one = storage.get_stats_for_run(result_one.run_id)
        assert stats_one.steps_succeeded == 1

        out_events_two = storage.get_logs_for_run(result_two.run_id)
        assert len(out_events_two) == len(events_two)

        assert set(_event_types(out_events_two)) == set(_event_types(events_two))

        assert set(map(lambda e: e.run_id, out_events_two)) == {result_two.run_id}

        stats_two = storage.get_stats_for_run(result_two.run_id)
        assert stats_two.steps_succeeded == 1

    def test_basic_get_logs_for_run_multiple_runs_cursors(self, storage):
        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        events_one, result_one = _synthesize_events(_solids)
        for event in events_one:
            storage.store_event(event)

        events_two, result_two = _synthesize_events(_solids)
        for event in events_two:
            storage.store_event(event)

        out_events_one = storage.get_logs_for_run(result_one.run_id, cursor=-1)
        assert len(out_events_one) == len(events_one)

        assert set(_event_types(out_events_one)) == set(_event_types(events_one))

        assert set(map(lambda e: e.run_id, out_events_one)) == {result_one.run_id}

        out_events_two = storage.get_logs_for_run(result_two.run_id, cursor=-1)
        assert len(out_events_two) == len(events_two)
        assert set(_event_types(out_events_two)) == set(_event_types(events_one))

        assert set(map(lambda e: e.run_id, out_events_two)) == {result_two.run_id}

    def test_event_watcher_single_run_event(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        event_list = []

        run_id = make_new_run_id()

        storage.watch(run_id, -1, lambda x: event_list.append(x))

        events, _ = _synthesize_events(_solids, run_id=run_id)
        for event in events:
            storage.store_event(event)

        start = time.time()
        while len(event_list) < len(events) and time.time() - start < TEST_TIMEOUT:
            time.sleep(0.01)

        assert len(event_list) == len(events)
        assert all([isinstance(event, EventLogEntry) for event in event_list])

    def test_event_watcher_filter_run_event(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        run_id_one = make_new_run_id()
        run_id_two = make_new_run_id()

        # only watch one of the runs
        event_list = []
        storage.watch(run_id_two, -1, lambda x: event_list.append(x))

        events_one, _result_one = _synthesize_events(_solids, run_id=run_id_one)
        for event in events_one:
            storage.store_event(event)

        events_two, _result_two = _synthesize_events(_solids, run_id=run_id_two)
        for event in events_two:
            storage.store_event(event)

        start = time.time()
        while len(event_list) < len(events_two) and time.time() - start < TEST_TIMEOUT:
            time.sleep(0.01)

        assert len(event_list) == len(events_two)
        assert all([isinstance(event, EventLogEntry) for event in event_list])

    def test_event_watcher_filter_two_runs_event(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        event_list_one = []
        event_list_two = []

        run_id_one = make_new_run_id()
        run_id_two = make_new_run_id()

        storage.watch(run_id_one, -1, lambda x: event_list_one.append(x))
        storage.watch(run_id_two, -1, lambda x: event_list_two.append(x))

        events_one, _result_one = _synthesize_events(_solids, run_id=run_id_one)
        for event in events_one:
            storage.store_event(event)

        events_two, _result_two = _synthesize_events(_solids, run_id=run_id_two)
        for event in events_two:
            storage.store_event(event)

        start = time.time()
        while (
            len(event_list_one) < len(events_one) or len(event_list_two) < len(events_two)
        ) and time.time() - start < TEST_TIMEOUT:
            pass

        assert len(event_list_one) == len(events_one)
        assert len(event_list_two) == len(events_two)
        assert all([isinstance(event, EventLogEntry) for event in event_list_one])
        assert all([isinstance(event, EventLogEntry) for event in event_list_two])

    def test_correct_timezone(self, storage):
        curr_time = time.time()

        event = EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id="foo",
            timestamp=curr_time,
            dagster_event=DagsterEvent(
                DagsterEventType.PIPELINE_START.value,
                "nonce",
                event_specific_data=EngineEventData.in_process(999),
            ),
        )

        storage.store_event(event)

        logs = storage.get_logs_for_run("foo")

        assert len(logs) == 1

        log = logs[0]

        stats = storage.get_stats_for_run("foo")

        assert int(log.timestamp) == int(stats.start_time)
        assert int(log.timestamp) == int(curr_time)

    def test_asset_materialization(self, storage):
        asset_key = AssetKey(["path", "to", "asset_one"])

        @solid
        def materialize_one(_):
            yield AssetMaterialization(
                asset_key=asset_key,
                metadata={
                    "text": "hello",
                    "json": {"hello": "world"},
                    "one_float": 1.0,
                    "one_int": 1,
                },
            )
            yield Output(1)

        def _solids():
            materialize_one()

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(_solids, instance=instance)

            for event in events_one:
                storage.store_event(event)

            assert asset_key in set(storage.all_asset_keys())
            events = storage.get_asset_events(asset_key)
            assert len(events) == 1
            event = events[0]
            assert isinstance(event, EventLogEntry)
            assert (
                event.dagster_event.event_type_value == DagsterEventType.ASSET_MATERIALIZATION.value
            )

            records = storage.get_event_records(EventRecordsFilter(asset_key=asset_key))
            assert len(records) == 1
            record = records[0]
            assert isinstance(record, EventLogRecord)
            assert record.event_log_entry == event

    def test_asset_events_error_parsing(self, storage):
        if not isinstance(storage, SqlEventLogStorage):
            pytest.skip("This test is for SQL-backed Event Log behavior")
        _logs = []

        def mock_log(msg):
            _logs.append(msg)

        asset_key = AssetKey("asset_one")

        @solid
        def materialize_one(_):
            yield AssetMaterialization(asset_key=asset_key)
            yield Output(1)

        def _solids():
            materialize_one()

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)
            events_one, _ = _synthesize_events(_solids, instance=instance)
            for event in events_one:
                storage.store_event(event)

            with ExitStack() as stack:
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sql_event_log.logging.warning",
                        side_effect=mock_log,
                    )
                )
                # for generic sql-based event log storage
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sql_event_log.deserialize_json_to_dagster_namedtuple",
                        return_value="not_an_event_record",
                    )
                )
                # for sqlite event log storage, which overrides the record fetching implementation
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sqlite.sqlite_event_log.deserialize_json_to_dagster_namedtuple",
                        return_value="not_an_event_record",
                    )
                )

                assert asset_key in set(storage.all_asset_keys())
                events = storage.get_asset_events(asset_key)
                assert len(events) == 0
                assert len(_logs) == 1
                assert re.match("Could not resolve event record as EventLogEntry", _logs[0])

            with ExitStack() as stack:
                _logs = []  # reset logs
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sql_event_log.logging.warning",
                        side_effect=mock_log,
                    )
                )

                # for generic sql-based event log storage
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sql_event_log.deserialize_json_to_dagster_namedtuple",
                        side_effect=seven.JSONDecodeError("error", "", 0),
                    )
                )
                # for sqlite event log storage, which overrides the record fetching implementation
                stack.enter_context(
                    mock.patch(
                        "dagster._core.storage.event_log.sqlite.sqlite_event_log.deserialize_json_to_dagster_namedtuple",
                        side_effect=seven.JSONDecodeError("error", "", 0),
                    )
                )
                assert asset_key in set(storage.all_asset_keys())
                events = storage.get_asset_events(asset_key)
                assert len(events) == 0
                assert len(_logs) == 1
                assert re.match("Could not parse event record id", _logs[0])

    def test_secondary_index_asset_keys(self, storage):
        asset_key_one = AssetKey(["one"])
        asset_key_two = AssetKey(["two"])

        @solid
        def materialize_one(_):
            yield AssetMaterialization(asset_key=asset_key_one)
            yield Output(1)

        @solid
        def materialize_two(_):
            yield AssetMaterialization(asset_key=asset_key_two)
            yield Output(1)

        def _one():
            materialize_one()

        def _two():
            materialize_two()

        events_one, _ = _synthesize_events(_one)
        for event in events_one:
            storage.store_event(event)

        asset_keys = storage.all_asset_keys()
        assert len(asset_keys) == 1
        assert asset_key_one in set(asset_keys)
        migrate_asset_key_data(storage)
        asset_keys = storage.all_asset_keys()
        assert len(asset_keys) == 1
        assert asset_key_one in set(asset_keys)
        events_two, _ = _synthesize_events(_two)
        for event in events_two:
            storage.store_event(event)
        asset_keys = storage.all_asset_keys()
        assert len(asset_keys) == 2
        assert asset_key_one in set(asset_keys)
        assert asset_key_two in set(asset_keys)

    def test_run_step_stats(self, storage):
        @solid(input_defs=[InputDefinition("_input", str)], output_defs=[OutputDefinition(str)])
        def should_fail(context, _input):
            context.log.info("fail")
            raise Exception("booo")

        def _one():
            should_fail(should_succeed())

        events, result = _synthesize_events(_one, check_success=False)
        for event in events:
            storage.store_event(event)

        step_stats = sorted(storage.get_step_stats_for_run(result.run_id), key=lambda x: x.end_time)
        assert len(step_stats) == 2
        assert step_stats[0].step_key == "should_succeed"
        assert step_stats[0].status == StepEventStatus.SUCCESS
        assert step_stats[0].end_time > step_stats[0].start_time
        assert step_stats[0].attempts == 1
        assert len(step_stats[0].attempts_list) == 1
        assert step_stats[1].step_key == "should_fail"
        assert step_stats[1].status == StepEventStatus.FAILURE
        assert step_stats[1].end_time > step_stats[0].start_time
        assert step_stats[1].attempts == 1
        assert len(step_stats[1].attempts_list) == 1

    def test_run_step_stats_with_retries(self, storage):
        @solid(input_defs=[InputDefinition("_input", str)], output_defs=[OutputDefinition(str)])
        def should_retry(context, _input):
            raise RetryRequested(max_retries=3)

        def _one():
            should_retry(should_succeed())

        events, result = _synthesize_events(_one, check_success=False)
        for event in events:
            storage.store_event(event)

        step_stats = storage.get_step_stats_for_run(result.run_id, step_keys=["should_retry"])
        assert len(step_stats) == 1
        assert step_stats[0].step_key == "should_retry"
        assert step_stats[0].status == StepEventStatus.FAILURE
        assert step_stats[0].end_time > step_stats[0].start_time
        assert step_stats[0].attempts == 4
        assert len(step_stats[0].attempts_list) == 4

    # After adding the IN_PROGRESS field to the StepEventStatus enum, tests in internal fail
    # Temporarily skipping this test
    @pytest.mark.skip
    def test_run_step_stats_with_in_progress(self, storage):
        def _in_progress_run_records(run_id):
            now = time.time()
            return [
                _event_record(run_id, "A", now - 325, DagsterEventType.STEP_START),
                _event_record(run_id, "C", now - 175, DagsterEventType.STEP_START),
                _event_record(run_id, "C", now - 150, DagsterEventType.STEP_SKIPPED),
                _event_record(run_id, "D", now - 150, DagsterEventType.STEP_START),
                _event_record(run_id, "D", now - 150, DagsterEventType.STEP_UP_FOR_RETRY),
                _event_record(run_id, "E", now - 150, DagsterEventType.STEP_START),
                _event_record(run_id, "E", now - 150, DagsterEventType.STEP_UP_FOR_RETRY),
                _event_record(run_id, "E", now - 125, DagsterEventType.STEP_RESTARTED),
            ]

        for record in _in_progress_run_records(run_id=DEFAULT_RUN_ID):
            storage.store_event(record)

        step_stats = storage.get_step_stats_for_run(DEFAULT_RUN_ID)

        assert len(step_stats) == 4

        assert step_stats[0].step_key == "A"
        assert step_stats[0].status == StepEventStatus.IN_PROGRESS
        assert not step_stats[0].end_time
        assert step_stats[0].attempts == 1
        assert len(step_stats[0].attempts_list) == 1

        assert step_stats[1].step_key == "C"
        assert step_stats[1].status == StepEventStatus.SKIPPED
        assert step_stats[1].end_time > step_stats[1].start_time
        assert step_stats[1].attempts == 1
        assert len(step_stats[1].attempts_list) == 1

        assert step_stats[2].step_key == "D"
        assert step_stats[2].status == StepEventStatus.IN_PROGRESS
        assert not step_stats[2].end_time
        assert step_stats[2].attempts == 1
        assert len(step_stats[2].attempts_list) == 1

        assert step_stats[3].step_key == "E"
        assert step_stats[3].status == StepEventStatus.IN_PROGRESS
        assert not step_stats[3].end_time
        assert step_stats[3].attempts == 2
        assert len(step_stats[3].attempts_list) == 2

    def test_run_step_stats_with_resource_markers(self, storage):
        @solid(required_resource_keys={"foo"})
        def foo_solid():
            pass

        def _pipeline():
            foo_solid()

        events, result = _synthesize_events(_pipeline, check_success=False)
        for event in events:
            storage.store_event(event)

        step_stats = storage.get_step_stats_for_run(result.run_id)
        assert len(step_stats) == 1
        assert step_stats[0].step_key == "foo_solid"
        assert step_stats[0].status == StepEventStatus.SUCCESS
        assert step_stats[0].end_time > step_stats[0].start_time
        assert len(step_stats[0].markers) == 1
        assert step_stats[0].markers[0].end_time >= step_stats[0].markers[0].start_time + 0.1

    @pytest.mark.parametrize(
        "cursor_dt", cursor_datetime_args()
    )  # test both tz-aware and naive datetimes
    def test_get_event_records(self, storage, cursor_dt):
        if isinstance(storage, SqliteEventLogStorage):
            # test sqlite in test_get_event_records_sqlite
            pytest.skip()

        asset_key = AssetKey(["path", "to", "asset_one"])

        @solid
        def materialize_one(_):
            yield AssetMaterialization(
                asset_key=asset_key,
                metadata={
                    "text": "hello",
                    "json": {"hello": "world"},
                    "one_float": 1.0,
                    "one_int": 1,
                },
            )
            yield Output(1)

        def _solids():
            materialize_one()

        events, _ = _synthesize_events(_solids)

        for event in events:
            storage.store_event(event)

        all_records = storage.get_event_records()
        # all logs returned in descending order
        assert all_records
        min_record_num = all_records[-1].storage_id
        max_record_num = min_record_num + len(all_records) - 1
        assert [r[0] for r in all_records] == list(range(max_record_num, min_record_num - 1, -1))
        assert _event_types([all_records[0].event_log_entry]) == [DagsterEventType.PIPELINE_SUCCESS]
        assert _event_types([all_records[-1].event_log_entry]) == [DagsterEventType.PIPELINE_START]

        # after cursor
        def _build_cursor(record_id_cursor, run_cursor_dt):
            if not run_cursor_dt:
                return record_id_cursor
            return RunShardedEventsCursor(id=record_id_cursor, run_updated_after=run_cursor_dt)

        assert not list(
            filter(
                lambda r: r.storage_id <= 2,
                storage.get_event_records(
                    EventRecordsFilter(after_cursor=_build_cursor(2, cursor_dt))
                ),
            )
        )
        assert [
            i.storage_id
            for i in storage.get_event_records(
                EventRecordsFilter(after_cursor=_build_cursor(min_record_num + 2, cursor_dt)),
                ascending=True,
                limit=2,
            )
        ] == [min_record_num + 3, min_record_num + 4]
        assert [
            i.storage_id
            for i in storage.get_event_records(
                EventRecordsFilter(after_cursor=_build_cursor(min_record_num + 2, cursor_dt)),
                ascending=False,
                limit=2,
            )
        ] == [max_record_num, max_record_num - 1]

        filtered_records = storage.get_event_records(
            EventRecordsFilter(event_type=DagsterEventType.PIPELINE_SUCCESS)
        )
        assert _event_types([r.event_log_entry for r in filtered_records]) == [
            DagsterEventType.PIPELINE_SUCCESS
        ]

    def test_get_event_records_sqlite(self, storage):
        if not isinstance(storage, SqliteEventLogStorage):
            pytest.skip()

        asset_key = AssetKey(["path", "to", "asset_one"])

        events = []

        def _append_event(event):
            events.append(event)

        @solid
        def materialize_one(_):
            yield AssetMaterialization(
                asset_key=asset_key,
                metadata={
                    "text": "hello",
                    "json": {"hello": "world"},
                    "one_float": 1.0,
                    "one_int": 1,
                },
            )
            yield Output(1)

        @pipeline(mode_defs=[_mode_def(_append_event)])
        def a_pipe():
            materialize_one()

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            # first run
            execute_run(
                InMemoryPipeline(a_pipe),
                instance.create_run_for_pipeline(
                    a_pipe, run_id="1", run_config={"loggers": {"callback": {}, "console": {}}}
                ),
                instance,
            )

            for event in events:
                storage.store_event(event)

            run_records = instance.get_run_records()
            assert len(run_records) == 1

            # all logs returned in descending order
            all_event_records = storage.get_event_records()
            assert _event_types([all_event_records[0].event_log_entry]) == [
                DagsterEventType.PIPELINE_SUCCESS
            ]
            assert _event_types([all_event_records[-1].event_log_entry]) == [
                DagsterEventType.PIPELINE_START
            ]

            # second run
            events = []
            execute_run(
                InMemoryPipeline(a_pipe),
                instance.create_run_for_pipeline(
                    a_pipe, run_id="2", run_config={"loggers": {"callback": {}, "console": {}}}
                ),
                instance,
            )
            run_records = instance.get_run_records()
            assert len(run_records) == 2
            for event in events:
                storage.store_event(event)

            # third run
            events = []
            execute_run(
                InMemoryPipeline(a_pipe),
                instance.create_run_for_pipeline(
                    a_pipe, run_id="3", run_config={"loggers": {"callback": {}, "console": {}}}
                ),
                instance,
            )
            run_records = instance.get_run_records()
            assert len(run_records) == 3
            for event in events:
                storage.store_event(event)

            update_timestamp = run_records[-1].update_timestamp
            tzaware_dt = pendulum.from_timestamp(datetime_as_float(update_timestamp), tz="UTC")

            # use tz-aware cursor
            filtered_records = storage.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.PIPELINE_SUCCESS,
                    after_cursor=RunShardedEventsCursor(
                        id=0, run_updated_after=tzaware_dt
                    ),  # events after first run
                ),
                ascending=True,
            )
            assert len(filtered_records) == 2
            assert _event_types([r.event_log_entry for r in filtered_records]) == [
                DagsterEventType.PIPELINE_SUCCESS,
                DagsterEventType.PIPELINE_SUCCESS,
            ]
            assert [r.event_log_entry.run_id for r in filtered_records] == ["2", "3"]

            # use tz-naive cursor
            filtered_records = storage.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.PIPELINE_SUCCESS,
                    after_cursor=RunShardedEventsCursor(
                        id=0, run_updated_after=tzaware_dt.naive()
                    ),  # events after first run
                ),
                ascending=True,
            )
            assert len(filtered_records) == 2
            assert _event_types([r.event_log_entry for r in filtered_records]) == [
                DagsterEventType.PIPELINE_SUCCESS,
                DagsterEventType.PIPELINE_SUCCESS,
            ]
            assert [r.event_log_entry.run_id for r in filtered_records] == ["2", "3"]

    def test_watch_exc_recovery(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        # test that an exception in one watch doesn't fail out others

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        err_run_id = make_new_run_id()
        safe_run_id = make_new_run_id()

        class CBException(Exception):
            pass

        def _throw(_):
            raise CBException("problem in watch callback")

        err_events, _ = _synthesize_events(_solids, run_id=err_run_id)
        safe_events, _ = _synthesize_events(_solids, run_id=safe_run_id)

        event_list = []

        storage.watch(err_run_id, -1, _throw)
        storage.watch(safe_run_id, -1, lambda x: event_list.append(x))

        for event in err_events:
            storage.store_event(event)

        storage.end_watch(err_run_id, _throw)

        for event in safe_events:
            storage.store_event(event)

        start = time.time()
        while len(event_list) < len(safe_events) and time.time() - start < TEST_TIMEOUT:
            time.sleep(0.01)

        assert len(event_list) == len(safe_events)
        assert all([isinstance(event, EventLogEntry) for event in event_list])

    # https://github.com/dagster-io/dagster/issues/5127
    @pytest.mark.skip
    def test_watch_unwatch(self, storage):
        if not self.can_watch():
            pytest.skip("storage cannot watch runs")

        # test for dead lock bug

        @solid
        def return_one(_):
            return 1

        def _solids():
            return_one()

        err_run_id = make_new_run_id()
        safe_run_id = make_new_run_id()

        def _unsub(_):
            storage.end_watch(err_run_id, _unsub)

        err_events, _ = _synthesize_events(_solids, run_id=err_run_id)
        safe_events, _ = _synthesize_events(_solids, run_id=safe_run_id)

        event_list = []

        # Direct end_watch emulates behavior of clean up on exception downstream
        # of the subscription in the dagit webserver.
        storage.watch(err_run_id, -1, _unsub)

        # Other active watches should proceed correctly.
        storage.watch(safe_run_id, -1, lambda x: event_list.append(x))

        for event in err_events:
            storage.store_event(event)

        for event in safe_events:
            storage.store_event(event)

        start = time.time()
        while len(event_list) < len(safe_events) and time.time() - start < TEST_TIMEOUT:
            time.sleep(0.01)

        assert len(event_list) == len(safe_events)
        assert all([isinstance(event, EventLogEntry) for event in event_list])

    def test_engine_event_markers(self, storage):
        @solid
        def return_one(_):
            return 1

        @pipeline
        def a_pipe():
            return_one()

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            run_id = make_new_run_id()
            run = instance.create_run_for_pipeline(a_pipe, run_id=run_id)

            instance.report_engine_event(
                "blah blah", run, EngineEventData(marker_start="FOO"), step_key="return_one"
            )
            instance.report_engine_event(
                "blah blah", run, EngineEventData(marker_end="FOO"), step_key="return_one"
            )
            logs = storage.get_logs_for_run(run_id)
            for entry in logs:
                assert entry.step_key == "return_one"

    def test_latest_materializations(self, storage):
        @solid
        def one(_):
            yield AssetMaterialization(AssetKey("a"), tags={"num": str(1)})
            yield AssetMaterialization(AssetKey("b"), tags={"num": str(1)})
            yield AssetMaterialization(AssetKey("c"), tags={"num": str(1)})
            yield AssetMaterialization(AssetKey("d"), tags={"num": str(1)})
            yield AssetObservation(AssetKey("a"), metadata={"foo": "bar"})
            yield Output(1)

        @solid
        def two(_):
            yield AssetMaterialization(AssetKey("b"), tags={"num": str(2)})
            yield AssetMaterialization(AssetKey("c"), tags={"num": str(2)})
            yield Output(2)

        def _event_tags(event):
            assert event.dagster_event_type == DagsterEventType.ASSET_MATERIALIZATION
            return event.dagster_event.step_materialization_data.materialization.tags

        def _fetch_events(storage):
            return storage.get_latest_materialization_events(
                [
                    AssetKey("a"),
                    AssetKey("b"),
                    AssetKey("c"),
                    AssetKey("d"),
                ]
            )

        events, _ = _synthesize_events(lambda: one())
        for event in events:
            storage.store_event(event)

        events_by_key = _fetch_events(storage)
        assert len(events_by_key) == 4
        assert _event_tags(events_by_key[AssetKey("a")])["num"] == "1"
        assert _event_tags(events_by_key[AssetKey("b")])["num"] == "1"
        assert _event_tags(events_by_key[AssetKey("c")])["num"] == "1"
        assert _event_tags(events_by_key[AssetKey("d")])["num"] == "1"

        # wipe 2 of the assets, make sure we respect that
        if self.can_wipe():
            storage.wipe_asset(AssetKey("a"))
            storage.wipe_asset(AssetKey("b"))
            events_by_key = _fetch_events(storage)
            assert events_by_key.get(AssetKey("a")) is None
            assert events_by_key.get(AssetKey("b")) is None
            assert _event_tags(events_by_key[AssetKey("c")])["num"] == "1"
            assert _event_tags(events_by_key[AssetKey("d")])["num"] == "1"

            # rematerialize one of the wiped assets, one of the existing assets
            events, _ = _synthesize_events(lambda: two())
            for event in events:
                storage.store_event(event)

            events_by_key = _fetch_events(storage)
            assert events_by_key.get(AssetKey("a")) is None
            assert _event_tags(events_by_key[AssetKey("b")])["num"] == "2"
            assert _event_tags(events_by_key[AssetKey("c")])["num"] == "2"
            assert _event_tags(events_by_key[AssetKey("d")])["num"] == "1"

        else:
            events, _ = _synthesize_events(lambda: two())
            for event in events:
                storage.store_event(event)
            events_by_key = _fetch_events(storage)
            assert _event_tags(events_by_key[AssetKey("a")])["num"] == "1"
            assert _event_tags(events_by_key[AssetKey("b")])["num"] == "2"
            assert _event_tags(events_by_key[AssetKey("c")])["num"] == "2"
            assert _event_tags(events_by_key[AssetKey("d")])["num"] == "1"

    def test_asset_keys(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: one_solid(), instance=instance)
            events_two, _ = _synthesize_events(lambda: two_solids(), instance=instance)

            for event in events_one + events_two:
                storage.store_event(event)

            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 3
            assert set([asset_key.to_string() for asset_key in asset_keys]) == set(
                ['["asset_1"]', '["asset_2"]', '["path", "to", "asset_3"]']
            )

    def test_has_asset_key(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: one_solid(), instance=instance)
            events_two, _ = _synthesize_events(lambda: two_solids(), instance=instance)

            for event in events_one + events_two:
                storage.store_event(event)

            assert storage.has_asset_key(AssetKey(["path", "to", "asset_3"]))
            assert not storage.has_asset_key(AssetKey(["path", "to", "bogus", "asset"]))

    def test_asset_events(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: one_solid(), instance=instance)
            events_two, _ = _synthesize_events(lambda: two_solids(), instance=instance)

            for event in events_one + events_two:
                storage.store_event(event)

            asset_events = storage.get_asset_events(AssetKey("asset_1"))
            assert len(asset_events) == 2
            for event in asset_events:
                assert isinstance(event, EventLogEntry)
                assert event.is_dagster_event
                assert event.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION
                assert event.dagster_event.asset_key

            asset_events = storage.get_asset_events(AssetKey(["path", "to", "asset_3"]))
            assert len(asset_events) == 1

    def test_asset_events_range(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: one_solid(), instance=instance)
            two_solids_first, _ = _synthesize_events(lambda: two_solids(), instance=instance)
            two_solids_second, _ = _synthesize_events(lambda: two_solids(), instance=instance)

            for event in events_one + two_solids_first + two_solids_second:
                storage.store_event(event)

            # descending
            asset_events = storage.get_asset_events(AssetKey("asset_1"), include_cursor=True)
            assert len(asset_events) == 3
            [id_three, id_two, id_one] = [id for id, event in asset_events]

            after_events = storage.get_asset_events(
                AssetKey("asset_1"), include_cursor=True, after_cursor=id_one
            )
            assert len(after_events) == 2
            assert [id for id, event in after_events] == [id_three, id_two]

            before_events = storage.get_asset_events(
                AssetKey("asset_1"), include_cursor=True, before_cursor=id_three
            )
            assert len(before_events) == 2
            assert [id for id, event in before_events] == [id_two, id_one]

            between_events = storage.get_asset_events(
                AssetKey("asset_1"),
                include_cursor=True,
                before_cursor=id_three,
                after_cursor=id_one,
            )
            assert len(between_events) == 1
            assert [id for id, event in between_events] == [id_two]

            # ascending
            asset_events = storage.get_asset_events(
                AssetKey("asset_1"), include_cursor=True, ascending=True
            )
            assert len(asset_events) == 3
            [id_one, id_two, id_three] = [id for id, event in asset_events]

            after_events = storage.get_asset_events(
                AssetKey("asset_1"), include_cursor=True, after_cursor=id_one, ascending=True
            )
            assert len(after_events) == 2
            assert [id for id, event in after_events] == [id_two, id_three]

            before_events = storage.get_asset_events(
                AssetKey("asset_1"), include_cursor=True, before_cursor=id_three, ascending=True
            )
            assert len(before_events) == 2
            assert [id for id, event in before_events] == [id_one, id_two]

            between_events = storage.get_asset_events(
                AssetKey("asset_1"),
                include_cursor=True,
                before_cursor=id_three,
                after_cursor=id_one,
                ascending=True,
            )
            assert len(between_events) == 1
            assert [id for id, event in between_events] == [id_two]

    def test_asset_run_ids(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            one_run_id = "one"
            two_run_id = "two"
            one_events, _ = _synthesize_events(
                lambda: one_solid(), run_id=one_run_id, instance=instance
            )
            two_events, _ = _synthesize_events(
                lambda: two_solids(), run_id=two_run_id, instance=instance
            )
            for event in one_events + two_events:
                storage.store_event(event)

            run_ids = storage.get_asset_run_ids(AssetKey("asset_1"))
            assert set(run_ids) == set([one_run_id, two_run_id])

    def test_asset_normalization(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            @solid
            def solid_normalization(_):
                yield AssetMaterialization(asset_key="path/to-asset_4")
                yield Output(1)

            events, _ = _synthesize_events(lambda: solid_normalization(), instance=instance)
            for event in events:
                storage.store_event(event)

            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 1
            asset_key = asset_keys[0]
            assert asset_key.to_string() == '["path", "to", "asset_4"]'
            assert asset_key.path == ["path", "to", "asset_4"]

    def test_asset_wipe(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            one_run_id = "one_run_id"
            two_run_id = "two_run_id"
            events_one, _ = _synthesize_events(
                lambda: one_solid(), run_id=one_run_id, instance=instance
            )
            events_two, _ = _synthesize_events(
                lambda: two_solids(), run_id=two_run_id, instance=instance
            )
            for event in events_one + events_two:
                storage.store_event(event)

            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 3
            assert storage.has_asset_key(AssetKey("asset_1"))
            asset_events = storage.get_asset_events(AssetKey("asset_1"))
            assert len(asset_events) == 2
            asset_run_ids = storage.get_asset_run_ids(AssetKey("asset_1"))
            assert set(asset_run_ids) == set([one_run_id, two_run_id])

            log_count = len(storage.get_logs_for_run(one_run_id))
            if self.can_wipe():
                for asset_key in asset_keys:
                    storage.wipe_asset(asset_key)

                asset_keys = storage.all_asset_keys()
                assert len(asset_keys) == 0
                assert not storage.has_asset_key(AssetKey("asset_1"))
                asset_events = storage.get_asset_events(AssetKey("asset_1"))
                assert len(asset_events) == 0
                asset_run_ids = storage.get_asset_run_ids(AssetKey("asset_1"))
                assert set(asset_run_ids) == set()
                assert log_count == len(storage.get_logs_for_run(one_run_id))

                one_run_id = "one_run_id_2"
                events_one, _ = _synthesize_events(
                    lambda: one_solid(), run_id=one_run_id, instance=instance
                )
                for event in events_one:
                    storage.store_event(event)

                asset_keys = storage.all_asset_keys()
                assert len(asset_keys) == 1
                assert storage.has_asset_key(AssetKey("asset_1"))
                asset_events = storage.get_asset_events(AssetKey("asset_1"))
                assert len(asset_events) == 1
                asset_run_ids = storage.get_asset_run_ids(AssetKey("asset_1"))
                assert set(asset_run_ids) == set([one_run_id])

    def test_asset_secondary_index(self, storage):
        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: one_solid(), instance=instance)

            for event in events_one:
                storage.store_event(event)

            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 1
            migrate_asset_key_data(storage)

            two_first_run_id = "first"
            two_second_run_id = "second"
            events_two, _ = _synthesize_events(
                lambda: two_solids(), run_id=two_first_run_id, instance=instance
            )
            events_two_two, _ = _synthesize_events(
                lambda: two_solids(), run_id=two_second_run_id, instance=instance
            )
            for event in events_two + events_two_two:
                storage.store_event(event)

            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 3

            storage.delete_events(two_first_run_id)
            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 3

            storage.delete_events(two_second_run_id)
            asset_keys = storage.all_asset_keys()
            assert len(asset_keys) == 1

    def test_asset_partition_query(self, storage):
        @solid(config_schema={"partition": Field(str, is_required=False)})
        def solid_partitioned(context):
            yield AssetMaterialization(
                asset_key=AssetKey("asset_key"), partition=context.solid_config.get("partition")
            )
            yield Output(1)

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            get_partitioned_config = lambda partition: {
                "solids": {"solid_partitioned": {"config": {"partition": partition}}}
            }

            for partition in [f"partition_{x}" for x in ["a", "a", "b", "c"]]:
                run_events, _ = _synthesize_events(
                    lambda: solid_partitioned(),
                    instance=instance,
                    run_config=get_partitioned_config(partition),
                )
                for event in run_events:
                    storage.store_event(event)

            events = storage.get_asset_events(AssetKey("asset_key"))
            assert len(events) == 4

            events = storage.get_asset_events(
                AssetKey("asset_key"), partitions=["partition_a", "partition_b"]
            )
            assert len(events) == 3

    def test_get_asset_keys(self, storage):
        @op
        def gen_op():
            yield AssetMaterialization(asset_key=AssetKey(["a"]))
            yield AssetMaterialization(asset_key=AssetKey(["c"]))
            yield AssetMaterialization(asset_key=AssetKey(["banana"]))
            yield AssetMaterialization(asset_key=AssetKey(["b", "x"]))
            yield AssetMaterialization(asset_key=AssetKey(["b", "y"]))
            yield AssetMaterialization(asset_key=AssetKey(["b", "z"]))
            yield Output(1)

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events, _ = _synthesize_events(lambda: gen_op(), instance=instance)
            for event in events:
                storage.store_event(event)

            asset_keys = storage.get_asset_keys()
            assert len(asset_keys) == 6
            # should come out sorted
            assert [asset_key.to_string() for asset_key in asset_keys] == [
                '["a"]',
                '["b", "x"]',
                '["b", "y"]',
                '["b", "z"]',
                '["banana"]',
                '["c"]',
            ]

            # pagination fields
            asset_keys = storage.get_asset_keys(cursor='["b", "y"]', limit=1)
            assert len(asset_keys) == 1
            assert asset_keys[0].to_string() == '["b", "z"]'

            # prefix filter
            asset_keys = storage.get_asset_keys(prefix=["b"])
            assert len(asset_keys) == 3
            assert [asset_key.to_string() for asset_key in asset_keys] == [
                '["b", "x"]',
                '["b", "y"]',
                '["b", "z"]',
            ]

    def test_get_materialization_count_by_partition(self, storage):
        a = AssetKey("no_materializations_asset")
        b = AssetKey("no_partitions_asset")
        c = AssetKey("two_partitions_asset")
        d = AssetKey("one_partition_asset")

        @op
        def materialize():
            yield AssetMaterialization(b)
            yield AssetMaterialization(c, partition="a")
            yield Output(None)

        @op
        def materialize_two():
            yield AssetMaterialization(d, partition="x")
            yield AssetMaterialization(c, partition="a")
            yield AssetMaterialization(c, partition="b")
            yield Output(None)

        def _fetch_counts(storage):
            return storage.get_materialization_count_by_partition([c, d])

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: materialize(), instance=instance)
            for event in events_one:
                storage.store_event(event)

            materialization_count_by_key = storage.get_materialization_count_by_partition([a, b, c])

            assert materialization_count_by_key.get(a) == {}
            assert materialization_count_by_key.get(b) == {}
            assert materialization_count_by_key.get(c)["a"] == 1
            assert len(materialization_count_by_key.get(c)) == 1

            events_two, _ = _synthesize_events(lambda: materialize_two(), instance=instance)
            for event in events_two:
                storage.store_event(event)

            materialization_count_by_key = storage.get_materialization_count_by_partition([a, b, c])
            assert materialization_count_by_key.get(c)["a"] == 2
            assert materialization_count_by_key.get(c)["b"] == 1

            # wipe asset, make sure we respect that
            if self.can_wipe():
                storage.wipe_asset(c)
                materialization_count_by_partition = _fetch_counts(storage)
                assert materialization_count_by_partition.get(c) == {}

                # rematerialize wiped asset
                events, _ = _synthesize_events(lambda: materialize_two(), instance=instance)
                for event in events:
                    storage.store_event(event)

                materialization_count_by_partition = _fetch_counts(storage)
                assert materialization_count_by_partition.get(c)["a"] == 1
                assert materialization_count_by_partition.get(d)["x"] == 2

    def test_get_observation(self, storage):
        a = AssetKey(["key_a"])

        @op
        def gen_op():
            yield AssetObservation(asset_key=a, metadata={"foo": "bar"})
            yield Output(1)

        with instance_for_test() as instance:
            if not storage._instance:  # pylint: disable=protected-access
                storage.register_instance(instance)

            events_one, _ = _synthesize_events(lambda: gen_op(), instance=instance)
            for event in events_one:
                storage.store_event(event)

            records = storage.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_OBSERVATION,
                    asset_key=a,
                )
            )

            assert len(records) == 1
