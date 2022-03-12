import sys
import time

import pendulum
import pytest

from dagster._core.host_representation import (
    ExternalRepositoryOrigin,
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    ScheduleInstigatorData,
    TickData,
    TickStatus,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._seven import get_current_datetime_in_utc
from dagster._utils.error import SerializableErrorInfo


class TestScheduleStorage:
    """
    You can extend this class to easily run these set of tests on any schedule storage. When extending,
    you simply need to override the `schedule_storage` fixture and return your implementation of
    `ScheduleStorage`.

    For example:

    ```
    class TestMyStorageImplementation(TestScheduleStorage):
        __test__ = True

        @pytest.fixture(scope='function', name='storage')
        def schedule_storage(self):  # pylint: disable=arguments-differ
            return MyStorageImplementation()
    ```
    """

    __test__ = False

    @pytest.fixture(name="storage", params=[])
    def schedule_storage(self, request):
        with request.param() as s:
            yield s

    # Override this for schedule storages that are not allowed to delete state or ticks
    def can_delete(self):
        return True

    @staticmethod
    def fake_repo_target():
        return ExternalRepositoryOrigin(
            ManagedGrpcPythonEnvRepositoryLocationOrigin(
                LoadableTargetOrigin(
                    executable_path=sys.executable, module_name="fake", attribute="fake"
                ),
            ),
            "fake_repo_name",
        )

    @classmethod
    def build_schedule(
        cls,
        schedule_name,
        cron_schedule,
        status=InstigatorStatus.STOPPED,
    ):
        return InstigatorState(
            cls.fake_repo_target().get_instigator_origin(schedule_name),
            InstigatorType.SCHEDULE,
            status,
            ScheduleInstigatorData(cron_schedule, start_timestamp=None),
        )

    @classmethod
    def build_sensor(cls, sensor_name, status=InstigatorStatus.STOPPED):
        origin = cls.fake_repo_target().get_instigator_origin(sensor_name)
        return InstigatorState(origin, InstigatorType.SENSOR, status)

    def test_basic_schedule_storage(self, storage):
        assert storage

        schedule = self.build_schedule("my_schedule", "* * * * *")
        storage.add_instigator_state(schedule)
        schedules = storage.all_instigator_state(
            self.fake_repo_target().get_id(),
            InstigatorType.SCHEDULE,
        )
        assert len(schedules) == 1

        schedule = schedules[0]
        assert schedule.instigator_name == "my_schedule"
        assert schedule.instigator_data.cron_schedule == "* * * * *"
        assert schedule.instigator_data.start_timestamp == None

    def test_add_multiple_schedules(self, storage):
        assert storage

        schedule = self.build_schedule("my_schedule", "* * * * *")
        schedule_2 = self.build_schedule("my_schedule_2", "* * * * *")
        schedule_3 = self.build_schedule("my_schedule_3", "* * * * *")

        storage.add_instigator_state(schedule)
        storage.add_instigator_state(schedule_2)
        storage.add_instigator_state(schedule_3)

        schedules = storage.all_instigator_state(
            self.fake_repo_target().get_id(), InstigatorType.SCHEDULE
        )
        assert len(schedules) == 3

        assert any(s.instigator_name == "my_schedule" for s in schedules)
        assert any(s.instigator_name == "my_schedule_2" for s in schedules)
        assert any(s.instigator_name == "my_schedule_3" for s in schedules)

    def test_get_schedule_state(self, storage):
        assert storage

        state = self.build_schedule("my_schedule", "* * * * *")
        storage.add_instigator_state(state)
        schedule = storage.get_instigator_state(state.instigator_origin_id)

        assert schedule.instigator_name == "my_schedule"
        assert schedule.instigator_data.start_timestamp == None

    def test_get_schedule_state_not_found(self, storage):
        assert storage

        storage.add_instigator_state(self.build_schedule("my_schedule", "* * * * *"))
        schedule = storage.get_instigator_state("fake_id")

        assert schedule is None

    def test_update_schedule(self, storage):
        assert storage

        schedule = self.build_schedule("my_schedule", "* * * * *")
        storage.add_instigator_state(schedule)

        now_time = get_current_datetime_in_utc().timestamp()

        new_schedule = schedule.with_status(InstigatorStatus.RUNNING).with_data(
            ScheduleInstigatorData(
                cron_schedule=schedule.instigator_data.cron_schedule,
                start_timestamp=now_time,
            )
        )
        storage.update_instigator_state(new_schedule)

        schedules = storage.all_instigator_state(
            self.fake_repo_target().get_id(), InstigatorType.SCHEDULE
        )
        assert len(schedules) == 1

        schedule = schedules[0]
        assert schedule.instigator_name == "my_schedule"
        assert schedule.status == InstigatorStatus.RUNNING
        assert schedule.instigator_data.start_timestamp == now_time

        stopped_schedule = schedule.with_status(InstigatorStatus.STOPPED).with_data(
            ScheduleInstigatorData(schedule.instigator_data.cron_schedule)
        )
        storage.update_instigator_state(stopped_schedule)

        schedules = storage.all_instigator_state(
            self.fake_repo_target().get_id(), InstigatorType.SCHEDULE
        )
        assert len(schedules) == 1

        schedule = schedules[0]
        assert schedule.instigator_name == "my_schedule"
        assert schedule.status == InstigatorStatus.STOPPED
        assert schedule.instigator_data.start_timestamp == None

    def test_update_schedule_not_found(self, storage):
        assert storage

        schedule = self.build_schedule("my_schedule", "* * * * *")

        with pytest.raises(Exception):
            storage.update_instigator_state(schedule)

    def test_delete_schedule_state(self, storage):
        assert storage

        if not self.can_delete():
            pytest.skip("Storage cannot delete")

        schedule = self.build_schedule("my_schedule", "* * * * *")
        storage.add_instigator_state(schedule)
        storage.delete_instigator_state(schedule.instigator_origin_id)

        schedules = storage.all_instigator_state(
            self.fake_repo_target().get_id(), InstigatorType.SCHEDULE
        )
        assert len(schedules) == 0

    def test_delete_schedule_not_found(self, storage):
        assert storage

        if not self.can_delete():
            pytest.skip("Storage cannot delete")

        schedule = self.build_schedule("my_schedule", "* * * * *")

        with pytest.raises(Exception):
            storage.delete_instigator_state(schedule.instigator_origin_id)

    def test_add_schedule_with_same_name(self, storage):
        assert storage

        schedule = self.build_schedule("my_schedule", "* * * * *")
        storage.add_instigator_state(schedule)

        with pytest.raises(Exception):
            storage.add_instigator_state(schedule)

    def build_schedule_tick(self, current_time, status=TickStatus.STARTED, run_id=None, error=None):
        return TickData(
            "my_schedule",
            "my_schedule",
            InstigatorType.SCHEDULE,
            status,
            current_time,
            [run_id] if run_id else [],
            [],
            error,
        )

    def test_create_tick(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_schedule_tick(current_time))
        ticks = storage.get_ticks("my_schedule")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.instigator_name == "my_schedule"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.STARTED
        assert tick.run_ids == []
        assert tick.error == None

    def test_update_tick_to_success(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_schedule_tick(current_time))

        updated_tick = tick.with_status(TickStatus.SUCCESS).with_run(run_id="1234")
        assert updated_tick.status == TickStatus.SUCCESS

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_schedule")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.instigator_name == "my_schedule"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.SUCCESS
        assert tick.run_ids == ["1234"]
        assert tick.error == None

    def test_update_tick_to_skip(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_schedule_tick(current_time))

        updated_tick = tick.with_status(TickStatus.SKIPPED)
        assert updated_tick.status == TickStatus.SKIPPED

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_schedule")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.instigator_name == "my_schedule"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.SKIPPED
        assert tick.run_ids == []
        assert tick.error == None

    def test_update_tick_to_failure(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_schedule_tick(current_time))

        updated_tick = tick.with_status(
            TickStatus.FAILURE,
            error=SerializableErrorInfo(message="Error", stack=[], cls_name="TestError"),
        )
        assert updated_tick.status == TickStatus.FAILURE

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_schedule")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.tick_id > 0
        assert tick.instigator_name == "my_schedule"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.FAILURE
        assert tick.run_ids == []
        assert tick.error == SerializableErrorInfo(message="Error", stack=[], cls_name="TestError")

    def test_get_tick_stats(self, storage):
        assert storage

        current_time = time.time()

        error = SerializableErrorInfo(message="Error", stack=[], cls_name="TestError")

        # Create ticks
        for x in range(2):
            storage.create_tick(self.build_schedule_tick(current_time))

        for x in range(3):
            storage.create_tick(
                self.build_schedule_tick(current_time, TickStatus.SUCCESS, run_id=str(x)),
            )

        for x in range(4):
            storage.create_tick(
                self.build_schedule_tick(current_time, TickStatus.SKIPPED),
            )

        for x in range(5):
            storage.create_tick(
                self.build_schedule_tick(current_time, TickStatus.FAILURE, error=error),
            )

        stats = storage.get_tick_stats("my_schedule")
        assert stats.ticks_started == 2
        assert stats.ticks_succeeded == 3
        assert stats.ticks_skipped == 4
        assert stats.ticks_failed == 5

    def test_basic_storage(self, storage):
        assert storage
        sensor_state = self.build_sensor("my_sensor")
        storage.add_instigator_state(sensor_state)
        states = storage.all_instigator_state(self.fake_repo_target().get_id())
        assert len(states) == 1

        state = states[0]
        assert state.instigator_name == "my_sensor"

    def test_add_multiple_states(self, storage):
        assert storage

        state = self.build_sensor("my_sensor")
        state_2 = self.build_sensor("my_sensor_2")
        state_3 = self.build_sensor("my_sensor_3")

        storage.add_instigator_state(state)
        storage.add_instigator_state(state_2)
        storage.add_instigator_state(state_3)

        states = storage.all_instigator_state(self.fake_repo_target().get_id())
        assert len(states) == 3

        assert any(s.instigator_name == "my_sensor" for s in states)
        assert any(s.instigator_name == "my_sensor_2" for s in states)
        assert any(s.instigator_name == "my_sensor_3" for s in states)

    def test_get_instigator_state(self, storage):
        assert storage

        state = self.build_sensor("my_sensor")
        storage.add_instigator_state(state)
        state = storage.get_instigator_state(state.instigator_origin_id)

        assert state.instigator_name == "my_sensor"

    def test_get_instigator_state_not_found(self, storage):
        assert storage

        storage.add_instigator_state(self.build_sensor("my_sensor"))
        state = storage.get_instigator_state("fake_id")
        assert state is None

    def test_update_state(self, storage):
        assert storage

        state = self.build_sensor("my_sensor")
        storage.add_instigator_state(state)

        new_state = state.with_status(InstigatorStatus.RUNNING)
        storage.update_instigator_state(new_state)

        states = storage.all_instigator_state(self.fake_repo_target().get_id())
        assert len(states) == 1

        state = states[0]
        assert state.instigator_name == "my_sensor"
        assert state.status == InstigatorStatus.RUNNING

        stopped_state = state.with_status(InstigatorStatus.STOPPED)
        storage.update_instigator_state(stopped_state)

        states = storage.all_instigator_state(self.fake_repo_target().get_id())
        assert len(states) == 1

        state = states[0]
        assert state.instigator_name == "my_sensor"
        assert state.status == InstigatorStatus.STOPPED

    def test_update_state_not_found(self, storage):
        assert storage

        state = self.build_sensor("my_sensor")

        with pytest.raises(Exception):
            storage.update_instigator_state(state)

    def test_delete_instigator_state(self, storage):
        assert storage

        if not self.can_delete():
            pytest.skip("Storage cannot delete")

        state = self.build_sensor("my_sensor")
        storage.add_instigator_state(state)
        storage.delete_instigator_state(state.instigator_origin_id)

        states = storage.all_instigator_state(self.fake_repo_target().get_id())
        assert len(states) == 0

    def test_delete_state_not_found(self, storage):
        assert storage

        if not self.can_delete():
            pytest.skip("Storage cannot delete")

        state = self.build_sensor("my_sensor")

        with pytest.raises(Exception):
            storage.delete_instigator_state(state.instigator_origin_id)

    def test_add_state_with_same_name(self, storage):
        assert storage

        state = self.build_sensor("my_sensor")
        storage.add_instigator_state(state)

        with pytest.raises(Exception):
            storage.add_instigator_state(state)

    def build_sensor_tick(self, current_time, status=TickStatus.STARTED, run_id=None, error=None):
        return TickData(
            "my_sensor",
            "my_sensor",
            InstigatorType.SENSOR,
            status,
            current_time,
            [run_id] if run_id else [],
            error=error,
        )

    def test_create_sensor_tick(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_sensor_tick(current_time))
        tick_id = tick.tick_id

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.tick_id == tick_id
        assert tick.instigator_name == "my_sensor"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.STARTED
        assert tick.run_ids == []
        assert tick.error == None

    def test_get_sensor_tick(self, storage):
        assert storage
        now = pendulum.now()
        five_days_ago = now.subtract(days=5).timestamp()
        four_days_ago = now.subtract(days=4).timestamp()
        one_day_ago = now.subtract(days=1).timestamp()

        storage.create_tick(self.build_sensor_tick(five_days_ago, TickStatus.SKIPPED))
        storage.create_tick(self.build_sensor_tick(four_days_ago, TickStatus.SKIPPED))
        storage.create_tick(self.build_sensor_tick(one_day_ago, TickStatus.SKIPPED))
        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 3

        ticks = storage.get_ticks("my_sensor", after=five_days_ago + 1)
        assert len(ticks) == 2
        ticks = storage.get_ticks("my_sensor", before=one_day_ago - 1)
        assert len(ticks) == 2
        ticks = storage.get_ticks("my_sensor", after=five_days_ago + 1, before=one_day_ago - 1)
        assert len(ticks) == 1

    def test_update_sensor_tick_to_success(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_sensor_tick(current_time))

        updated_tick = tick.with_status(TickStatus.SUCCESS).with_run(run_id="1234")
        assert updated_tick.status == TickStatus.SUCCESS

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.tick_id > 0
        assert tick.instigator_name == "my_sensor"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.SUCCESS
        assert tick.run_ids == ["1234"]
        assert tick.error == None

    def test_update_sensor_tick_to_skip(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_sensor_tick(current_time))

        updated_tick = tick.with_status(TickStatus.SKIPPED)
        assert updated_tick.status == TickStatus.SKIPPED

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.tick_id > 0
        assert tick.instigator_name == "my_sensor"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.SKIPPED
        assert tick.run_ids == []
        assert tick.error == None

    def test_update_sensor_tick_to_failure(self, storage):
        assert storage

        current_time = time.time()
        tick = storage.create_tick(self.build_sensor_tick(current_time))
        error = SerializableErrorInfo(message="Error", stack=[], cls_name="TestError")

        updated_tick = tick.with_status(TickStatus.FAILURE, error=error)
        assert updated_tick.status == TickStatus.FAILURE

        storage.update_tick(updated_tick)

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.tick_id > 0
        assert tick.instigator_name == "my_sensor"
        assert tick.timestamp == current_time
        assert tick.status == TickStatus.FAILURE
        assert tick.run_ids == []
        assert tick.error == error

    def test_purge_ticks(self, storage):
        assert storage

        now = pendulum.now()
        five_minutes_ago = now.subtract(minutes=5).timestamp()
        four_minutes_ago = now.subtract(minutes=4).timestamp()
        one_minute_ago = now.subtract(minutes=1).timestamp()
        storage.create_tick(self.build_sensor_tick(five_minutes_ago, TickStatus.SKIPPED))
        storage.create_tick(
            self.build_sensor_tick(four_minutes_ago, TickStatus.SUCCESS, run_id="fake_run_id")
        )
        one_minute_tick = storage.create_tick(
            self.build_sensor_tick(one_minute_ago, TickStatus.SKIPPED)
        )
        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 3

        latest_tick = ticks[0]
        assert latest_tick.tick_id == one_minute_tick.tick_id

        storage.purge_ticks("my_sensor", TickStatus.SKIPPED, now.subtract(minutes=2).timestamp())

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 2

    def test_ticks_filtered(self, storage):
        storage.create_tick(self.build_sensor_tick(time.time(), status=TickStatus.STARTED))
        storage.create_tick(self.build_sensor_tick(time.time(), status=TickStatus.SUCCESS))
        storage.create_tick(self.build_sensor_tick(time.time(), status=TickStatus.SKIPPED))
        storage.create_tick(
            self.build_sensor_tick(
                time.time(),
                status=TickStatus.FAILURE,
                error=SerializableErrorInfo(message="foobar", stack=[], cls_name=None, cause=None),
            )
        )

        ticks = storage.get_ticks("my_sensor")
        assert len(ticks) == 4

        started = storage.get_ticks("my_sensor", statuses=[TickStatus.STARTED])
        assert len(started) == 1

        successes = storage.get_ticks("my_sensor", statuses=[TickStatus.SUCCESS])
        assert len(successes) == 1

        skips = storage.get_ticks("my_sensor", statuses=[TickStatus.SKIPPED])
        assert len(skips) == 1

        failures = storage.get_ticks("my_sensor", statuses=[TickStatus.FAILURE])
        assert len(failures) == 1

        # everything but skips
        non_skips = storage.get_ticks(
            "my_sensor", statuses=[TickStatus.STARTED, TickStatus.SUCCESS, TickStatus.FAILURE]
        )
        assert len(non_skips) == 3
