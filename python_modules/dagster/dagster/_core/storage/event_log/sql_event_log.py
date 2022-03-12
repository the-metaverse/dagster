import logging
from abc import abstractmethod
from collections import OrderedDict
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, cast

import pendulum
import sqlalchemy as db

from dagster import check, seven
from dagster._core.assets import AssetDetails
from dagster._core.definitions.events import AssetKey, AssetMaterialization
from dagster._core.errors import DagsterEventLogInvalidForRun
from dagster._core.events import DagsterEventType
from dagster._core.events.log import EventLogEntry
from dagster._core.execution.stats import build_run_step_stats_from_events
from dagster.serdes import deserialize_json_to_dagster_namedtuple, serialize_dagster_namedtuple
from dagster.serdes.errors import DeserializationError
from dagster.utils import datetime_as_float, utc_datetime_from_naive, utc_datetime_from_timestamp

from ..pipeline_run import PipelineRunStatsSnapshot
from .base import (
    EventLogRecord,
    EventLogStorage,
    EventRecordsFilter,
    RunShardedEventsCursor,
    extract_asset_events_cursor,
)
from .migration import ASSET_DATA_MIGRATIONS, ASSET_KEY_INDEX_COLS, EVENT_LOG_DATA_MIGRATIONS
from .schema import AssetKeyTable, SecondaryIndexMigrationTable, SqlEventLogStorageTable

MIN_ASSET_ROWS = 25


class SqlEventLogStorage(EventLogStorage):
    """Base class for SQL backed event log storages.

    Distinguishes between run-based connections and index connections in order to support run-level
    sharding, while maintaining the ability to do cross-run queries
    """

    @abstractmethod
    def run_connection(self, run_id):
        """Context manager yielding a connection to access the event logs for a specific run.

        Args:
            run_id (Optional[str]): Enables those storages which shard based on run_id, e.g.,
                SqliteEventLogStorage, to connect appropriately.
        """

    @abstractmethod
    def index_connection(self):
        """Context manager yielding a connection to access cross-run indexed tables.

        Args:
            run_id (Optional[str]): Enables those storages which shard based on run_id, e.g.,
                SqliteEventLogStorage, to connect appropriately.
        """

    @abstractmethod
    def upgrade(self):
        """This method should perform any schema migrations necessary to bring an
        out-of-date instance of the storage up to date.
        """

    def prepare_insert_event(self, event):
        """Helper method for preparing the event log SQL insertion statement.  Abstracted away to
        have a single place for the logical table representation of the event, while having a way
        for SQL backends to implement different execution implementations for `store_event`. See
        the `dagster-postgres` implementation which overrides the generic SQL implementation of
        `store_event`.
        """

        dagster_event_type = None
        asset_key_str = None
        partition = None
        step_key = event.step_key

        if event.is_dagster_event:
            dagster_event_type = event.dagster_event.event_type_value
            step_key = event.dagster_event.step_key
            if event.dagster_event.asset_key:
                check.inst_param(event.dagster_event.asset_key, "asset_key", AssetKey)
                asset_key_str = event.dagster_event.asset_key.to_string()
            if event.dagster_event.partition:
                partition = event.dagster_event.partition

        # https://stackoverflow.com/a/54386260/324449
        return SqlEventLogStorageTable.insert().values(  # pylint: disable=no-value-for-parameter
            run_id=event.run_id,
            event=serialize_dagster_namedtuple(event),
            dagster_event_type=dagster_event_type,
            # Postgres requires a datetime that is in UTC but has no timezone info set
            # in order to be stored correctly
            timestamp=datetime.utcfromtimestamp(event.timestamp),
            step_key=step_key,
            asset_key=asset_key_str,
            partition=partition,
        )

    def has_asset_key_index_cols(self):
        with self.index_connection() as conn:
            column_names = [x.get("name") for x in db.inspect(conn).get_columns(AssetKeyTable.name)]
            return "last_materialization_timestamp" in column_names

    def store_asset(self, event):
        check.inst_param(event, "event", EventLogEntry)
        if not event.is_dagster_event or not event.dagster_event.asset_key:
            return

        materialization = event.dagster_event.step_materialization_data.materialization
        # We switched to storing the entire event record of the last materialization instead of just
        # the AssetMaterialization object, so that we have access to metadata like timestamp,
        # pipeline, run_id, etc.
        #
        # This should make certain asset queries way more performant, without having to do extra
        # queries against the event log.
        #
        # This should be accompanied by a schema change in 0.12.0, renaming `last_materialization`
        # to `last_materialization_event`, for clarity.  For now, we should do some back-compat.
        #
        # https://github.com/dagster-io/dagster/issues/3945
        if self.has_asset_key_index_cols():
            insert_statement = (
                AssetKeyTable.insert().values(  # pylint: disable=no-value-for-parameter
                    asset_key=event.dagster_event.asset_key.to_string(),
                    last_materialization=serialize_dagster_namedtuple(event),
                    last_materialization_timestamp=utc_datetime_from_timestamp(event.timestamp),
                    last_run_id=event.run_id,
                    tags=seven.json.dumps(materialization.tags) if materialization.tags else None,
                )
            )
            update_statement = (
                AssetKeyTable.update()
                .values(  # pylint: disable=no-value-for-parameter
                    last_materialization=serialize_dagster_namedtuple(event),
                    last_materialization_timestamp=utc_datetime_from_timestamp(event.timestamp),
                    last_run_id=event.run_id,
                    tags=seven.json.dumps(materialization.tags) if materialization.tags else None,
                )
                .where(
                    AssetKeyTable.c.asset_key == event.dagster_event.asset_key.to_string(),
                )
            )
        else:
            insert_statement = (
                AssetKeyTable.insert().values(  # pylint: disable=no-value-for-parameter
                    asset_key=event.dagster_event.asset_key.to_string(),
                    last_materialization=serialize_dagster_namedtuple(event),
                    last_run_id=event.run_id,
                )
            )
            update_statement = (
                AssetKeyTable.update()
                .values(  # pylint: disable=no-value-for-parameter
                    last_materialization=serialize_dagster_namedtuple(event),
                    last_run_id=event.run_id,
                )
                .where(
                    AssetKeyTable.c.asset_key == event.dagster_event.asset_key.to_string(),
                )
            )

        with self.index_connection() as conn:
            try:
                conn.execute(insert_statement)
            except db.exc.IntegrityError:
                conn.execute(update_statement)

    def store_event(self, event):
        """Store an event corresponding to a pipeline run.

        Args:
            event (EventLogEntry): The event to store.
        """
        check.inst_param(event, "event", EventLogEntry)
        insert_event_statement = self.prepare_insert_event(event)
        run_id = event.run_id

        with self.run_connection(run_id) as conn:
            conn.execute(insert_event_statement)

        if (
            event.is_dagster_event
            and event.dagster_event.is_step_materialization
            and event.dagster_event.asset_key
        ):
            # Currently, only materializations are stored in the asset catalog.
            # We will store observations after adding a column migration to
            # store latest asset observation timestamp in the asset key table.
            self.store_asset(event)

    def get_logs_for_run_by_log_id(
        self,
        run_id,
        cursor=-1,
        dagster_event_type=None,
        limit=None,
    ):
        check.str_param(run_id, "run_id")
        check.int_param(cursor, "cursor")
        check.invariant(
            cursor >= -1,
            "Don't know what to do with negative cursor {cursor}".format(cursor=cursor),
        )

        dagster_event_types = (
            {dagster_event_type}
            if isinstance(dagster_event_type, DagsterEventType)
            else check.opt_set_param(
                dagster_event_type, "dagster_event_type", of_type=DagsterEventType
            )
        )

        query = (
            db.select([SqlEventLogStorageTable.c.id, SqlEventLogStorageTable.c.event])
            .where(SqlEventLogStorageTable.c.run_id == run_id)
            .order_by(SqlEventLogStorageTable.c.id.asc())
        )
        if dagster_event_types:
            query = query.where(
                SqlEventLogStorageTable.c.dagster_event_type.in_(
                    [dagster_event_type.value for dagster_event_type in dagster_event_types]
                )
            )

        # adjust 0 based index cursor to SQL offset
        query = query.offset(cursor + 1)

        if limit:
            query = query.limit(limit)

        with self.run_connection(run_id) as conn:
            results = conn.execute(query).fetchall()

        events = {}
        try:
            for (
                record_id,
                json_str,
            ) in results:
                events[record_id] = check.inst_param(
                    deserialize_json_to_dagster_namedtuple(json_str), "event", EventLogEntry
                )
        except (seven.JSONDecodeError, DeserializationError) as err:
            raise DagsterEventLogInvalidForRun(run_id=run_id) from err

        return events

    def get_logs_for_run(
        self,
        run_id,
        cursor=-1,
        of_type=None,
        limit=None,
    ):
        """Get all of the logs corresponding to a run.

        Args:
            run_id (str): The id of the run for which to fetch logs.
            cursor (Optional[int]): Zero-indexed logs will be returned starting from cursor + 1,
                i.e., if cursor is -1, all logs will be returned. (default: -1)
            of_type (Optional[DagsterEventType]): the dagster event type to filter the logs.
            limit (Optional[int]): the maximum number of events to fetch
        """
        check.str_param(run_id, "run_id")
        check.int_param(cursor, "cursor")
        check.invariant(
            cursor >= -1,
            "Don't know what to do with negative cursor {cursor}".format(cursor=cursor),
        )

        check.invariant(
            not of_type
            or isinstance(of_type, DagsterEventType)
            or isinstance(of_type, (frozenset, set))
        )

        events_by_id = self.get_logs_for_run_by_log_id(run_id, cursor, of_type, limit)
        return [event for id, event in sorted(events_by_id.items(), key=lambda x: x[0])]

    def get_stats_for_run(self, run_id):
        check.str_param(run_id, "run_id")

        query = (
            db.select(
                [
                    SqlEventLogStorageTable.c.dagster_event_type,
                    db.func.count().label("n_events_of_type"),
                    db.func.max(SqlEventLogStorageTable.c.timestamp).label("last_event_timestamp"),
                ]
            )
            .where(
                db.and_(
                    SqlEventLogStorageTable.c.run_id == run_id,
                    SqlEventLogStorageTable.c.dagster_event_type != None,
                )
            )
            .group_by("dagster_event_type")
        )

        with self.run_connection(run_id) as conn:
            results = conn.execute(query).fetchall()

        try:
            counts = {}
            times = {}
            for result in results:
                (dagster_event_type, n_events_of_type, last_event_timestamp) = result
                check.invariant(dagster_event_type is not None)
                counts[dagster_event_type] = n_events_of_type
                times[dagster_event_type] = last_event_timestamp

            enqueued_time = times.get(DagsterEventType.PIPELINE_ENQUEUED.value, None)
            launch_time = times.get(DagsterEventType.PIPELINE_STARTING.value, None)
            start_time = times.get(DagsterEventType.PIPELINE_START.value, None)
            end_time = times.get(
                DagsterEventType.PIPELINE_SUCCESS.value,
                times.get(
                    DagsterEventType.PIPELINE_FAILURE.value,
                    times.get(DagsterEventType.PIPELINE_CANCELED.value, None),
                ),
            )

            return PipelineRunStatsSnapshot(
                run_id=run_id,
                steps_succeeded=counts.get(DagsterEventType.STEP_SUCCESS.value, 0),
                steps_failed=counts.get(DagsterEventType.STEP_FAILURE.value, 0),
                materializations=counts.get(DagsterEventType.ASSET_MATERIALIZATION.value, 0),
                expectations=counts.get(DagsterEventType.STEP_EXPECTATION_RESULT.value, 0),
                enqueued_time=datetime_as_float(enqueued_time) if enqueued_time else None,
                launch_time=datetime_as_float(launch_time) if launch_time else None,
                start_time=datetime_as_float(start_time) if start_time else None,
                end_time=datetime_as_float(end_time) if end_time else None,
            )
        except (seven.JSONDecodeError, DeserializationError) as err:
            raise DagsterEventLogInvalidForRun(run_id=run_id) from err

    def get_step_stats_for_run(self, run_id, step_keys=None):
        check.str_param(run_id, "run_id")
        check.opt_list_param(step_keys, "step_keys", of_type=str)

        # Originally, this was two different queries:
        # 1) one query which aggregated top-level step stats by grouping by event type / step_key in
        #    a single query, using pure SQL (e.g. start_time, end_time, status, attempt counts).
        # 2) one query which fetched all the raw events for a specific event type and then inspected
        #    the deserialized event object to aggregate stats derived from sequences of events.
        #    (e.g. marker events, materializations, expectations resuls, attempts timing, etc.)
        #
        # For simplicity, we now just do the second type of query and derive the stats in Python
        # from the raw events.  This has the benefit of being easier to read and also the benefit of
        # being able to share code with the in-memory event log storage implementation.  We may
        # choose to revisit this in the future, especially if we are able to do JSON-column queries
        # in SQL as a way of bypassing the serdes layer in all cases.
        raw_event_query = (
            db.select([SqlEventLogStorageTable.c.event])
            .where(SqlEventLogStorageTable.c.run_id == run_id)
            .where(SqlEventLogStorageTable.c.step_key != None)
            .where(
                SqlEventLogStorageTable.c.dagster_event_type.in_(
                    [
                        DagsterEventType.STEP_START.value,
                        DagsterEventType.STEP_SUCCESS.value,
                        DagsterEventType.STEP_SKIPPED.value,
                        DagsterEventType.STEP_FAILURE.value,
                        DagsterEventType.STEP_RESTARTED.value,
                        DagsterEventType.ASSET_MATERIALIZATION.value,
                        DagsterEventType.STEP_EXPECTATION_RESULT.value,
                        DagsterEventType.STEP_RESTARTED.value,
                        DagsterEventType.STEP_UP_FOR_RETRY.value,
                        DagsterEventType.ENGINE_EVENT.value,
                    ]
                )
            )
            .order_by(SqlEventLogStorageTable.c.id.asc())
        )
        if step_keys:
            raw_event_query = raw_event_query.where(
                SqlEventLogStorageTable.c.step_key.in_(step_keys)
            )

        with self.run_connection(run_id) as conn:
            results = conn.execute(raw_event_query).fetchall()

        try:
            records = [
                check.inst_param(
                    deserialize_json_to_dagster_namedtuple(json_str), "event", EventLogEntry
                )
                for (json_str,) in results
            ]
            return build_run_step_stats_from_events(run_id, records)
        except (seven.JSONDecodeError, DeserializationError) as err:
            raise DagsterEventLogInvalidForRun(run_id=run_id) from err

    def _apply_migration(self, migration_name, migration_fn, print_fn, force):
        if self.has_secondary_index(migration_name):
            if not force:
                if print_fn:
                    print_fn("Skipping already reindexed summary: {}".format(migration_name))
                return
        if print_fn:
            print_fn("Starting reindex: {}".format(migration_name))
        migration_fn()(self, print_fn)
        self.enable_secondary_index(migration_name)
        if print_fn:
            print_fn("Finished reindexing: {}".format(migration_name))

    def reindex_events(self, print_fn=None, force=False):
        """Call this method to run any data migrations across the event_log table"""
        for migration_name, migration_fn in EVENT_LOG_DATA_MIGRATIONS.items():
            self._apply_migration(migration_name, migration_fn, print_fn, force)

    def reindex_assets(self, print_fn=None, force=False):
        """Call this method to run any data migrations across the asset_keys table"""
        for migration_name, migration_fn in ASSET_DATA_MIGRATIONS.items():
            self._apply_migration(migration_name, migration_fn, print_fn, force)

    def wipe(self):
        """Clears the event log storage."""
        # Should be overridden by SqliteEventLogStorage and other storages that shard based on
        # run_id

        # https://stackoverflow.com/a/54386260/324449
        with self.run_connection(run_id=None) as conn:
            conn.execute(SqlEventLogStorageTable.delete())  # pylint: disable=no-value-for-parameter
            conn.execute(AssetKeyTable.delete())  # pylint: disable=no-value-for-parameter

        with self.index_connection() as conn:
            conn.execute(SqlEventLogStorageTable.delete())  # pylint: disable=no-value-for-parameter
            conn.execute(AssetKeyTable.delete())  # pylint: disable=no-value-for-parameter

    def delete_events(self, run_id):
        with self.run_connection(run_id) as conn:
            self.delete_events_for_run(conn, run_id)

    def delete_events_for_run(self, conn, run_id):
        check.str_param(run_id, "run_id")

        delete_statement = (
            SqlEventLogStorageTable.delete().where(  # pylint: disable=no-value-for-parameter
                SqlEventLogStorageTable.c.run_id == run_id
            )
        )
        removed_asset_key_query = (
            db.select([SqlEventLogStorageTable.c.asset_key])
            .where(SqlEventLogStorageTable.c.run_id == run_id)
            .where(SqlEventLogStorageTable.c.asset_key != None)
            .group_by(SqlEventLogStorageTable.c.asset_key)
        )

        removed_asset_keys = [
            AssetKey.from_db_string(row[0])
            for row in conn.execute(removed_asset_key_query).fetchall()
        ]
        conn.execute(delete_statement)
        if len(removed_asset_keys) > 0:
            keys_to_check = []
            keys_to_check.extend([key.to_string() for key in removed_asset_keys])
            keys_to_check.extend([key.to_string(legacy=True) for key in removed_asset_keys])
            remaining_asset_keys = [
                AssetKey.from_db_string(row[0])
                for row in conn.execute(
                    db.select([SqlEventLogStorageTable.c.asset_key])
                    .where(SqlEventLogStorageTable.c.asset_key.in_(keys_to_check))
                    .group_by(SqlEventLogStorageTable.c.asset_key)
                )
            ]
            to_remove = set(removed_asset_keys) - set(remaining_asset_keys)
            if to_remove:
                keys_to_remove = []
                keys_to_remove.extend([key.to_string() for key in to_remove])
                keys_to_remove.extend([key.to_string(legacy=True) for key in to_remove])
                conn.execute(
                    AssetKeyTable.delete().where(  # pylint: disable=no-value-for-parameter
                        AssetKeyTable.c.asset_key.in_(keys_to_remove)
                    )
                )

    @property
    def is_persistent(self):
        return True

    def update_event_log_record(self, record_id, event):
        """Utility method for migration scripts to update SQL representation of event records."""
        check.int_param(record_id, "record_id")
        check.inst_param(event, "event", EventLogEntry)
        dagster_event_type = None
        asset_key_str = None
        if event.is_dagster_event:
            dagster_event_type = event.dagster_event.event_type_value
            if event.dagster_event.asset_key:
                check.inst_param(event.dagster_event.asset_key, "asset_key", AssetKey)
                asset_key_str = event.dagster_event.asset_key.to_string()

        with self.run_connection(run_id=event.run_id) as conn:
            conn.execute(
                SqlEventLogStorageTable.update()  # pylint: disable=no-value-for-parameter
                .where(SqlEventLogStorageTable.c.id == record_id)
                .values(
                    event=serialize_dagster_namedtuple(event),
                    dagster_event_type=dagster_event_type,
                    timestamp=datetime.utcfromtimestamp(event.timestamp),
                    step_key=event.step_key,
                    asset_key=asset_key_str,
                )
            )

    def get_event_log_table_data(self, run_id, record_id):
        """Utility method to test representation of the record in the SQL table.  Returns all of
        the columns stored in the event log storage (as opposed to the deserialized `EventLogEntry`).
        This allows checking that certain fields are extracted to support performant lookups (e.g.
        extracting `step_key` for fast filtering)"""
        with self.run_connection(run_id=run_id) as conn:
            query = (
                db.select([SqlEventLogStorageTable])
                .where(SqlEventLogStorageTable.c.id == record_id)
                .order_by(SqlEventLogStorageTable.c.id.asc())
            )
            return conn.execute(query).fetchone()

    def has_secondary_index(self, name):
        """This method uses a checkpoint migration table to see if summary data has been constructed
        in a secondary index table.  Can be used to checkpoint event_log data migrations.
        """
        query = (
            db.select([1])
            .where(SecondaryIndexMigrationTable.c.name == name)
            .where(SecondaryIndexMigrationTable.c.migration_completed != None)
            .limit(1)
        )
        with self.index_connection() as conn:
            results = conn.execute(query).fetchall()

        return len(results) > 0

    def enable_secondary_index(self, name):
        """This method marks an event_log data migration as complete, to indicate that a summary
        data migration is complete.
        """
        query = (
            SecondaryIndexMigrationTable.insert().values(  # pylint: disable=no-value-for-parameter
                name=name,
                migration_completed=datetime.now(),
            )
        )
        with self.index_connection() as conn:
            try:
                conn.execute(query)
            except db.exc.IntegrityError:
                conn.execute(
                    SecondaryIndexMigrationTable.update()  # pylint: disable=no-value-for-parameter
                    .where(SecondaryIndexMigrationTable.c.name == name)
                    .values(migration_completed=datetime.now())
                )

    def _apply_filter_to_query(
        self,
        query,
        event_records_filter=None,
        asset_details=None,
        apply_cursor_filters=True,
    ):
        if not event_records_filter:
            return query

        if event_records_filter.event_type:
            query = query.where(
                SqlEventLogStorageTable.c.dagster_event_type
                == event_records_filter.event_type.value
            )

        if event_records_filter.asset_key:
            query = query.where(
                db.or_(
                    SqlEventLogStorageTable.c.asset_key
                    == event_records_filter.asset_key.to_string(),
                    SqlEventLogStorageTable.c.asset_key
                    == event_records_filter.asset_key.to_string(legacy=True),
                )
            )

        if event_records_filter.asset_partitions:
            query = query.where(
                SqlEventLogStorageTable.c.partition.in_(event_records_filter.asset_partitions)
            )

        if asset_details and asset_details.last_wipe_timestamp:
            query = query.where(
                SqlEventLogStorageTable.c.timestamp
                > datetime.utcfromtimestamp(asset_details.last_wipe_timestamp)
            )

        if apply_cursor_filters:
            # allow the run-sharded sqlite implementation to disable this cursor filtering so that
            # it can implement its own custom cursor logic, as cursor ids are not unique across run
            # shards
            if event_records_filter.before_cursor is not None:
                before_cursor_id = (
                    event_records_filter.before_cursor.id
                    if isinstance(event_records_filter.before_cursor, RunShardedEventsCursor)
                    else event_records_filter.before_cursor
                )
                before_query = db.select([SqlEventLogStorageTable.c.id]).where(
                    SqlEventLogStorageTable.c.id == before_cursor_id
                )
                query = query.where(SqlEventLogStorageTable.c.id < before_query)

            if event_records_filter.after_cursor is not None:
                after_cursor_id = (
                    event_records_filter.after_cursor.id
                    if isinstance(event_records_filter.after_cursor, RunShardedEventsCursor)
                    else event_records_filter.after_cursor
                )
                query = query.where(SqlEventLogStorageTable.c.id > after_cursor_id)

        if event_records_filter.before_timestamp:
            query = query.where(
                SqlEventLogStorageTable.c.timestamp
                < datetime.utcfromtimestamp(event_records_filter.before_timestamp)
            )

        if event_records_filter.after_timestamp:
            query = query.where(
                SqlEventLogStorageTable.c.timestamp
                > datetime.utcfromtimestamp(event_records_filter.after_timestamp)
            )

        return query

    def get_event_records(
        self,
        event_records_filter: Optional[EventRecordsFilter] = None,
        limit: Optional[int] = None,
        ascending: bool = False,
    ) -> Iterable[EventLogRecord]:
        """Returns a list of (record_id, record)."""
        check.opt_inst_param(event_records_filter, "event_records_filter", EventRecordsFilter)
        check.opt_int_param(limit, "limit")
        check.bool_param(ascending, "ascending")

        query = db.select([SqlEventLogStorageTable.c.id, SqlEventLogStorageTable.c.event])
        if event_records_filter and event_records_filter.asset_key:
            asset_details = next(iter(self._get_assets_details([event_records_filter.asset_key])))
        else:
            asset_details = None

        query = self._apply_filter_to_query(
            query=query,
            event_records_filter=event_records_filter,
            asset_details=asset_details,
        )
        if limit:
            query = query.limit(limit)

        if ascending:
            query = query.order_by(SqlEventLogStorageTable.c.id.asc())
        else:
            query = query.order_by(SqlEventLogStorageTable.c.id.desc())

        with self.index_connection() as conn:
            results = conn.execute(query).fetchall()

        event_records = []
        for row_id, json_str in results:
            try:
                event_record = deserialize_json_to_dagster_namedtuple(json_str)
                if not isinstance(event_record, EventLogEntry):
                    logging.warning(
                        "Could not resolve event record as EventLogEntry for id `{}`.".format(
                            row_id
                        )
                    )
                    continue
                else:
                    event_records.append(
                        EventLogRecord(storage_id=row_id, event_log_entry=event_record)
                    )
            except seven.JSONDecodeError:
                logging.warning("Could not parse event record id `{}`.".format(row_id))

        return event_records

    def has_asset_key(self, asset_key: AssetKey) -> bool:
        check.inst_param(asset_key, "asset_key", AssetKey)
        rows = self._fetch_asset_rows(asset_keys=[asset_key])
        return bool(rows)

    def all_asset_keys(self):
        rows = self._fetch_asset_rows()
        asset_keys = [AssetKey.from_db_string(row[0]) for row in sorted(rows, key=lambda x: x[0])]
        return [asset_key for asset_key in asset_keys if asset_key]

    def get_asset_keys(
        self,
        prefix: Optional[List[str]] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Iterable[AssetKey]:
        rows = self._fetch_asset_rows(prefix=prefix, limit=limit, cursor=cursor)
        asset_keys = [AssetKey.from_db_string(row[0]) for row in sorted(rows, key=lambda x: x[0])]
        return [asset_key for asset_key in asset_keys if asset_key]

    def get_latest_materialization_events(
        self, asset_keys: Sequence[AssetKey]
    ) -> Mapping[AssetKey, Optional[EventLogEntry]]:
        check.list_param(asset_keys, "asset_keys", AssetKey)
        rows = self._fetch_asset_rows(asset_keys=asset_keys)
        to_backcompat_fetch = set()
        results: Dict[AssetKey, Optional[EventLogEntry]] = {}
        for row in rows:
            asset_key = AssetKey.from_db_string(row[0])
            if not asset_key:
                continue
            event_or_materialization = (
                deserialize_json_to_dagster_namedtuple(row[1]) if row[1] else None
            )
            if isinstance(event_or_materialization, EventLogEntry):
                results[asset_key] = event_or_materialization
            else:
                to_backcompat_fetch.add(asset_key)

        if to_backcompat_fetch:
            latest_event_subquery = (
                db.select(
                    [
                        SqlEventLogStorageTable.c.asset_key,
                        db.func.max(SqlEventLogStorageTable.c.timestamp).label("timestamp"),
                    ]
                )
                .where(
                    db.and_(
                        SqlEventLogStorageTable.c.asset_key.in_(
                            [asset_key.to_string() for asset_key in to_backcompat_fetch]
                        ),
                        SqlEventLogStorageTable.c.dagster_event_type
                        == DagsterEventType.ASSET_MATERIALIZATION.value,
                    )
                )
                .group_by(SqlEventLogStorageTable.c.asset_key)
                .alias("latest_materializations")
            )
            backcompat_query = db.select(
                [SqlEventLogStorageTable.c.asset_key, SqlEventLogStorageTable.c.event]
            ).select_from(
                latest_event_subquery.join(
                    SqlEventLogStorageTable,
                    db.and_(
                        SqlEventLogStorageTable.c.asset_key == latest_event_subquery.c.asset_key,
                        SqlEventLogStorageTable.c.timestamp == latest_event_subquery.c.timestamp,
                    ),
                )
            )
            with self.index_connection() as conn:
                event_rows = conn.execute(backcompat_query).fetchall()

            for row in event_rows:
                asset_key = AssetKey.from_db_string(row[0])
                if asset_key:
                    results[asset_key] = cast(
                        EventLogEntry, deserialize_json_to_dagster_namedtuple(row[1])
                    )

        return results

    def _fetch_asset_rows(self, asset_keys=None, prefix=None, limit=None, cursor=None):
        # fetches rows containing asset_key, last_materialization, and asset_details from the DB,
        # applying the filters specified in the arguments.
        #
        # Differs from _fetch_raw_asset_rows, in that it loops through to make sure enough rows are
        # returned to satisfy the limit.
        #
        # returns a list of rows where each row is a tuple of serialized asset_key, materialization,
        # and asset_details
        should_query = True
        current_cursor = cursor
        if self.has_secondary_index(ASSET_KEY_INDEX_COLS):
            # if we have migrated, we can limit using SQL
            fetch_limit = limit
        else:
            # if we haven't migrated, overfetch in case the first N results are wiped
            fetch_limit = max(limit, MIN_ASSET_ROWS) if limit else None
        result = []

        while should_query:
            rows, has_more, current_cursor = self._fetch_raw_asset_rows(
                asset_keys=asset_keys, prefix=prefix, limit=fetch_limit, cursor=current_cursor
            )
            result.extend(rows)
            should_query = bool(has_more) and bool(limit) and len(result) < cast(int, limit)

        is_partial_query = bool(asset_keys) or bool(prefix) or bool(limit) or bool(cursor)
        if not is_partial_query and self._can_mark_assets_as_migrated(rows):
            self.enable_secondary_index(ASSET_KEY_INDEX_COLS)

        return result[:limit] if limit else result

    def _fetch_raw_asset_rows(self, asset_keys=None, prefix=None, limit=None, cursor=None):
        # fetches rows containing asset_key, last_materialization, and asset_details from the DB,
        # applying the filters specified in the arguments.  Does not guarantee that the number of
        # rows returned will match the limit specified.  This helper function is used to fetch a
        # chunk of asset key rows, which may or may not be wiped.
        #
        # Returns a tuple of (rows, has_more, cursor), where each row is a tuple of serialized
        # asset_key, materialization, and asset_details

        columns = [
            AssetKeyTable.c.asset_key,
            AssetKeyTable.c.last_materialization,
            AssetKeyTable.c.asset_details,
        ]

        is_partial_query = bool(asset_keys) or bool(prefix) or bool(limit) or bool(cursor)
        if self.has_asset_key_index_cols() and not is_partial_query:
            # if the schema has been migrated, fetch the last_materialization_timestamp to see if
            # we can lazily migrate the data table
            columns.append(AssetKeyTable.c.last_materialization_timestamp)
            columns.append(AssetKeyTable.c.wipe_timestamp)

        query = db.select(columns).order_by(AssetKeyTable.c.asset_key.asc())
        query = self._apply_asset_filter_to_query(query, asset_keys, prefix, limit, cursor)

        if self.has_secondary_index(ASSET_KEY_INDEX_COLS):
            query = query.where(
                db.or_(
                    AssetKeyTable.c.wipe_timestamp == None,
                    AssetKeyTable.c.last_materialization_timestamp > AssetKeyTable.c.wipe_timestamp,
                )
            )
            with self.index_connection() as conn:
                rows = conn.execute(query).fetchall()

            return rows, False, None

        with self.index_connection() as conn:
            rows = conn.execute(query).fetchall()

        wiped_timestamps_by_asset_key = {}
        row_by_asset_key = OrderedDict()

        for row in rows:
            asset_key = AssetKey.from_db_string(row[0])
            if not asset_key:
                continue
            asset_details = AssetDetails.from_db_string(row[2])
            if not asset_details or not asset_details.last_wipe_timestamp:
                row_by_asset_key[asset_key] = row
                continue
            materialization_or_event = (
                deserialize_json_to_dagster_namedtuple(row[1]) if row[1] else None
            )
            if isinstance(materialization_or_event, EventLogEntry):
                if asset_details.last_wipe_timestamp > materialization_or_event.timestamp:
                    # this asset has not been materialized since being wiped, skip
                    continue
                else:
                    # add the key
                    row_by_asset_key[asset_key] = row
            else:
                row_by_asset_key[asset_key] = row
                wiped_timestamps_by_asset_key[asset_key] = asset_details.last_wipe_timestamp

        if wiped_timestamps_by_asset_key:
            materialization_times = self._fetch_backcompat_materialization_times(
                wiped_timestamps_by_asset_key.keys()
            )
            for asset_key, wiped_timestamp in wiped_timestamps_by_asset_key.items():
                materialization_time = materialization_times.get(asset_key)
                if not materialization_time or utc_datetime_from_naive(
                    materialization_time
                ) < utc_datetime_from_timestamp(wiped_timestamp):
                    # remove rows that have not been materialized since being wiped
                    row_by_asset_key.pop(asset_key)

        has_more = limit and len(rows) == limit
        new_cursor = rows[-1][0] if rows else None

        return row_by_asset_key.values(), has_more, new_cursor

    def _fetch_backcompat_materialization_times(self, asset_keys):
        # fetches the latest materialization timestamp for the given asset_keys.  Uses the (slower)
        # raw event log table.
        backcompat_query = (
            db.select(
                [
                    SqlEventLogStorageTable.c.asset_key,
                    db.func.max(SqlEventLogStorageTable.c.timestamp),
                ]
            )
            .where(
                SqlEventLogStorageTable.c.asset_key.in_(
                    [asset_key.to_string() for asset_key in asset_keys]
                )
            )
            .group_by(SqlEventLogStorageTable.c.asset_key)
            .order_by(db.func.max(SqlEventLogStorageTable.c.timestamp).asc())
        )
        with self.index_connection() as conn:
            backcompat_rows = conn.execute(backcompat_query).fetchall()
        return {AssetKey.from_db_string(row[0]): row[1] for row in backcompat_rows}

    def _can_mark_assets_as_migrated(self, rows):
        if not self.has_asset_key_index_cols():
            return False

        if self.has_secondary_index(ASSET_KEY_INDEX_COLS):
            # we have already migrated
            return False

        for row in rows:
            if not _get_from_row(row, "last_materialization_timestamp"):
                return False

            if _get_from_row(row, "asset_details") and not _get_from_row(row, "wipe_timestamp"):
                return False

        return True

    def _apply_asset_filter_to_query(
        self,
        query,
        asset_keys=None,
        prefix=None,
        limit=None,
        cursor=None,
    ):
        if asset_keys:
            query = query.where(
                AssetKeyTable.c.asset_key.in_([asset_key.to_string() for asset_key in asset_keys])
            )

        if prefix:
            prefix_str = seven.dumps(prefix)[:-1]
            query = query.where(AssetKeyTable.c.asset_key.startswith(prefix_str))

        if cursor:
            query = query.where(AssetKeyTable.c.asset_key > cursor)

        if limit:
            query = query.limit(limit)
        return query

    def _get_assets_details(self, asset_keys: Sequence[AssetKey]):
        check.list_param(asset_keys, "asset_key", AssetKey)
        rows = None
        with self.index_connection() as conn:
            rows = conn.execute(
                db.select([AssetKeyTable.c.asset_key, AssetKeyTable.c.asset_details]).where(
                    AssetKeyTable.c.asset_key.in_(
                        [asset_key.to_string() for asset_key in asset_keys]
                    ),
                )
            ).fetchall()

            asset_key_to_details = {
                row[0]: (deserialize_json_to_dagster_namedtuple(row[1]) if row[1] else None)
                for row in rows
            }

            # returns a list of the corresponding asset_details to provided asset_keys
            return [
                asset_key_to_details.get(asset_key.to_string(), None) for asset_key in asset_keys
            ]

    def _add_assets_wipe_filter_to_query(
        self, query, assets_details: Sequence[str], asset_keys: Sequence[AssetKey]
    ):
        check.invariant(
            len(assets_details) == len(asset_keys),
            "asset_details and asset_keys must be the same length",
        )
        for i in range(len(assets_details)):
            asset_key, asset_details = asset_keys[i], assets_details[i]
            if asset_details and asset_details.last_wipe_timestamp:  # type: ignore[attr-defined]
                asset_key_in_row = db.or_(
                    SqlEventLogStorageTable.c.asset_key == asset_key.to_string(),
                    SqlEventLogStorageTable.c.asset_key == asset_key.to_string(legacy=True),
                )
                # If asset key is in row, keep the row if the timestamp > wipe timestamp, else remove the row.
                # If asset key is not in row, keep the row.
                query = query.where(
                    db.or_(
                        db.and_(
                            asset_key_in_row,
                            SqlEventLogStorageTable.c.timestamp
                            > datetime.utcfromtimestamp(asset_details.last_wipe_timestamp),  # type: ignore[attr-defined]
                        ),
                        db.not_(asset_key_in_row),
                    )
                )

        return query

    def get_asset_events(
        self,
        asset_key,
        partitions=None,
        before_cursor=None,
        after_cursor=None,
        limit=None,
        ascending=False,
        include_cursor=False,  # deprecated
        before_timestamp=None,
        cursor=None,  # deprecated
    ):
        check.inst_param(asset_key, "asset_key", AssetKey)
        check.opt_list_param(partitions, "partitions", of_type=str)
        before_cursor, after_cursor = extract_asset_events_cursor(
            cursor, before_cursor, after_cursor, ascending
        )
        event_records = self.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
                asset_partitions=partitions,
                before_cursor=before_cursor,
                after_cursor=after_cursor,
                before_timestamp=before_timestamp,
            ),
            limit=limit,
            ascending=ascending,
        )
        if include_cursor:
            return [tuple([record.storage_id, record.event_log_entry]) for record in event_records]
        else:
            return [record.event_log_entry for record in event_records]

    def get_asset_run_ids(self, asset_key):
        check.inst_param(asset_key, "asset_key", AssetKey)
        query = (
            db.select(
                [SqlEventLogStorageTable.c.run_id, db.func.max(SqlEventLogStorageTable.c.timestamp)]
            )
            .where(
                db.or_(
                    SqlEventLogStorageTable.c.asset_key == asset_key.to_string(),
                    SqlEventLogStorageTable.c.asset_key == asset_key.to_string(legacy=True),
                )
            )
            .group_by(
                SqlEventLogStorageTable.c.run_id,
            )
            .order_by(db.func.max(SqlEventLogStorageTable.c.timestamp).desc())
        )

        asset_keys = [asset_key]
        asset_details = self._get_assets_details(asset_keys)
        query = self._add_assets_wipe_filter_to_query(query, asset_details, asset_keys)

        with self.index_connection() as conn:
            results = conn.execute(query).fetchall()

        return [run_id for (run_id, _timestamp) in results]

    def _asset_materialization_from_json_column(self, json_str):
        if not json_str:
            return None

        # We switched to storing the entire event record of the last materialization instead of just
        # the AssetMaterialization object, so that we have access to metadata like timestamp,
        # pipeline, run_id, etc.
        #
        # This should make certain asset queries way more performant, without having to do extra
        # queries against the event log.
        #
        # This should be accompanied by a schema change in 0.12.0, renaming `last_materialization`
        # to `last_materialization_event`, for clarity.  For now, we should do some back-compat.
        #
        # https://github.com/dagster-io/dagster/issues/3945

        event_or_materialization = deserialize_json_to_dagster_namedtuple(json_str)
        if isinstance(event_or_materialization, AssetMaterialization):
            return event_or_materialization

        if (
            not isinstance(event_or_materialization, EventLogEntry)
            or not event_or_materialization.is_dagster_event
            or not event_or_materialization.dagster_event.asset_key
        ):
            return None

        return event_or_materialization.dagster_event.step_materialization_data.materialization

    def wipe_asset(self, asset_key):
        check.inst_param(asset_key, "asset_key", AssetKey)

        wipe_timestamp = pendulum.now("UTC").timestamp()

        if self.has_asset_key_index_cols():
            with self.index_connection() as conn:
                conn.execute(
                    AssetKeyTable.update()  # pylint: disable=no-value-for-parameter
                    .where(
                        db.or_(
                            AssetKeyTable.c.asset_key == asset_key.to_string(),
                            AssetKeyTable.c.asset_key == asset_key.to_string(legacy=True),
                        )
                    )
                    .values(
                        asset_details=serialize_dagster_namedtuple(
                            AssetDetails(last_wipe_timestamp=wipe_timestamp)
                        ),
                        wipe_timestamp=utc_datetime_from_timestamp(wipe_timestamp),
                    )
                )

        else:
            with self.index_connection() as conn:
                conn.execute(
                    AssetKeyTable.update()  # pylint: disable=no-value-for-parameter
                    .where(
                        db.or_(
                            AssetKeyTable.c.asset_key == asset_key.to_string(),
                            AssetKeyTable.c.asset_key == asset_key.to_string(legacy=True),
                        )
                    )
                    .values(
                        asset_details=serialize_dagster_namedtuple(
                            AssetDetails(last_wipe_timestamp=wipe_timestamp)
                        ),
                    )
                )

    def get_materialization_count_by_partition(
        self, asset_keys: Sequence[AssetKey]
    ) -> Mapping[AssetKey, Mapping[str, int]]:
        check.list_param(asset_keys, "asset_keys", AssetKey)

        query = (
            db.select(
                [
                    SqlEventLogStorageTable.c.asset_key,
                    SqlEventLogStorageTable.c.partition,
                    db.func.count(SqlEventLogStorageTable.c.id),
                ]
            )
            .where(
                db.and_(
                    db.or_(
                        SqlEventLogStorageTable.c.asset_key.in_(
                            [asset_key.to_string() for asset_key in asset_keys]
                        ),
                        SqlEventLogStorageTable.c.asset_key.in_(
                            [asset_key.to_string(legacy=True) for asset_key in asset_keys]
                        ),
                    ),
                    SqlEventLogStorageTable.c.partition != None,
                )
            )
            .group_by(SqlEventLogStorageTable.c.asset_key, SqlEventLogStorageTable.c.partition)
        )

        assets_details = self._get_assets_details(asset_keys)
        query = self._add_assets_wipe_filter_to_query(query, assets_details, asset_keys)

        with self.index_connection() as conn:
            results = conn.execute(query).fetchall()

        materialization_count_by_partition: Dict[AssetKey, Dict[str, int]] = {
            asset_key: {} for asset_key in asset_keys
        }
        for row in results:
            asset_key = AssetKey.from_db_string(row[0])
            if asset_key:
                materialization_count_by_partition[asset_key][row[1]] = row[2]

        return materialization_count_by_partition


def _get_from_row(row, column):
    """utility function for extracting a column from a sqlalchemy row proxy, since '_asdict' is not
    supported in sqlalchemy 1.3"""
    if not row.has_key(column):
        return None
    return row[column]
