"""Pipeline definitions for the airline_demo.
"""

import os

from dagster_aws.emr import emr_pyspark_step_launcher
from dagster_aws.s3 import (
    S3FileHandle,
    file_handle_to_s3,
    s3_file_cache,
    s3_file_manager,
    s3_resource,
)
from dagster_pyspark import pyspark_resource
from pyspark.sql import SparkSession

from dagster import (
    Field,
    IOManager,
    ModeDefinition,
    Noneable,
    PresetDefinition,
    composite_solid,
    fs_io_manager,
    io_manager,
    pipeline,
)
from dagster._core.definitions.no_step_launcher import no_step_launcher
from dagster._core.storage.file_cache import fs_file_cache
from dagster._core.storage.file_manager import local_file_manager
from dagster._core.storage.temp_file_manager import tempfile_resource

from .cache_file_from_s3 import cache_file_from_s3
from .resources import postgres_db_info_resource, redshift_db_info_resource
from .solids import (
    average_sfo_outbound_avg_delays_by_destination,
    delays_by_geography,
    delays_vs_fares,
    delays_vs_fares_nb,
    eastbound_delays,
    ingest_csv_file_handle_to_spark,
    join_q2_data,
    load_data_to_database_from_spark,
    process_sfo_weather_data,
    q2_sfo_outbound_flights,
    s3_to_df,
    s3_to_dw_table,
    sfo_delays_by_destination,
    tickets_with_destination,
    westbound_delays,
)


class ParquetIOManager(IOManager):
    def __init__(self, base_dir=None):
        self._base_dir = base_dir or os.getcwd()

    def _get_path(self, context):
        return os.path.join(self._base_dir, context.run_id, context.step_key, context.name)

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))


@io_manager(config_schema={"base_dir": Field(Noneable(str), is_required=False, default_value=None)})
def local_parquet_io_manager(init_context):
    return ParquetIOManager(base_dir=init_context.resource_config["base_dir"])


# start_pipelines_marker_2
test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "pyspark_step_launcher": no_step_launcher,
        "pyspark": pyspark_resource,
        "db_info": redshift_db_info_resource,
        "tempfile": tempfile_resource,
        "s3": s3_resource,
        "file_cache": fs_file_cache,
        "file_manager": local_file_manager,
        "io_manager": fs_io_manager,
        "pyspark_io_manager": local_parquet_io_manager,
    },
)


local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "pyspark_step_launcher": no_step_launcher,
        "pyspark": pyspark_resource,
        "s3": s3_resource,
        "db_info": postgres_db_info_resource,
        "tempfile": tempfile_resource,
        "file_cache": fs_file_cache,
        "file_manager": local_file_manager,
        "io_manager": fs_io_manager,
        "pyspark_io_manager": local_parquet_io_manager,
    },
)


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "pyspark_step_launcher": emr_pyspark_step_launcher,
        "pyspark": pyspark_resource,
        "s3": s3_resource,
        "db_info": redshift_db_info_resource,
        "tempfile": tempfile_resource,
        "file_cache": s3_file_cache,
        "file_manager": s3_file_manager,
        "io_manager": fs_io_manager,
        "pyspark_io_manager": local_parquet_io_manager,
    },
)

# end_pipelines_marker_2


# start_pipelines_marker_0
@pipeline(
    # ordered so the local is first and therefore the default
    mode_defs=[local_mode, test_mode, prod_mode],
    # end_pipelines_marker_0
    preset_defs=[
        PresetDefinition.from_pkg_resources(
            name="local_fast",
            mode="local",
            pkg_resource_defs=[
                ("airline_demo.environments", "local_base.yaml"),
                ("airline_demo.environments", "local_fast_ingest.yaml"),
            ],
        ),
        PresetDefinition.from_pkg_resources(
            name="local_full",
            mode="local",
            pkg_resource_defs=[
                ("airline_demo.environments", "local_base.yaml"),
                ("airline_demo.environments", "local_full_ingest.yaml"),
            ],
        ),
        PresetDefinition.from_pkg_resources(
            name="prod_fast",
            mode="prod",
            pkg_resource_defs=[
                ("airline_demo.environments", "prod_base.yaml"),
                ("airline_demo.environments", "s3_storage.yaml"),
                ("airline_demo.environments", "local_fast_ingest.yaml"),
            ],
        ),
    ],
)
def airline_demo_ingest_pipeline():
    # on time data
    # start_airline_demo_ingest_pipeline
    load_data_to_database_from_spark.alias("load_q2_on_time_data")(
        data_frame=join_q2_data(
            april_data=s3_to_df.alias("april_on_time_s3_to_df")(),
            may_data=s3_to_df.alias("may_on_time_s3_to_df")(),
            june_data=s3_to_df.alias("june_on_time_s3_to_df")(),
            master_cord_data=s3_to_df.alias("master_cord_s3_to_df")(),
        )
    )
    # end_airline_demo_ingest_pipeline

    # load weather data
    load_data_to_database_from_spark.alias("load_q2_sfo_weather")(
        process_sfo_weather_data(
            ingest_csv_file_handle_to_spark.alias("ingest_q2_sfo_weather")(
                cache_file_from_s3.alias("download_q2_sfo_weather")()
            )
        )
    )

    s3_to_dw_table.alias("process_q2_coupon_data")()
    s3_to_dw_table.alias("process_q2_market_data")()
    s3_to_dw_table.alias("process_q2_ticket_data")()


def define_airline_demo_ingest_pipeline():
    return airline_demo_ingest_pipeline


@composite_solid
def process_delays_by_geo() -> S3FileHandle:
    return file_handle_to_s3.alias("upload_delays_by_geography_pdf_plots")(
        delays_by_geography(
            westbound_delays=westbound_delays(), eastbound_delays=eastbound_delays()
        )
    )


@pipeline(
    mode_defs=[test_mode, local_mode, prod_mode],
    preset_defs=[
        PresetDefinition.from_pkg_resources(
            name="local",
            mode="local",
            pkg_resource_defs=[
                ("airline_demo.environments", "local_base.yaml"),
                ("airline_demo.environments", "local_warehouse.yaml"),
            ],
        )
    ],
)
def airline_demo_warehouse_pipeline():
    process_delays_by_geo()

    outbound_delays = average_sfo_outbound_avg_delays_by_destination(q2_sfo_outbound_flights())

    file_handle_to_s3.alias("upload_delays_vs_fares_pdf_plots")(
        delays_vs_fares_nb.alias("fares_vs_delays")(
            delays_vs_fares(
                tickets_with_destination=tickets_with_destination(),
                average_sfo_outbound_avg_delays_by_destination=outbound_delays,
            )
        )
    )

    file_handle_to_s3.alias("upload_outbound_avg_delay_pdf_plots")(
        sfo_delays_by_destination(outbound_delays)
    )


def define_airline_demo_warehouse_pipeline():
    return airline_demo_warehouse_pipeline
