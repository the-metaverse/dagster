import os
from datetime import datetime, timedelta

from hacker_news.jobs.hacker_news_api_download import download_prod_job, download_staging_job

from dagster import Partition
from dagster._core.definitions import JobDefinition
from dagster._core.execution.api import create_execution_plan


def assert_partitioned_schedule_builds(
    job_def: JobDefinition,
    start: datetime,
    end: datetime,
):
    partition_set = job_def.get_partition_set_def()
    run_config = partition_set.run_config_for_partition(Partition((start, end)))
    create_execution_plan(job_def, run_config=run_config)


def test_daily_download_schedule():
    os.environ["SLACK_DAGSTER_ETL_BOT_TOKEN"] = "something"
    start = datetime.strptime("2020-10-01", "%Y-%m-%d")
    end = start + timedelta(hours=1)

    assert_partitioned_schedule_builds(
        download_prod_job,
        start,
        end,
    )
    assert_partitioned_schedule_builds(
        download_staging_job,
        start,
        end,
    )
