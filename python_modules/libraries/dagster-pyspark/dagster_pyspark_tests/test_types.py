import shutil

import pytest
from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from dagster_pyspark import pyspark_resource
from pyspark.sql import Row, SparkSession

from dagster import (
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    execute_solid,
    file_relative_path,
    solid,
)
from dagster._utils import dict_without_keys
from dagster._utils.test import get_temp_dir

spark = SparkSession.builder.getOrCreate()

dataframe_parametrize_argnames = "file_type,read,other"
dataframe_parametrize_argvalues = [
    pytest.param("csv", spark.read.csv, False, id="csv"),
    pytest.param("parquet", spark.read.parquet, False, id="parquet"),
    pytest.param("json", spark.read.json, False, id="json"),
    pytest.param("csv", spark.read.load, True, id="other_csv"),
    pytest.param("parquet", spark.read.load, True, id="other_parquet"),
    pytest.param("json", spark.read.load, True, id="other_json"),
]


def create_pyspark_df():
    data = [Row(_c0=str(i), _c1=str(i)) for i in range(100)]
    return spark.createDataFrame(data)


@pytest.mark.parametrize(dataframe_parametrize_argnames, dataframe_parametrize_argvalues)
def test_dataframe_outputs(file_type, read, other):
    df = create_pyspark_df()

    @solid(output_defs=[OutputDefinition(dagster_type=DagsterPySparkDataFrame, name="df")])
    def return_df(_):
        return df

    with get_temp_dir() as temp_path:
        shutil.rmtree(temp_path)

        options = {"path": temp_path}
        if other:
            options["format"] = file_type
            file_type = "other"

        result = execute_solid(
            return_df,
            mode_def=ModeDefinition(resource_defs={"pyspark": pyspark_resource}),
            run_config={"solids": {"return_df": {"outputs": [{"df": {file_type: options}}]}}},
        )
        assert result.success
        actual = read(options["path"], **dict_without_keys(options, "path"))
        assert sorted(df.collect()) == sorted(actual.collect())

        result = execute_solid(
            return_df,
            mode_def=ModeDefinition(resource_defs={"pyspark": pyspark_resource}),
            run_config={
                "solids": {
                    "return_df": {
                        "outputs": [
                            {
                                "df": {
                                    file_type: dict(
                                        {
                                            "mode": "overwrite",
                                            "compression": "gzip",
                                        },
                                        **options,
                                    )
                                }
                            }
                        ]
                    }
                }
            },
        )
        assert result.success
        actual = read(options["path"], **dict_without_keys(options, "path"))
        assert sorted(df.collect()) == sorted(actual.collect())


@pytest.mark.parametrize(dataframe_parametrize_argnames, dataframe_parametrize_argvalues)
def test_dataframe_inputs(file_type, read, other):
    @solid(
        input_defs=[InputDefinition(dagster_type=DagsterPySparkDataFrame, name="input_df")],
    )
    def return_df(_, input_df):
        return input_df

    options = {"path": file_relative_path(__file__, "num.{file_type}".format(file_type=file_type))}
    if other:
        options["format"] = file_type
        file_type = "other"

    result = execute_solid(
        return_df,
        mode_def=ModeDefinition(resource_defs={"pyspark": pyspark_resource}),
        run_config={"solids": {"return_df": {"inputs": {"input_df": {file_type: options}}}}},
    )
    assert result.success
    actual = read(options["path"], **dict_without_keys(options, "path"))
    assert sorted(result.output_value().collect()) == sorted(actual.collect())
