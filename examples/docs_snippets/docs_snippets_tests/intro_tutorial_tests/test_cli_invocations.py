import os
import runpy

import pytest
from click.testing import CliRunner
from dagit.app import create_app_from_workspace_process_context
from starlette.testclient import TestClient

from dagster._cli.pipeline import pipeline_execute_command
from dagster._cli.workspace import get_workspace_process_context_from_kwargs
from dagster.core.instance import DagsterInstance
from dagster.core.test_utils import instance_for_test
from dagster.utils import check_script, pushd, script_relative_path

PIPELINES_OR_ERROR_QUERY = """
{
    repositoriesOrError {
        ... on PythonError {
            message
            stack
        }
        ... on RepositoryConnection {
            nodes {
                pipelines {
                    name
                }
            }
        }
    }
}
"""

cli_args = [
    # dirname, filename, fn_name, env_yaml, mode, preset, return_code, exception
    (
        "basics/single_solid_pipeline/",
        "hello_cereal.py",
        "hello_cereal_job",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "basics/connecting_solids/",
        "serial_pipeline.py",
        "serial",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "basics/connecting_solids/",
        "complex_pipeline.py",
        "diamond",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "basics/e04_quality/",
        "inputs_typed.py",
        "inputs_job",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "basics/e04_quality/",
        "custom_types.py",
        "custom_type_job",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "basics/e04_quality/",
        "custom_types_2.py",
        "custom_type_job",
        None,
        None,
        None,
        1,
        None,
    ),
    (
        "advanced/materializations/",
        "materializations.py",
        "materialization_job",
        None,
        None,
        None,
        0,
        None,
    ),
    (
        "advanced/scheduling/",
        "scheduler.py",
        "hello_cereal_job",
        "inputs_env.yaml",
        None,
        None,
        0,
        None,
    ),
]


def path_to_tutorial_file(path):
    return script_relative_path(
        os.path.join("../../docs_snippets/intro_tutorial/", path)
    )


def load_dagit_for_workspace_cli_args(n_pipelines=1, **kwargs):
    instance = DagsterInstance.ephemeral()
    with get_workspace_process_context_from_kwargs(
        instance, version="", read_only=False, kwargs=kwargs
    ) as workspace_process_context:
        client = TestClient(
            create_app_from_workspace_process_context(workspace_process_context)
        )

        res = client.get(
            "/graphql?query={query_string}".format(
                query_string=PIPELINES_OR_ERROR_QUERY
            )
        )
        json_res = res.json()
        assert "data" in json_res
        assert "repositoriesOrError" in json_res["data"]
        assert "nodes" in json_res["data"]["repositoriesOrError"]
        assert (
            len(json_res["data"]["repositoriesOrError"]["nodes"][0]["pipelines"])
            == n_pipelines
        )

    return res


def dagster_pipeline_execute(args, return_code):
    with instance_for_test():
        runner = CliRunner()
        res = runner.invoke(pipeline_execute_command, args)
    assert res.exit_code == return_code, res.exception

    return res


@pytest.mark.parametrize(
    "dirname,filename,fn_name,_env_yaml,_mode,_preset,_return_code,_exception", cli_args
)
# dagit -f filename -n fn_name
def test_load_pipeline(
    dirname, filename, fn_name, _env_yaml, _mode, _preset, _return_code, _exception
):
    with pushd(path_to_tutorial_file(dirname)):
        filepath = path_to_tutorial_file(os.path.join(dirname, filename))
        load_dagit_for_workspace_cli_args(python_file=filepath, fn_name=fn_name)


@pytest.mark.parametrize(
    "dirname,filename,fn_name,env_yaml,mode,preset,return_code,_exception", cli_args
)
# dagster pipeline execute -f filename -n fn_name -e env_yaml --preset preset
def test_dagster_pipeline_execute(
    dirname, filename, fn_name, env_yaml, mode, preset, return_code, _exception
):
    with pushd(path_to_tutorial_file(dirname)):
        filepath = path_to_tutorial_file(os.path.join(dirname, filename))
        yamlpath = (
            path_to_tutorial_file(os.path.join(dirname, env_yaml)) if env_yaml else None
        )
        dagster_pipeline_execute(
            ["-f", filepath, "-a", fn_name]
            + (["-c", yamlpath] if yamlpath else [])
            + (["--mode", mode] if mode else [])
            + (["--preset", preset] if preset else []),
            return_code,
        )


@pytest.mark.parametrize(
    "dirname,filename,_fn_name,_env_yaml,_mode,_preset,return_code,_exception", cli_args
)
def test_script(
    dirname, filename, _fn_name, _env_yaml, _mode, _preset, return_code, _exception
):
    with pushd(path_to_tutorial_file(dirname)):
        filepath = path_to_tutorial_file(os.path.join(dirname, filename))
        check_script(filepath, return_code)


@pytest.mark.parametrize(
    "dirname,filename,_fn_name,_env_yaml,_mode,_preset,_return_code,exception", cli_args
)
def test_runpy(
    dirname, filename, _fn_name, _env_yaml, _mode, _preset, _return_code, exception
):
    with pushd(path_to_tutorial_file(dirname)):
        filepath = path_to_tutorial_file(os.path.join(dirname, filename))
        if exception:
            with pytest.raises(exception):
                runpy.run_path(filepath, run_name="__main__")
        else:
            runpy.run_path(filepath, run_name="__main__")
