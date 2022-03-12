import os
import sys

import pytest
from click import UsageError
from click.testing import CliRunner

from dagster import seven
from dagster._cli.job import job_list_command
from dagster._cli.pipeline import execute_list_command, pipeline_list_command
from dagster.core.test_utils import instance_for_test
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.grpc.server import GrpcServerProcess
from dagster.utils import file_relative_path


def no_print(_):
    return None


def assert_correct_bar_repository_output(result):
    assert result.exit_code == 0
    assert result.output == (
        "Repository bar\n"
        "**************\n"
        "Pipeline: baz\n"
        "Description:\n"
        "Not much tbh\n"
        "Solids: (Execution Order)\n"
        "    do_input\n"
        "*************\n"
        "Pipeline: foo\n"
        "Solids: (Execution Order)\n"
        "    do_something\n"
        "    do_input\n"
        "********************\n"
        "Pipeline: memoizable\n"
        "Solids: (Execution Order)\n"
        "    my_solid\n"
        "****************************************\n"
        "Pipeline: partitioned_scheduled_pipeline\n"
        "Solids: (Execution Order)\n"
        "    do_something\n"
        "******************\n"
        "Pipeline: quux_job\n"
        "Solids: (Execution Order)\n"
        "    do_something_op\n"
        "*************\n"
        "Pipeline: qux\n"
        "Solids: (Execution Order)\n"
        "    do_something_op\n"
        "    do_input_op\n"
    )


def assert_correct_job_list_bar_repository_output(result):
    assert result.exit_code == 0
    assert result.output == (
        "Repository bar\n"
        "**************\n"
        "Job: quux_job\n"
        "Ops: (Execution Order)\n"
        "    do_something_op\n"
        "********\n"
        "Job: qux\n"
        "Ops: (Execution Order)\n"
        "    do_something_op\n"
        "    do_input_op\n"
    )


def assert_correct_extra_repository_output(result):
    assert result.exit_code == 0
    assert result.output == (
        "Repository extra\n"
        "****************\n"
        "Pipeline: extra\n"
        "Solids: (Execution Order)\n"
        "    do_something\n"
        "*******************\n"
        "Pipeline: extra_job\n"
        "Solids: (Execution Order)\n"
        "    do_something\n"
    )


def assert_correct_job_list_extra_repository_output(result):
    assert result.exit_code == 0
    assert result.output == (
        "Repository extra\n"
        "****************\n"
        "Job: extra_job\n"
        "Ops: (Execution Order)\n"
        "    do_something\n"
    )


@pytest.mark.skipif(seven.IS_WINDOWS, reason="no named sockets on Windows")
def test_list_command_grpc_socket():
    with instance_for_test():
        runner = CliRunner()

        server_process = GrpcServerProcess(
            loadable_target_origin=LoadableTargetOrigin(
                executable_path=sys.executable,
                python_file=file_relative_path(__file__, "test_cli_commands.py"),
                attribute="bar",
            ),
        )

        with server_process.create_ephemeral_client() as api_client:
            execute_list_command(
                {"grpc_socket": api_client.socket},
                no_print,
            )
            execute_list_command(
                {"grpc_socket": api_client.socket, "grpc_host": api_client.host},
                no_print,
            )

            result = runner.invoke(pipeline_list_command, ["--grpc-socket", api_client.socket])
            assert_correct_bar_repository_output(result)

            result = runner.invoke(
                pipeline_list_command,
                ["--grpc-socket", api_client.socket, "--grpc-host", api_client.host],
            )
            assert_correct_bar_repository_output(result)

        server_process.wait()


def test_list_command_deployed_grpc():
    with instance_for_test():
        runner = CliRunner()

        server_process = GrpcServerProcess(
            loadable_target_origin=LoadableTargetOrigin(
                executable_path=sys.executable,
                python_file=file_relative_path(__file__, "test_cli_commands.py"),
                attribute="bar",
            ),
            force_port=True,
        )

        with server_process.create_ephemeral_client() as api_client:
            result = runner.invoke(pipeline_list_command, ["--grpc-port", api_client.port])
            assert_correct_bar_repository_output(result)

            result = runner.invoke(
                pipeline_list_command,
                ["--grpc-port", api_client.port, "--grpc-host", api_client.host],
            )
            assert_correct_bar_repository_output(result)

            result = runner.invoke(pipeline_list_command, ["--grpc-port", api_client.port])
            assert_correct_bar_repository_output(result)

            result = runner.invoke(
                pipeline_list_command,
                ["--grpc-port", api_client.port, "--grpc-socket", "foonamedsocket"],
            )
            assert result.exit_code != 0

            execute_list_command(
                {"grpc_port": api_client.port},
                no_print,
            )

            # Can't supply both port and socket
            with pytest.raises(UsageError):
                execute_list_command(
                    {"grpc_port": api_client.port, "grpc_socket": "foonamedsocket"},
                    no_print,
                )

        server_process.wait()


def test_list_command_cli():
    with instance_for_test():

        runner = CliRunner()

        result = runner.invoke(
            pipeline_list_command,
            ["-f", file_relative_path(__file__, "test_cli_commands.py"), "-a", "bar"],
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command,
            ["-f", file_relative_path(__file__, "test_cli_commands.py"), "-a", "bar"],
        )
        assert_correct_job_list_bar_repository_output(result)

        result = runner.invoke(
            pipeline_list_command,
            [
                "-f",
                file_relative_path(__file__, "test_cli_commands.py"),
                "-a",
                "bar",
                "-d",
                os.path.dirname(__file__),
            ],
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command,
            [
                "-f",
                file_relative_path(__file__, "test_cli_commands.py"),
                "-a",
                "bar",
                "-d",
                os.path.dirname(__file__),
            ],
        )
        assert_correct_job_list_bar_repository_output(result)

        result = runner.invoke(
            pipeline_list_command,
            ["-m", "dagster_tests.cli_tests.command_tests.test_cli_commands", "-a", "bar"],
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command,
            ["-m", "dagster_tests.cli_tests.command_tests.test_cli_commands", "-a", "bar"],
        )
        assert_correct_job_list_bar_repository_output(result)

        result = runner.invoke(
            pipeline_list_command, ["-w", file_relative_path(__file__, "workspace.yaml")]
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command, ["-w", file_relative_path(__file__, "workspace.yaml")]
        )
        assert_correct_job_list_bar_repository_output(result)

        result = runner.invoke(
            pipeline_list_command,
            [
                "-w",
                file_relative_path(__file__, "workspace.yaml"),
                "-w",
                file_relative_path(__file__, "override.yaml"),
            ],
        )
        assert_correct_extra_repository_output(result)

        result = runner.invoke(
            job_list_command,
            [
                "-w",
                file_relative_path(__file__, "workspace.yaml"),
                "-w",
                file_relative_path(__file__, "override.yaml"),
            ],
        )
        assert_correct_job_list_extra_repository_output(result)

        result = runner.invoke(
            pipeline_list_command,
            [
                "-f",
                "foo.py",
                "-m",
                "dagster_tests.cli_tests.command_tests.test_cli_commands",
                "-a",
                "bar",
            ],
        )
        assert result.exit_code == 2

        result = runner.invoke(
            job_list_command,
            [
                "-f",
                "foo.py",
                "-m",
                "dagster_tests.cli_tests.command_tests.test_cli_commands",
                "-a",
                "bar",
            ],
        )
        assert result.exit_code == 2

        result = runner.invoke(
            pipeline_list_command,
            ["-m", "dagster_tests.cli_tests.command_tests.test_cli_commands"],
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command,
            ["-m", "dagster_tests.cli_tests.command_tests.test_cli_commands"],
        )
        assert_correct_job_list_bar_repository_output(result)

        result = runner.invoke(
            pipeline_list_command, ["-f", file_relative_path(__file__, "test_cli_commands.py")]
        )
        assert_correct_bar_repository_output(result)

        result = runner.invoke(
            job_list_command, ["-f", file_relative_path(__file__, "test_cli_commands.py")]
        )
        assert_correct_job_list_bar_repository_output(result)


def test_list_command():
    with instance_for_test():
        execute_list_command(
            {
                "repository_yaml": None,
                "python_file": file_relative_path(__file__, "test_cli_commands.py"),
                "module_name": None,
                "fn_name": "bar",
            },
            no_print,
        )

        execute_list_command(
            {
                "repository_yaml": None,
                "python_file": file_relative_path(__file__, "test_cli_commands.py"),
                "module_name": None,
                "fn_name": "bar",
                "working_directory": os.path.dirname(__file__),
            },
            no_print,
        )

        execute_list_command(
            {
                "repository_yaml": None,
                "python_file": None,
                "module_name": "dagster_tests.cli_tests.command_tests.test_cli_commands",
                "fn_name": "bar",
            },
            no_print,
        )

        with pytest.raises(UsageError):
            execute_list_command(
                {
                    "repository_yaml": None,
                    "python_file": "foo.py",
                    "module_name": "dagster_tests.cli_tests.command_tests.test_cli_commands",
                    "fn_name": "bar",
                },
                no_print,
            )


def test_job_command_only_selects_job(capsys):
    with instance_for_test():
        runner = CliRunner()
        result = runner.invoke(
            job_list_command,
            ["-f", file_relative_path(__file__, "repo_pipeline_and_job.py"), "-a", "my_repo"],
        )

        assert "my_job" in result.output
        assert not "my_pipeline" in result.output
