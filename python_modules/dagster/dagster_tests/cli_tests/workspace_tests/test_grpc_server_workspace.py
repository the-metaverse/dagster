from contextlib import ExitStack

import pytest
import yaml

from dagster import seven
from dagster.check import CheckError
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.host_representation import GrpcServerRepositoryLocationOrigin
from dagster._core.test_utils import environ
from dagster._core.workspace.load import location_origins_from_config
from dagster._grpc.server import GrpcServerProcess
from dagster._utils import file_relative_path


@pytest.mark.skipif(seven.IS_WINDOWS, reason="no named sockets on Windows")
def test_grpc_socket_workspace():
    first_server_process = GrpcServerProcess()
    with first_server_process.create_ephemeral_client() as first_server:
        second_server_process = GrpcServerProcess()
        with second_server_process.create_ephemeral_client() as second_server:
            first_socket = first_server.socket
            second_socket = second_server.socket
            workspace_yaml = """
load_from:
- grpc_server:
    host: localhost
    socket: {socket_one}
- grpc_server:
    socket: {socket_two}
    location_name: 'local_port_default_host'
                """.format(
                socket_one=first_socket, socket_two=second_socket
            )

            origins = location_origins_from_config(
                yaml.safe_load(workspace_yaml),
                # fake out as if it were loaded by a yaml file in this directory
                file_relative_path(__file__, "not_a_real.yaml"),
            )

            with ExitStack() as stack:
                repository_locations = {
                    name: stack.enter_context(origin.create_location())
                    for name, origin in origins.items()
                }
                assert len(repository_locations) == 2

                default_location_name = "grpc:localhost:{socket}".format(socket=first_socket)
                assert repository_locations.get(default_location_name)
                local_port = repository_locations.get(default_location_name)

                assert local_port.socket == first_socket
                assert local_port.host == "localhost"
                assert local_port.port is None

                assert repository_locations.get("local_port_default_host")
                local_port_default_host = repository_locations.get("local_port_default_host")

                assert local_port_default_host.socket == second_socket
                assert local_port_default_host.host == "localhost"
                assert local_port_default_host.port is None

                assert all(map(lambda x: x.name, repository_locations.values()))
        second_server_process.wait()
    first_server_process.wait()


def test_grpc_server_env_vars():
    with environ(
        {
            "FOO_PORT": "1234",
            "FOO_SOCKET": "barsocket",
            "FOO_HOST": "barhost",
        }
    ):
        valid_yaml = """
    load_from:
        - grpc_server:
            host:
              env: FOO_HOST
            port:
              env: FOO_PORT
            location_name: 'my_grpc_server_port'
        - grpc_server:
            host:
              env: FOO_HOST
            socket:
              env: FOO_SOCKET
            location_name: 'my_grpc_server_socket'
    """

        origins = location_origins_from_config(
            yaml.safe_load(valid_yaml),
            file_relative_path(__file__, "not_a_real.yaml"),
        )

        assert len(origins) == 2

        port_origin = origins["my_grpc_server_port"]
        assert isinstance(origins["my_grpc_server_port"], GrpcServerRepositoryLocationOrigin)

        assert port_origin.port == 1234
        assert port_origin.host == "barhost"

        socket_origin = origins["my_grpc_server_socket"]
        assert isinstance(origins["my_grpc_server_socket"], GrpcServerRepositoryLocationOrigin)

        assert socket_origin.socket == "barsocket"
        assert socket_origin.host == "barhost"


def test_ssl_grpc_server_workspace():
    server_process = GrpcServerProcess(force_port=True)
    try:
        with server_process.create_ephemeral_client() as client:

            assert client.heartbeat(echo="Hello")

            port = server_process.port
            ssl_yaml = f"""
    load_from:
    - grpc_server:
        host: localhost
        port: {port}
        ssl: true
    """
            origins = location_origins_from_config(
                yaml.safe_load(ssl_yaml),
                # fake out as if it were loaded by a yaml file in this directory
                file_relative_path(__file__, "not_a_real.yaml"),
            )
            origin = list(origins.values())[0]
            assert origin.use_ssl

            # Actually connecting to the server will fail since it's expecting SSL
            # and we didn't set up the server with SSL
            try:
                with origin.create_location():
                    assert False
            except DagsterUserCodeUnreachableError:
                pass

    finally:
        server_process.wait()


def test_grpc_server_workspace():
    first_server_process = GrpcServerProcess(force_port=True)
    with first_server_process.create_ephemeral_client() as first_server:
        second_server_process = GrpcServerProcess(force_port=True)
        with second_server_process.create_ephemeral_client() as second_server:
            first_port = first_server.port
            second_port = second_server.port
            workspace_yaml = """
load_from:
- grpc_server:
    host: localhost
    port: {port_one}
- grpc_server:
    port: {port_two}
    location_name: 'local_port_default_host'
                """.format(
                port_one=first_port, port_two=second_port
            )

            origins = location_origins_from_config(
                yaml.safe_load(workspace_yaml),
                # fake out as if it were loaded by a yaml file in this directory
                file_relative_path(__file__, "not_a_real.yaml"),
            )

            with ExitStack() as stack:
                repository_locations = {
                    name: stack.enter_context(origin.create_location())
                    for name, origin in origins.items()
                }
                assert len(repository_locations) == 2

                default_location_name = "grpc:localhost:{port}".format(port=first_port)
                assert repository_locations.get(default_location_name)
                local_port = repository_locations.get(default_location_name)

                assert local_port.port == first_port
                assert local_port.host == "localhost"
                assert local_port.socket is None

                assert repository_locations.get("local_port_default_host")
                local_port_default_host = repository_locations.get("local_port_default_host")

                assert local_port_default_host.port == second_port
                assert local_port_default_host.host == "localhost"
                assert local_port_default_host.socket is None

                assert all(map(lambda x: x.name, repository_locations.values()))
        second_server_process.wait()
    first_server_process.wait()


def test_cannot_set_socket_and_port():
    workspace_yaml = """
load_from:
  - grpc_server:
      socket: myname
      port: 5678
    """

    with pytest.raises(CheckError, match="must supply either a socket or a port"):
        location_origins_from_config(
            yaml.safe_load(workspace_yaml),
            # fake out as if it were loaded by a yaml file in this directory
            file_relative_path(__file__, "not_a_real.yaml"),
        )
