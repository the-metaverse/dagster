import json
from unittest import mock

from dagster_k8s import K8sRunLauncher
from dagster_k8s.job import DAGSTER_PG_PASSWORD_ENV_VAR, UserDefinedDagsterK8sConfig

from dagster import pipeline, reconstructable
from dagster._core.host_representation import RepositoryHandle
from dagster._core.launcher import LaunchRunContext
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._core.test_utils import (
    create_run_for_test,
    in_process_test_workspace,
    instance_for_test,
)
from dagster._grpc.types import ExecuteRunArgs
from dagster.utils import merge_dicts
from dagster.utils.hosted_user_process import external_pipeline_from_recon_pipeline


def test_launcher_from_config(kubeconfig_file):
    default_config = {
        "service_account_name": "dagit-admin",
        "instance_config_map": "dagster-instance",
        "postgres_password_secret": "dagster-postgresql-secret",
        "dagster_home": "/opt/dagster/dagster_home",
        "job_image": "fake_job_image",
        "load_incluster_config": False,
        "kubeconfig_file": kubeconfig_file,
    }

    with instance_for_test(
        overrides={
            "run_launcher": {
                "module": "dagster_k8s",
                "class": "K8sRunLauncher",
                "config": default_config,
            }
        }
    ) as instance:
        run_launcher = instance.run_launcher
        assert isinstance(run_launcher, K8sRunLauncher)
        assert run_launcher.fail_pod_on_run_failure == None

    with instance_for_test(
        overrides={
            "run_launcher": {
                "module": "dagster_k8s",
                "class": "K8sRunLauncher",
                "config": merge_dicts(default_config, {"fail_pod_on_run_failure": True}),
            }
        }
    ) as instance:
        run_launcher = instance.run_launcher
        assert isinstance(run_launcher, K8sRunLauncher)
        assert run_launcher.fail_pod_on_run_failure


def test_user_defined_k8s_config_in_run_tags(kubeconfig_file):

    labels = {"foo_label_key": "bar_label_value"}

    # Construct a K8s run launcher in a fake k8s environment.
    mock_k8s_client_batch_api = mock.MagicMock()
    k8s_run_launcher = K8sRunLauncher(
        service_account_name="dagit-admin",
        instance_config_map="dagster-instance",
        postgres_password_secret="dagster-postgresql-secret",
        dagster_home="/opt/dagster/dagster_home",
        job_image="fake_job_image",
        load_incluster_config=False,
        kubeconfig_file=kubeconfig_file,
        k8s_client_batch_api=mock_k8s_client_batch_api,
        labels=labels,
    )

    # Construct Dagster run tags with user defined k8s config.
    expected_resources = {
        "requests": {"cpu": "250m", "memory": "64Mi"},
        "limits": {"cpu": "500m", "memory": "2560Mi"},
    }
    user_defined_k8s_config = UserDefinedDagsterK8sConfig(
        container_config={"resources": expected_resources},
    )
    user_defined_k8s_config_json = json.dumps(user_defined_k8s_config.to_dict())
    tags = {"dagster-k8s/config": user_defined_k8s_config_json}

    # Create fake external pipeline.
    recon_pipeline = reconstructable(fake_pipeline)
    recon_repo = recon_pipeline.repository
    repo_def = recon_repo.get_definition()
    with instance_for_test() as instance:
        with in_process_test_workspace(instance, recon_repo) as workspace:
            location = workspace.get_repository_location(workspace.repository_location_names[0])
            repo_handle = RepositoryHandle(
                repository_name=repo_def.name,
                repository_location=location,
            )
            fake_external_pipeline = external_pipeline_from_recon_pipeline(
                recon_pipeline,
                solid_selection=None,
                repository_handle=repo_handle,
            )

            # Launch the run in a fake Dagster instance.
            pipeline_name = "demo_pipeline"
            run = create_run_for_test(
                instance,
                pipeline_name=pipeline_name,
                tags=tags,
                external_pipeline_origin=fake_external_pipeline.get_external_origin(),
                pipeline_code_origin=fake_external_pipeline.get_python_origin(),
            )
            k8s_run_launcher.register_instance(instance)
            k8s_run_launcher.launch_run(LaunchRunContext(run, workspace))

            updated_run = instance.get_run_by_id(run.run_id)
            assert updated_run.tags[DOCKER_IMAGE_TAG] == "fake_job_image"

        # Check that user defined k8s config was passed down to the k8s job.
        mock_method_calls = mock_k8s_client_batch_api.method_calls
        assert len(mock_method_calls) > 0
        method_name, _args, kwargs = mock_method_calls[0]
        assert method_name == "create_namespaced_job"

        container = kwargs["body"].spec.template.spec.containers[0]

        job_resources = container.resources
        assert job_resources.to_dict() == expected_resources
        assert DAGSTER_PG_PASSWORD_ENV_VAR in [env.name for env in container.env]

        labels = kwargs["body"].spec.template.metadata.labels
        assert labels["foo_label_key"] == "bar_label_value"

        args = container.args
        assert (
            args
            == ExecuteRunArgs(
                pipeline_origin=run.pipeline_code_origin,
                pipeline_run_id=run.run_id,
                instance_ref=instance.get_ref(),
                set_exit_code_on_failure=None,
            ).get_command_args()
        )


def test_raise_on_error(kubeconfig_file):
    # Construct a K8s run launcher in a fake k8s environment.
    mock_k8s_client_batch_api = mock.MagicMock()
    k8s_run_launcher = K8sRunLauncher(
        service_account_name="dagit-admin",
        instance_config_map="dagster-instance",
        postgres_password_secret="dagster-postgresql-secret",
        dagster_home="/opt/dagster/dagster_home",
        job_image="fake_job_image",
        load_incluster_config=False,
        kubeconfig_file=kubeconfig_file,
        k8s_client_batch_api=mock_k8s_client_batch_api,
        fail_pod_on_run_failure=True,
    )
    # Create fake external pipeline.
    recon_pipeline = reconstructable(fake_pipeline)
    recon_repo = recon_pipeline.repository
    repo_def = recon_repo.get_definition()
    with instance_for_test() as instance:
        with in_process_test_workspace(instance, recon_repo) as workspace:
            location = workspace.get_repository_location(workspace.repository_location_names[0])
            repo_handle = RepositoryHandle(
                repository_name=repo_def.name,
                repository_location=location,
            )
            fake_external_pipeline = external_pipeline_from_recon_pipeline(
                recon_pipeline,
                solid_selection=None,
                repository_handle=repo_handle,
            )

            # Launch the run in a fake Dagster instance.
            pipeline_name = "demo_pipeline"
            run = create_run_for_test(
                instance,
                pipeline_name=pipeline_name,
                external_pipeline_origin=fake_external_pipeline.get_external_origin(),
                pipeline_code_origin=fake_external_pipeline.get_python_origin(),
            )
            k8s_run_launcher.register_instance(instance)
            k8s_run_launcher.launch_run(LaunchRunContext(run, workspace))

        mock_method_calls = mock_k8s_client_batch_api.method_calls
        assert len(mock_method_calls) > 0
        method_name, _args, kwargs = mock_method_calls[0]
        assert method_name == "create_namespaced_job"

        container = kwargs["body"].spec.template.spec.containers[0]
        args = container.args
        assert (
            args
            == ExecuteRunArgs(
                pipeline_origin=run.pipeline_code_origin,
                pipeline_run_id=run.run_id,
                instance_ref=instance.get_ref(),
                set_exit_code_on_failure=True,
            ).get_command_args()
        )


def test_no_postgres(kubeconfig_file):
    # Construct a K8s run launcher in a fake k8s environment.
    mock_k8s_client_batch_api = mock.MagicMock()
    k8s_run_launcher = K8sRunLauncher(
        service_account_name="dagit-admin",
        instance_config_map="dagster-instance",
        dagster_home="/opt/dagster/dagster_home",
        job_image="fake_job_image",
        load_incluster_config=False,
        kubeconfig_file=kubeconfig_file,
        k8s_client_batch_api=mock_k8s_client_batch_api,
    )

    # Create fake external pipeline.
    recon_pipeline = reconstructable(fake_pipeline)
    recon_repo = recon_pipeline.repository
    repo_def = recon_repo.get_definition()

    with instance_for_test() as instance:
        with in_process_test_workspace(instance, recon_repo) as workspace:
            location = workspace.get_repository_location(workspace.repository_location_names[0])
            repo_handle = RepositoryHandle(
                repository_name=repo_def.name,
                repository_location=location,
            )
            fake_external_pipeline = external_pipeline_from_recon_pipeline(
                recon_pipeline,
                solid_selection=None,
                repository_handle=repo_handle,
            )

            # Launch the run in a fake Dagster instance.
            pipeline_name = "demo_pipeline"
            run = create_run_for_test(
                instance,
                pipeline_name=pipeline_name,
                external_pipeline_origin=fake_external_pipeline.get_external_origin(),
                pipeline_code_origin=fake_external_pipeline.get_python_origin(),
            )
            k8s_run_launcher.register_instance(instance)
            k8s_run_launcher.launch_run(LaunchRunContext(run, workspace))

            updated_run = instance.get_run_by_id(run.run_id)
            assert updated_run.tags[DOCKER_IMAGE_TAG] == "fake_job_image"

        # Check that user defined k8s config was passed down to the k8s job.
        mock_method_calls = mock_k8s_client_batch_api.method_calls
        assert len(mock_method_calls) > 0
        method_name, _args, kwargs = mock_method_calls[0]
        assert method_name == "create_namespaced_job"
        assert DAGSTER_PG_PASSWORD_ENV_VAR not in [
            env.name for env in kwargs["body"].spec.template.spec.containers[0].env
        ]


@pipeline
def fake_pipeline():
    pass
