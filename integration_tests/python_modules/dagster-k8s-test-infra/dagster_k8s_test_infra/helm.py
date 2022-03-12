# pylint: disable=print-call, redefined-outer-name
import base64
import json
import os
import subprocess
import time
from contextlib import contextmanager

import boto3
import kubernetes
import pytest
import requests
import yaml
from dagster_k8s.utils import wait_for_pod

from dagster import check
from dagster._utils import find_free_port, git_repository_root, merge_dicts

from .integration_utils import IS_BUILDKITE, check_output, get_test_namespace, image_pull_policy

TEST_AWS_CONFIGMAP_NAME = "test-aws-env-configmap"
TEST_CONFIGMAP_NAME = "test-env-configmap"
TEST_OTHER_CONFIGMAP_NAME = "test-other-env-configmap"
TEST_SECRET_NAME = "test-env-secret"
TEST_OTHER_SECRET_NAME = "test-other-env-secret"

TEST_VOLUME_CONFIGMAP_NAME = "test-volume-configmap"

TEST_IMAGE_PULL_SECRET_NAME = "test-image-pull-secret"
TEST_OTHER_IMAGE_PULL_SECRET_NAME = "test-other-image-pull-secret"

# By default, dagster.workers.fullname is ReleaseName-celery-workers
CELERY_WORKER_NAME_PREFIX = "dagster-celery-workers"


@pytest.fixture(scope="session")
def should_cleanup(pytestconfig):
    if IS_BUILDKITE:
        return False
    else:
        return not pytestconfig.getoption("--no-cleanup")


@pytest.fixture(scope="session")
def namespace(pytestconfig, should_cleanup):
    """If an existing Helm chart namespace is specified via pytest CLI with the argument
    --existing-helm-namespace, we will use that chart.

    Otherwise, provision a test namespace and install Helm chart into that namespace.

    Yields the Helm chart namespace.
    """
    existing_helm_namespace = pytestconfig.getoption("--existing-helm-namespace")

    if existing_helm_namespace:
        namespace = existing_helm_namespace
    else:
        # Will be something like dagster-test-3fcd70 to avoid ns collisions in shared test environment
        namespace = get_test_namespace()

        print("--- \033[32m:k8s: Creating test namespace %s\033[0m" % namespace)
        kube_api = kubernetes.client.CoreV1Api()

        print("Creating namespace %s" % namespace)
        kube_namespace = kubernetes.client.V1Namespace(
            metadata=kubernetes.client.V1ObjectMeta(name=namespace)
        )
        kube_api.create_namespace(kube_namespace)

    yield namespace

    # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
    # w/ a kind cluster
    if should_cleanup:
        print("Deleting namespace %s" % namespace)
        kube_api.delete_namespace(name=namespace)


@pytest.fixture(scope="session")
def run_monitoring_namespace(pytestconfig, should_cleanup):
    """If an existing Helm chart namespace is specified via pytest CLI with the argument
    --existing-helm-namespace, we will use that chart.

    Otherwise, provision a test namespace and install Helm chart into that namespace.

    Yields the Helm chart namespace.
    """
    existing_helm_namespace = pytestconfig.getoption("--existing-helm-namespace")

    if existing_helm_namespace:
        namespace = existing_helm_namespace
    else:
        # Will be something like dagster-test-3fcd70 to avoid ns collisions in shared test environment
        namespace = get_test_namespace()

        print("--- \033[32m:k8s: Creating test namespace %s\033[0m" % namespace)
        kube_api = kubernetes.client.CoreV1Api()

        print("Creating namespace %s" % namespace)
        kube_namespace = kubernetes.client.V1Namespace(
            metadata=kubernetes.client.V1ObjectMeta(name=namespace)
        )
        kube_api.create_namespace(kube_namespace)

    yield namespace

    # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
    # w/ a kind cluster
    if should_cleanup:
        print("Deleting namespace %s" % namespace)
        kube_api.delete_namespace(name=namespace)


@pytest.fixture(scope="session")
def configmaps(namespace, should_cleanup):
    print(
        f"Creating k8s test object ConfigMaps: {TEST_CONFIGMAP_NAME}, {TEST_OTHER_CONFIGMAP_NAME}, {TEST_VOLUME_CONFIGMAP_NAME}"
    )
    kube_api = kubernetes.client.CoreV1Api()

    configmap = kubernetes.client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        data={"TEST_ENV_VAR": "foobar"},
        metadata=kubernetes.client.V1ObjectMeta(name=TEST_CONFIGMAP_NAME),
    )
    kube_api.create_namespaced_config_map(namespace=namespace, body=configmap)

    other_configmap = kubernetes.client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        data={"TEST_OTHER_ENV_VAR": "bazquux"},
        metadata=kubernetes.client.V1ObjectMeta(name=TEST_OTHER_CONFIGMAP_NAME),
    )
    kube_api.create_namespaced_config_map(namespace=namespace, body=other_configmap)

    volume_configmap = kubernetes.client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        data={"volume_mounted_file.yaml": "BAR_CONTENTS"},
        metadata=kubernetes.client.V1ObjectMeta(name=TEST_VOLUME_CONFIGMAP_NAME),
    )

    kube_api.create_namespaced_config_map(namespace=namespace, body=volume_configmap)

    yield

    if should_cleanup:
        kube_api.delete_namespaced_config_map(name=TEST_CONFIGMAP_NAME, namespace=namespace)
        kube_api.delete_namespaced_config_map(name=TEST_OTHER_CONFIGMAP_NAME, namespace=namespace)
        kube_api.delete_namespaced_config_map(name=TEST_OTHER_CONFIGMAP_NAME, namespace=namespace)


@pytest.fixture(scope="session")
def aws_configmap(namespace, should_cleanup):
    if not IS_BUILDKITE:
        kube_api = kubernetes.client.CoreV1Api()
        aws_data = {
            "AWS_ACCOUNT_ID": os.getenv("AWS_ACCOUNT_ID"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        }

        if not aws_data["AWS_ACCESS_KEY_ID"] or not aws_data["AWS_SECRET_ACCESS_KEY"]:
            sm_client = boto3.client("secretsmanager", region_name="us-west-1")
            try:
                creds = json.loads(
                    sm_client.get_secret_value(
                        SecretId=os.getenv("AWS_SSM_REFERENCE", "development/DOCKER_AWS_CREDENTIAL")
                    ).get("SecretString")
                )
                aws_data["AWS_ACCESS_KEY_ID"] = creds["aws_access_key_id"]
                aws_data["AWS_SECRET_ACCESS_KEY"] = creds["aws_secret_access_key"]
            except:
                raise Exception(
                    "Must have AWS credentials set in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
                    "to be able to run Helm tests locally"
                )

        print("Creating ConfigMap %s with AWS credentials" % (TEST_AWS_CONFIGMAP_NAME))
        aws_configmap = kubernetes.client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            data=aws_data,
            metadata=kubernetes.client.V1ObjectMeta(name=TEST_AWS_CONFIGMAP_NAME),
        )
        kube_api.create_namespaced_config_map(namespace=namespace, body=aws_configmap)

    yield TEST_AWS_CONFIGMAP_NAME

    if should_cleanup and not IS_BUILDKITE:
        kube_api.delete_namespaced_config_map(name=TEST_AWS_CONFIGMAP_NAME, namespace=namespace)


@pytest.fixture(scope="session")
def secrets(namespace, should_cleanup):
    print("Creating k8s test secrets")
    kube_api = kubernetes.client.CoreV1Api()

    # Secret values are expected to be base64 encoded
    secret_val = base64.b64encode(b"foobar").decode("utf-8")
    secret = kubernetes.client.V1Secret(
        api_version="v1",
        kind="Secret",
        data={"TEST_SECRET_ENV_VAR": secret_val},
        metadata=kubernetes.client.V1ObjectMeta(name=TEST_SECRET_NAME),
    )
    kube_api.create_namespaced_secret(namespace=namespace, body=secret)

    kube_api.create_namespaced_secret(
        namespace=namespace,
        body=kubernetes.client.V1Secret(
            api_version="v1",
            kind="Secret",
            data={},
            metadata=kubernetes.client.V1ObjectMeta(name=TEST_IMAGE_PULL_SECRET_NAME),
        ),
    )
    kube_api.create_namespaced_secret(
        namespace=namespace,
        body=kubernetes.client.V1Secret(
            api_version="v1",
            kind="Secret",
            data={},
            metadata=kubernetes.client.V1ObjectMeta(name=TEST_OTHER_IMAGE_PULL_SECRET_NAME),
        ),
    )

    other_secret_val = base64.b64encode(b"bazquux").decode("utf-8")
    other_secret = kubernetes.client.V1Secret(
        api_version="v1",
        kind="Secret",
        data={"TEST_OTHER_SECRET_ENV_VAR": other_secret_val},
        metadata=kubernetes.client.V1ObjectMeta(name=TEST_OTHER_SECRET_NAME),
    )

    kube_api.create_namespaced_secret(namespace=namespace, body=other_secret)

    yield

    if should_cleanup:
        kube_api.delete_namespaced_secret(name=TEST_SECRET_NAME, namespace=namespace)
        kube_api.delete_namespaced_secret(name=TEST_OTHER_SECRET_NAME, namespace=namespace)
        kube_api.delete_namespaced_secret(name=TEST_IMAGE_PULL_SECRET_NAME, namespace=namespace)
        kube_api.delete_namespaced_secret(
            name=TEST_OTHER_IMAGE_PULL_SECRET_NAME, namespace=namespace
        )


@pytest.fixture(scope="session")
def helm_namespace_for_user_deployments_subchart_disabled(
    dagster_docker_image,
    cluster_provider,
    helm_namespace_for_user_deployments_subchart,
    should_cleanup,
    configmaps,
    aws_configmap,
    secrets,
):  # pylint: disable=unused-argument
    namespace = helm_namespace_for_user_deployments_subchart

    with helm_chart_for_user_deployments_subchart_disabled(
        namespace, dagster_docker_image, should_cleanup
    ):
        print("Helm chart successfully installed in namespace %s" % namespace)
        yield namespace


@pytest.fixture(scope="session")
def helm_namespace_for_user_deployments_subchart(
    dagster_docker_image,
    cluster_provider,
    namespace,
    should_cleanup,
    configmaps,
    aws_configmap,
    secrets,
):  # pylint: disable=unused-argument
    with helm_chart_for_user_deployments_subchart(namespace, dagster_docker_image, should_cleanup):
        yield namespace


@pytest.fixture(scope="session")
def helm_namespace_for_daemon(
    dagster_docker_image,
    cluster_provider,
    namespace,
    should_cleanup,
    configmaps,
    aws_configmap,
    secrets,
):  # pylint: disable=unused-argument
    with helm_chart_for_daemon(namespace, dagster_docker_image, should_cleanup):
        yield namespace


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("rabbitmq", marks=pytest.mark.mark_rabbitmq),
        pytest.param("redis", marks=pytest.mark.mark_redis),
    ],
)
def celery_backend(request):
    return request.param


@pytest.fixture(scope="session")
def helm_namespace(
    dagster_docker_image,
    cluster_provider,
    namespace,
    should_cleanup,
    configmaps,
    aws_configmap,
    secrets,
    celery_backend,
):  # pylint: disable=unused-argument
    with helm_chart(namespace, dagster_docker_image, celery_backend, should_cleanup):
        yield namespace


@pytest.fixture(scope="session")
def helm_namespace_for_k8s_run_launcher(
    dagster_docker_image,
    cluster_provider,
    namespace,
    should_cleanup,
    configmaps,
    aws_configmap,
    secrets,
):  # pylint: disable=unused-argument
    with helm_chart_for_k8s_run_launcher(
        namespace, dagster_docker_image, should_cleanup, run_monitoring=True
    ):
        yield namespace


@contextmanager
def _helm_chart_helper(
    namespace, should_cleanup, helm_config, helm_install_name, chart_name="helm/dagster"
):
    """Install helm chart."""
    check.str_param(namespace, "namespace")
    check.bool_param(should_cleanup, "should_cleanup")
    check.str_param(helm_install_name, "helm_install_name")

    print("--- \033[32m:helm: Installing Helm chart {}\033[0m".format(helm_install_name))

    try:
        helm_config_yaml = yaml.dump(helm_config, default_flow_style=False)
        release_name = chart_name.split("/")[-1]
        helm_cmd = [
            "helm",
            "install",
            "--namespace",
            namespace,
            "--debug",
            "-f",
            "-",
            release_name,
            os.path.join(git_repository_root(), chart_name),
        ]

        print("Running Helm Install: \n", " ".join(helm_cmd), "\nWith config:\n", helm_config_yaml)

        p = subprocess.Popen(
            helm_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate(helm_config_yaml.encode("utf-8"))
        print("Helm install completed with stdout: ", stdout.decode("utf-8"))
        print("Helm install completed with stderr: ", stderr.decode("utf-8"))
        assert p.returncode == 0

        # Wait for Dagit pod to be ready (won't actually stay up w/out js rebuild)
        kube_api = kubernetes.client.CoreV1Api()

        if chart_name == "helm/dagster":
            print("Waiting for Dagit pod to be ready...")
            start_time = time.time()
            while True:
                if time.time() - start_time > 120:
                    raise Exception("No dagit pod after 2 minutes")

                pods = kube_api.list_namespaced_pod(namespace=namespace)
                pod_names = [p.metadata.name for p in pods.items if "dagit" in p.metadata.name]
                if pod_names:
                    dagit_pod = pod_names[0]
                    wait_for_pod(dagit_pod, namespace=namespace)
                    break
                time.sleep(1)

            print("Waiting for daemon pod to be ready...")
            start_time = time.time()
            while True:

                if time.time() - start_time > 120:
                    raise Exception("No daemon pod after 2 minutes")

                pods = kube_api.list_namespaced_pod(namespace=namespace)
                pod_names = [p.metadata.name for p in pods.items if "daemon" in p.metadata.name]
                if pod_names:
                    daemon_pod = pod_names[0]
                    wait_for_pod(daemon_pod, namespace=namespace)
                    break
                time.sleep(1)

            # Wait for Celery worker queues to become ready
            pods = kubernetes.client.CoreV1Api().list_namespaced_pod(namespace=namespace)
            deployments = kubernetes.client.AppsV1Api().list_namespaced_deployment(
                namespace=namespace
            )

            pod_names = [
                p.metadata.name for p in pods.items if CELERY_WORKER_NAME_PREFIX in p.metadata.name
            ]
            if helm_config.get("runLauncher").get("type") == "CeleryK8sRunLauncher":
                worker_queues = (
                    helm_config.get("runLauncher")
                    .get("config")
                    .get("celeryK8sRunLauncher")
                    .get("workerQueues", [])
                )
                for queue in worker_queues:
                    num_pods_for_queue = len(
                        [
                            pod_name
                            for pod_name in pod_names
                            if f"{CELERY_WORKER_NAME_PREFIX}-{queue.get('name')}" in pod_name
                        ]
                    )
                    assert queue.get("replicaCount") == num_pods_for_queue

                    labels = queue.get("labels")
                    if labels:
                        target_deployments = []
                        for item in deployments.items:
                            if queue.get("name") in item.metadata.name:
                                target_deployments.append(item)

                        assert len(target_deployments) > 0
                        for target in target_deployments:
                            for key in labels:
                                assert target.spec.template.metadata.labels.get(key) == labels.get(
                                    key
                                )

                print("Waiting for celery workers")
                for pod_name in pod_names:
                    print("Waiting for Celery worker pod %s" % pod_name)
                    wait_for_pod(pod_name, namespace=namespace)

                rabbitmq_enabled = "rabbitmq" in helm_config and helm_config["rabbitmq"].get(
                    "enabled"
                )
                redis_enabled = "redis" in helm_config and helm_config["redis"].get("enabled")

                if rabbitmq_enabled:
                    print("Waiting for rabbitmq pod to exist...")
                    start_time = time.time()
                    while True:
                        if time.time() - start_time > 120:
                            raise Exception("No rabbitmq pod after 2 minutes")

                        pods = kube_api.list_namespaced_pod(namespace=namespace)
                        pod_names = [
                            p.metadata.name for p in pods.items if "rabbitmq" in p.metadata.name
                        ]
                        if pod_names:
                            assert len(pod_names) == 1
                            print("Waiting for rabbitmq pod to be ready: " + str(pod_names[0]))

                            wait_for_pod(pod_names[0], namespace=namespace)
                            break
                        time.sleep(1)

                if redis_enabled:
                    print("Waiting for redis pods to exist...")
                    start_time = time.time()
                    while True:
                        if time.time() - start_time > 120:
                            raise Exception("No redis pods after 2 minutes")

                        pods = kube_api.list_namespaced_pod(namespace=namespace)
                        pod_names = [
                            p.metadata.name for p in pods.items if "redis" in p.metadata.name
                        ]
                        if pod_names and len(pod_names) >= 1:
                            for pod_name in pod_names:
                                print("Waiting for redis pod to be ready: " + str(pod_name))
                                wait_for_pod(pod_name, namespace=namespace)
                            break
                        time.sleep(5)

            else:
                assert (
                    len(pod_names) == 0
                ), "celery-worker pods {pod_names} exists when celery is not enabled.".format(
                    pod_names=pod_names
                )

        dagster_user_deployments_values = helm_config.get("dagster-user-deployments", {})
        if (
            dagster_user_deployments_values.get("enabled")
            and dagster_user_deployments_values.get("enableSubchart")
            or release_name == "dagster"
        ):
            # Wait for user code deployments to be ready
            print("Waiting for user code deployments")
            pods = kubernetes.client.CoreV1Api().list_namespaced_pod(namespace=namespace)
            pod_names = [
                p.metadata.name for p in pods.items if "user-code-deployment" in p.metadata.name
            ]
            for pod_name in pod_names:
                print("Waiting for user code deployment pod %s" % pod_name)
                wait_for_pod(pod_name, namespace=namespace)

        print("Helm chart successfully installed in namespace %s" % namespace)
        yield

    finally:
        # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
        # w/ a kind cluster
        if should_cleanup:
            print("Uninstalling helm chart")
            check_output(
                ["helm", "uninstall", release_name, "--namespace", namespace],
                cwd=git_repository_root(),
            )


@contextmanager
def helm_chart(namespace, docker_image, celery_backend, should_cleanup=True):
    check.str_param(namespace, "namespace")
    check.str_param(docker_image, "docker_image")
    check.bool_param(should_cleanup, "should_cleanup")

    if celery_backend == "rabbitmq":
        additional_config = {}
    elif celery_backend == "redis":
        additional_config = {
            "rabbitmq": {
                "enabled": False,
            },
            "redis": {
                "enabled": True,
                "internal": True,
                "host": "dagster-redis-master",
                "cluster": {"enabled": False},
            },
        }
    else:
        raise Exception(f"Unexpected Celery backend {celery_backend}")

    helm_config = merge_dicts(_base_helm_config(docker_image), additional_config)

    with _helm_chart_helper(namespace, should_cleanup, helm_config, helm_install_name="helm_chart"):
        yield


@contextmanager
def helm_chart_for_k8s_run_launcher(
    namespace, docker_image, should_cleanup=True, run_monitoring=False
):
    check.str_param(namespace, "namespace")
    check.str_param(docker_image, "docker_image")
    check.bool_param(should_cleanup, "should_cleanup")

    repository, tag = docker_image.split(":")
    pull_policy = image_pull_policy()
    helm_config = merge_dicts(
        _base_helm_config(docker_image),
        {
            "flower": {
                "enabled": False,
            },
            "runLauncher": {
                "type": "K8sRunLauncher",
                "config": {
                    "k8sRunLauncher": {
                        "jobNamespace": namespace,
                        "envConfigMaps": [{"name": TEST_CONFIGMAP_NAME}]
                        + ([{"name": TEST_AWS_CONFIGMAP_NAME}] if not IS_BUILDKITE else []),
                        "envSecrets": [{"name": TEST_SECRET_NAME}],
                        "envVars": ["BUILDKITE"],
                        "imagePullPolicy": image_pull_policy(),
                        "volumeMounts": [
                            {
                                "name": "test-volume",
                                "mountPath": "/opt/dagster/test_mount_path/volume_mounted_file.yaml",
                                "subPath": "volume_mounted_file.yaml",
                            }
                        ],
                        "volumes": [
                            {
                                "name": "test-volume",
                                "configMap": {"name": TEST_VOLUME_CONFIGMAP_NAME},
                            }
                        ],
                        "labels": {
                            "run_launcher_label_key": "run_launcher_label_value",
                        },
                    }
                },
            },
            "rabbitmq": {"enabled": False},
            "dagsterDaemon": {
                "enabled": True,
                "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                "heartbeatTolerance": 180,
                "runCoordinator": {"enabled": False},  # No run queue
                "env": ({"BUILDKITE": os.getenv("BUILDKITE")} if os.getenv("BUILDKITE") else {}),
                "envConfigMaps": [{"name": TEST_CONFIGMAP_NAME}],
                "envSecrets": [{"name": TEST_SECRET_NAME}],
                "annotations": {"dagster-integration-tests": "daemon-pod-annotation"},
                "runMonitoring": {"enabled": True, "pollIntervalSeconds": 5}
                if run_monitoring
                else {},
            },
            "imagePullSecrets": [{"name": TEST_IMAGE_PULL_SECRET_NAME}],
        },
    )

    with _helm_chart_helper(
        namespace, should_cleanup, helm_config, helm_install_name="helm_chart_for_k8s_run_launcher"
    ):
        yield


@contextmanager
def helm_chart_for_user_deployments_subchart_disabled(namespace, docker_image, should_cleanup=True):
    check.str_param(namespace, "namespace")
    check.str_param(docker_image, "docker_image")
    check.bool_param(should_cleanup, "should_cleanup")

    repository, tag = docker_image.split(":")
    pull_policy = image_pull_policy()
    helm_config = merge_dicts(
        _base_helm_config(docker_image),
        {
            "dagster-user-deployments": {
                "enabled": True,
                "enableSubchart": False,
                "deployments": [
                    {
                        "name": "user-code-deployment-1",
                        "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                        "dagsterApiGrpcArgs": [
                            "-m",
                            "dagster_test.test_project.test_pipelines.repo",
                            "-a",
                            "define_demo_execution_repo",
                        ],
                        "port": 3030,
                        "env": (
                            {"BUILDKITE": os.getenv("BUILDKITE")} if os.getenv("BUILDKITE") else {}
                        ),
                        "annotations": {"dagster-integration-tests": "ucd-1-pod-annotation"},
                        "service": {
                            "annotations": {"dagster-integration-tests": "ucd-1-svc-annotation"}
                        },
                        "volumeMounts": [
                            {
                                "name": "test-volume",
                                "mountPath": "/opt/dagster/test_mount_path/volume_mounted_file.yaml",
                                "subPath": "volume_mounted_file.yaml",
                            }
                        ],
                        "volumes": [
                            {
                                "name": "test-volume",
                                "configMap": {"name": TEST_VOLUME_CONFIGMAP_NAME},
                            }
                        ],
                    }
                ],
            },
        },
    )

    with _helm_chart_helper(
        namespace,
        should_cleanup,
        helm_config,
        helm_install_name="helm_chart_for_user_deployments_subchart_disabled",
    ):
        yield


@contextmanager
def helm_chart_for_user_deployments_subchart(namespace, docker_image, should_cleanup=True):
    check.str_param(namespace, "namespace")
    check.str_param(docker_image, "docker_image")
    check.bool_param(should_cleanup, "should_cleanup")

    repository, tag = docker_image.split(":")
    pull_policy = image_pull_policy()
    helm_config = {
        "deployments": [
            {
                "name": "user-code-deployment-1",
                "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                "dagsterApiGrpcArgs": [
                    "-m",
                    "dagster_test.test_project.test_pipelines.repo",
                    "-a",
                    "define_demo_execution_repo",
                ],
                "port": 3030,
            }
        ],
    }

    with _helm_chart_helper(
        namespace,
        should_cleanup,
        helm_config,
        helm_install_name="helm_chart_for_user_deployments_subchart",
        chart_name="helm/dagster/charts/dagster-user-deployments",
    ):
        yield


def _base_helm_config(docker_image):
    repository, tag = docker_image.split(":")
    pull_policy = image_pull_policy()
    return {
        "dagster-user-deployments": {
            "enabled": True,
            "enableSubchart": True,
            "deployments": [
                {
                    "name": "user-code-deployment-1",
                    "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                    "dagsterApiGrpcArgs": [
                        "-m",
                        "dagster_test.test_project.test_pipelines.repo",
                        "-a",
                        "define_demo_execution_repo",
                    ],
                    "port": 3030,
                    "env": (
                        {"BUILDKITE": os.getenv("BUILDKITE")} if os.getenv("BUILDKITE") else {}
                    ),
                    "envConfigMaps": (
                        [{"name": TEST_AWS_CONFIGMAP_NAME}] if not IS_BUILDKITE else []
                    ),
                    "annotations": {"dagster-integration-tests": "ucd-1-pod-annotation"},
                    "service": {
                        "annotations": {"dagster-integration-tests": "ucd-1-svc-annotation"}
                    },
                    "volumeMounts": [
                        {
                            "name": "test-volume",
                            "mountPath": "/opt/dagster/test_mount_path/volume_mounted_file.yaml",
                            "subPath": "volume_mounted_file.yaml",
                        }
                    ],
                    "volumes": [
                        {"name": "test-volume", "configMap": {"name": TEST_VOLUME_CONFIGMAP_NAME}}
                    ],
                }
            ],
        },
        "dagit": {
            "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
            "env": {"TEST_SET_ENV_VAR": "test_dagit_env_var"},
            "envConfigMaps": [{"name": TEST_CONFIGMAP_NAME}],
            "envSecrets": [{"name": TEST_SECRET_NAME}],
            "annotations": {"dagster-integration-tests": "dagit-pod-annotation"},
            "service": {"annotations": {"dagster-integration-tests": "dagit-svc-annotation"}},
        },
        "flower": {
            "enabled": True,
            "livenessProbe": {
                "tcpSocket": {"port": "flower"},
                "periodSeconds": 20,
                "failureThreshold": 3,
            },
            "startupProbe": {
                "tcpSocket": {"port": "flower"},
                "failureThreshold": 6,
                "periodSeconds": 10,
            },
        },
        "runLauncher": {
            "type": "CeleryK8sRunLauncher",
            "config": {
                "celeryK8sRunLauncher": {
                    "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                    "workerQueues": [
                        {"name": "dagster", "replicaCount": 2},
                        {
                            "name": "extra-queue-1",
                            "replicaCount": 1,
                            "labels": {"celery-label-key": "celery-label-value"},
                            "additionalCeleryArgs": ["-E", "--concurrency", "3"],
                        },
                    ],
                    "livenessProbe": {
                        "initialDelaySeconds": 15,
                        "periodSeconds": 10,
                        "timeoutSeconds": 10,
                        "successThreshold": 1,
                        "failureThreshold": 3,
                    },
                    "configSource": {
                        "broker_transport_options": {"priority_steps": [9]},
                        "worker_concurrency": 1,
                    },
                    "annotations": {"dagster-integration-tests": "celery-pod-annotation"},
                    "envConfigMaps": (
                        [{"name": TEST_AWS_CONFIGMAP_NAME}] if not IS_BUILDKITE else []
                    ),
                    "env": {"TEST_SET_ENV_VAR": "test_celery_env_var"},
                    "envSecrets": [{"name": TEST_SECRET_NAME}],
                    "volumeMounts": [
                        {
                            "name": "test-volume",
                            "mountPath": "/opt/dagster/test_mount_path/volume_mounted_file.yaml",
                            "subPath": "volume_mounted_file.yaml",
                        }
                    ],
                    "volumes": [
                        {
                            "name": "test-volume",
                            "configMap": {"name": TEST_VOLUME_CONFIGMAP_NAME},
                        }
                    ],
                    "labels": {
                        "run_launcher_label_key": "run_launcher_label_value",
                    },
                },
            },
        },
        "rabbitmq": {"enabled": True},
        "ingress": {
            "enabled": True,
            "dagit": {"host": "dagit.example.com"},
            "flower": {"flower": "flower.example.com"},
        },
        "scheduler": {"type": "DagsterDaemonScheduler", "config": {}},
        "serviceAccount": {"name": "dagit-admin"},
        "postgresqlPassword": "test",
        "postgresqlDatabase": "test",
        "postgresqlUser": "test",
        "dagsterDaemon": {
            "enabled": True,
            "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
            "heartbeatTolerance": 180,
            "runCoordinator": {"enabled": False},  # No run queue
            "env": ({"BUILDKITE": os.getenv("BUILDKITE")} if os.getenv("BUILDKITE") else {}),
            "envConfigMaps": [{"name": TEST_CONFIGMAP_NAME}],
            "envSecrets": [{"name": TEST_SECRET_NAME}],
            "annotations": {"dagster-integration-tests": "daemon-pod-annotation"},
            "runMonitoring": {
                "enabled": True,
                "pollIntervalSeconds": 5,
            },
        },
        # Used to set the environment variables in dagster.shared_env that determine the run config
        "pipelineRun": {"image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy}},
    }


@contextmanager
def helm_chart_for_daemon(namespace, docker_image, should_cleanup=True):
    check.str_param(namespace, "namespace")
    check.str_param(docker_image, "docker_image")
    check.bool_param(should_cleanup, "should_cleanup")

    repository, tag = docker_image.split(":")
    pull_policy = image_pull_policy()

    helm_config = merge_dicts(
        _base_helm_config(docker_image),
        {
            "dagsterDaemon": {
                "enabled": True,
                "image": {"repository": repository, "tag": tag, "pullPolicy": pull_policy},
                "heartbeatTolerance": 180,
                "runCoordinator": {"enabled": True},
                "env": ({"BUILDKITE": os.getenv("BUILDKITE")} if os.getenv("BUILDKITE") else {}),
                "envConfigMaps": [{"name": TEST_CONFIGMAP_NAME}],
                "envSecrets": [{"name": TEST_SECRET_NAME}],
                "annotations": {"dagster-integration-tests": "daemon-pod-annotation"},
            },
        },
    )
    with _helm_chart_helper(
        namespace, should_cleanup, helm_config, helm_install_name="helm_chart_for_daemon"
    ):
        yield


@pytest.fixture(scope="session")
def dagit_url(helm_namespace):
    with _port_forward_dagit(namespace=helm_namespace) as forwarded_dagit_url:
        yield forwarded_dagit_url


@pytest.fixture(scope="session")
def dagit_url_for_daemon(helm_namespace_for_daemon):
    with _port_forward_dagit(namespace=helm_namespace_for_daemon) as forwarded_dagit_url:
        yield forwarded_dagit_url


@pytest.fixture(scope="session")
def dagit_url_for_k8s_run_launcher(helm_namespace_for_k8s_run_launcher):
    with _port_forward_dagit(namespace=helm_namespace_for_k8s_run_launcher) as forwarded_dagit_url:
        yield forwarded_dagit_url


@contextmanager
def _port_forward_dagit(namespace):
    print("Port-forwarding dagit")
    kube_api = kubernetes.client.CoreV1Api()

    pods = kube_api.list_namespaced_pod(namespace=namespace)
    pod_names = [p.metadata.name for p in pods.items if "dagit" in p.metadata.name]

    if not pod_names:
        raise Exception("No pods with dagit in name")

    dagit_pod_name = pod_names[0]

    forward_port = find_free_port()

    try:
        p = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                "--namespace",
                namespace,
                dagit_pod_name,
                "{forward_port}:80".format(forward_port=forward_port),
            ],
            # Squelch the verbose "Handling connection for..." messages
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

        dagit_url = f"localhost:{forward_port}"

        # Validate port forwarding works
        start = time.time()

        while True:
            if time.time() - start > 60:
                raise Exception("Timed out while waiting for dagit port forwarding")

            print(
                "Waiting for port forwarding from k8s pod %s:80 to localhost:%d to be"
                " available..." % (dagit_pod_name, forward_port)
            )
            try:
                check_output(["curl", f"http://{dagit_url}"])
                break
            except Exception:
                time.sleep(1)

        print("Port forwarding in the backgound. Trying to connect to Dagit...")

        start_time = time.time()

        while True:
            if time.time() - start_time > 30:
                raise Exception("Timed out waiting for dagit server to be available")

            try:
                sanity_check = requests.get(f"http://{dagit_url}/dagit_info")
                assert "dagster" in sanity_check.text
                print("Connected to dagit, beginning tests for versions")
                break
            except requests.exceptions.ConnectionError:
                pass

            time.sleep(1)
        yield f"http://{dagit_url}"

    finally:
        print("Terminating port-forwarding")
        p.terminate()
