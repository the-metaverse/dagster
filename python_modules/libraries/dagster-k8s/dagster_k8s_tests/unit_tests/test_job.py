import pytest
from dagster_k8s import DagsterK8sJobConfig, construct_dagster_k8s_job
from dagster_k8s.job import (
    DAGSTER_PG_PASSWORD_ENV_VAR,
    DEFAULT_K8S_JOB_TTL_SECONDS_AFTER_FINISHED,
    USER_DEFINED_K8S_CONFIG_KEY,
    UserDefinedDagsterK8sConfig,
    get_user_defined_k8s_config,
)

from dagster import __version__ as dagster_version
from dagster import graph
from dagster._core.test_utils import environ, remove_none_recursively
from dagster.utils import merge_dicts


def test_job_serialization():

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        image_pull_policy="Always",
        image_pull_secrets=[{"name": "my_secret"}],
        service_account_name=None,
        instance_config_map="some-instance-configmap",
        postgres_password_secret="some-secret-name",
        env_config_maps=None,
        env_secrets=None,
    )
    assert DagsterK8sJobConfig.from_dict(cfg.to_dict()) == cfg


def test_user_defined_k8s_config_serialization():
    cfg = UserDefinedDagsterK8sConfig(
        container_config={
            "resources": {
                "requests": {"cpu": "250m", "memory": "64Mi"},
                "limits": {"cpu": "500m", "memory": "2560Mi"},
            }
        },
        pod_template_spec_metadata={"namespace": "value"},
        pod_spec_config={"dns_policy": "value"},
        job_config={"status": {"completed_indexes": "value"}},
        job_metadata={"namespace": "value"},
        job_spec_config={"backoff_limit": 120},
    )

    assert UserDefinedDagsterK8sConfig.from_dict(cfg.to_dict()) == cfg


def test_construct_dagster_k8s_job():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        image_pull_policy="Always",
        image_pull_secrets=[{"name": "my_secret"}],
        service_account_name=None,
        instance_config_map="some-instance-configmap",
        postgres_password_secret="postgres-bizbuz",
        env_config_maps=None,
        env_secrets=None,
    )
    job = construct_dagster_k8s_job(cfg, ["foo", "bar"], "job123").to_dict()
    assert job["kind"] == "Job"
    assert job["metadata"]["name"] == "job123"
    assert job["spec"]["template"]["spec"]["containers"][0]["image"] == "test/foo:latest"
    assert DAGSTER_PG_PASSWORD_ENV_VAR in [
        env["name"] for env in job["spec"]["template"]["spec"]["containers"][0]["env"]
    ]


def test_construct_dagster_k8s_job_no_postgres():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        image_pull_policy="Always",
        image_pull_secrets=[{"name": "my_secret"}],
        service_account_name=None,
        instance_config_map="some-instance-configmap",
        postgres_password_secret=None,
        env_config_maps=None,
        env_secrets=None,
    )
    job = construct_dagster_k8s_job(cfg, ["foo", "bar"], "job123").to_dict()
    assert job["kind"] == "Job"
    assert job["metadata"]["name"] == "job123"
    assert job["spec"]["template"]["spec"]["containers"][0]["image"] == "test/foo:latest"
    assert DAGSTER_PG_PASSWORD_ENV_VAR not in [
        env["name"] for env in job["spec"]["template"]["spec"]["containers"][0]["env"]
    ]


def test_construct_dagster_k8s_job_with_mounts():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        image_pull_policy="Always",
        image_pull_secrets=[{"name": "my_secret"}],
        service_account_name=None,
        instance_config_map="some-instance-configmap",
        postgres_password_secret=None,
        env_config_maps=None,
        env_secrets=None,
        volume_mounts=[{"name": "foo", "mountPath": "biz/buz", "subPath": "file.txt"}],
        volumes=[
            {"name": "foo", "configMap": {"name": "settings-cm"}},
        ],
    )
    job = construct_dagster_k8s_job(cfg, ["foo", "bar"], "job123").to_dict()

    assert len(job["spec"]["template"]["spec"]["volumes"]) == 2
    foo_volumes = [
        volume for volume in job["spec"]["template"]["spec"]["volumes"] if volume["name"] == "foo"
    ]
    assert len(foo_volumes) == 1
    assert foo_volumes[0]["config_map"]["name"] == "settings-cm"

    assert len(job["spec"]["template"]["spec"]["containers"][0]["volume_mounts"]) == 2
    foo_volumes_mounts = [
        volume
        for volume in job["spec"]["template"]["spec"]["containers"][0]["volume_mounts"]
        if volume["name"] == "foo"
    ]
    assert len(foo_volumes_mounts) == 1

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        image_pull_policy="Always",
        image_pull_secrets=[{"name": "my_secret"}],
        service_account_name=None,
        instance_config_map="some-instance-configmap",
        postgres_password_secret=None,
        env_config_maps=None,
        env_secrets=None,
        volume_mounts=[{"name": "foo", "mountPath": "biz/buz", "subPath": "file.txt"}],
        volumes=[
            {"name": "foo", "secret": {"secretName": "settings-secret"}},
        ],
    )
    job = construct_dagster_k8s_job(cfg, ["foo", "bar"], "job123").to_dict()
    assert len(job["spec"]["template"]["spec"]["volumes"]) == 2
    foo_volumes = [
        volume for volume in job["spec"]["template"]["spec"]["volumes"] if volume["name"] == "foo"
    ]
    assert len(foo_volumes) == 1
    assert foo_volumes[0]["secret"]["secret_name"] == "settings-secret"

    with pytest.raises(Exception, match="Unexpected keys in model class V1Volume: {'invalidKey'}"):
        DagsterK8sJobConfig(
            job_image="test/foo:latest",
            dagster_home="/opt/dagster/dagster_home",
            image_pull_policy="Always",
            image_pull_secrets=[{"name": "my_secret"}],
            service_account_name=None,
            instance_config_map="some-instance-configmap",
            postgres_password_secret=None,
            env_config_maps=None,
            env_secrets=None,
            volume_mounts=[{"name": "foo", "mountPath": "biz/buz", "subPath": "file.txt"}],
            volumes=[
                {"name": "foo", "invalidKey": "settings-secret"},
            ],
        )


def test_construct_dagster_k8s_job_with_env():
    with environ({"ENV_VAR_1": "one", "ENV_VAR_2": "two"}):
        cfg = DagsterK8sJobConfig(
            job_image="test/foo:latest",
            dagster_home="/opt/dagster/dagster_home",
            instance_config_map="some-instance-configmap",
            env_vars=["ENV_VAR_1", "ENV_VAR_2"],
        )

        job = construct_dagster_k8s_job(cfg, ["foo", "bar"], "job").to_dict()

        env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        env_mapping = {env_var["name"]: env_var for env_var in env}

        # Has DAGSTER_HOME and two additional env vars
        assert len(env_mapping) == 3
        assert env_mapping["ENV_VAR_1"]["value"] == "one"
        assert env_mapping["ENV_VAR_2"]["value"] == "two"


def test_construct_dagster_k8s_job_with_user_defined_env_camelcase():
    @graph
    def user_defined_k8s_env_tags_graph():
        pass

    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_env_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "container_config": {
                        "env": [
                            {"name": "ENV_VAR_1", "value": "one"},
                            {"name": "ENV_VAR_2", "value": "two"},
                            {
                                "name": "DD_AGENT_HOST",
                                "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}},
                            },
                        ]
                    }
                }
            }
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    env = job["spec"]["template"]["spec"]["containers"][0]["env"]
    env_mapping = remove_none_recursively({env_var["name"]: env_var for env_var in env})

    # Has DAGSTER_HOME and three additional env vars
    assert len(env_mapping) == 4
    assert env_mapping["ENV_VAR_1"]["value"] == "one"
    assert env_mapping["ENV_VAR_2"]["value"] == "two"
    assert env_mapping["DD_AGENT_HOST"]["value_from"] == {
        "field_ref": {"field_path": "status.hostIP"}
    }


def test_construct_dagster_k8s_job_with_user_defined_env_snake_case():
    @graph
    def user_defined_k8s_env_from_tags_graph():
        pass

    # These fields still work even when using underscore keys
    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_env_from_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "container_config": {
                        "env_from": [
                            {
                                "config_map_ref": {
                                    "name": "user_config_map_ref",
                                    "optional": "True",
                                }
                            },
                            {"secret_ref": {"name": "user_secret_ref_one", "optional": "True"}},
                            {
                                "secret_ref": {
                                    "name": "user_secret_ref_two",
                                    "optional": "False",
                                },
                                "prefix": "with_prefix",
                            },
                        ]
                    }
                }
            }
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
        env_config_maps=["config_map"],
        env_secrets=["secret"],
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    env_from = job["spec"]["template"]["spec"]["containers"][0]["env_from"]
    env_from_mapping = {
        (env_var.get("config_map_ref") or env_var.get("secret_ref")).get("name"): env_var
        for env_var in env_from
    }

    assert len(env_from_mapping) == 5
    assert env_from_mapping["config_map"]
    assert env_from_mapping["user_config_map_ref"]
    assert env_from_mapping["secret"]
    assert env_from_mapping["user_secret_ref_one"]
    assert env_from_mapping["user_secret_ref_two"]


def test_construct_dagster_k8s_job_with_user_defined_env_from():
    @graph
    def user_defined_k8s_env_from_tags_graph():
        pass

    # These fields still work even when using underscore keys
    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_env_from_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "container_config": {
                        "envFrom": [
                            {
                                "configMapRef": {
                                    "name": "user_config_map_ref",
                                    "optional": "True",
                                }
                            },
                            {"secretRef": {"name": "user_secret_ref_one", "optional": "True"}},
                            {
                                "secretRef": {
                                    "name": "user_secret_ref_two",
                                    "optional": "False",
                                },
                                "prefix": "with_prefix",
                            },
                        ]
                    }
                }
            }
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
        env_config_maps=["config_map"],
        env_secrets=["secret"],
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    env_from = job["spec"]["template"]["spec"]["containers"][0]["env_from"]
    env_from_mapping = {
        (env_var.get("config_map_ref") or env_var.get("secret_ref")).get("name"): env_var
        for env_var in env_from
    }

    assert len(env_from_mapping) == 5
    assert env_from_mapping["config_map"]
    assert env_from_mapping["user_config_map_ref"]
    assert env_from_mapping["secret"]
    assert env_from_mapping["user_secret_ref_one"]
    assert env_from_mapping["user_secret_ref_two"]


def test_construct_dagster_k8s_job_with_user_defined_volume_mounts_snake_case():
    @graph
    def user_defined_k8s_volume_mounts_tags_graph():
        pass

    # volume_mounts still work even when using underscore keys
    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_volume_mounts_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "container_config": {
                        "volume_mounts": [
                            {
                                "mountPath": "mount_path",
                                "mountPropagation": "mount_propagation",
                                "name": "a_volume_mount_one",
                                "readOnly": "False",
                                "subPath": "path/",
                            },
                            {
                                "mountPath": "mount_path",
                                "mountPropagation": "mount_propagation",
                                "name": "a_volume_mount_two",
                                "readOnly": "False",
                                "subPathExpr": "path/",
                            },
                        ]
                    }
                }
            }
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    volume_mounts = job["spec"]["template"]["spec"]["containers"][0]["volume_mounts"]
    volume_mounts_mapping = {volume_mount["name"]: volume_mount for volume_mount in volume_mounts}

    assert len(volume_mounts_mapping) == 3
    assert volume_mounts_mapping["dagster-instance"]
    assert volume_mounts_mapping["a_volume_mount_one"]
    assert volume_mounts_mapping["a_volume_mount_two"]


def test_construct_dagster_k8s_job_with_user_defined_volume_mounts_camel_case():
    @graph
    def user_defined_k8s_volume_mounts_tags_graph():
        pass

    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_volume_mounts_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "container_config": {
                        "volumeMounts": [
                            {
                                "mountPath": "mount_path",
                                "mountPropagation": "mount_propagation",
                                "name": "a_volume_mount_one",
                                "readOnly": "False",
                                "subPath": "path/",
                            },
                            {
                                "mountPath": "mount_path",
                                "mountPropagation": "mount_propagation",
                                "name": "a_volume_mount_two",
                                "readOnly": "False",
                                "subPathExpr": "path/",
                            },
                        ]
                    }
                }
            }
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    volume_mounts = job["spec"]["template"]["spec"]["containers"][0]["volume_mounts"]
    volume_mounts_mapping = {volume_mount["name"]: volume_mount for volume_mount in volume_mounts}

    assert len(volume_mounts_mapping) == 3
    assert volume_mounts_mapping["dagster-instance"]
    assert volume_mounts_mapping["a_volume_mount_one"]
    assert volume_mounts_mapping["a_volume_mount_two"]


def test_construct_dagster_k8s_job_with_user_defined_service_account_name_snake_case():
    @graph
    def user_defined_k8s_service_account_name_tags_graph():
        pass

    # service_account_name still works
    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_service_account_name_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "pod_spec_config": {
                        "service_account_name": "this-should-take-precedence",
                    },
                },
            },
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
        service_account_name="this-should-be-overriden",
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    service_account_name = job["spec"]["template"]["spec"]["service_account_name"]
    assert service_account_name == "this-should-take-precedence"


def test_construct_dagster_k8s_job_with_user_defined_service_account_name():
    @graph
    def user_defined_k8s_service_account_name_tags_graph():
        pass

    user_defined_k8s_config = get_user_defined_k8s_config(
        user_defined_k8s_service_account_name_tags_graph.to_job(
            tags={
                USER_DEFINED_K8S_CONFIG_KEY: {
                    "pod_spec_config": {
                        "serviceAccountName": "this-should-take-precedence",
                    },
                },
            },
        ).tags
    )

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="some-instance-configmap",
        service_account_name="this-should-be-overriden",
    )

    job = construct_dagster_k8s_job(
        cfg, ["foo", "bar"], "job", user_defined_k8s_config=user_defined_k8s_config
    ).to_dict()

    service_account_name = job["spec"]["template"]["spec"]["service_account_name"]
    assert service_account_name == "this-should-take-precedence"


def test_construct_dagster_k8s_job_with_ttl_snake_case():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="test",
    )
    job = construct_dagster_k8s_job(cfg, [], "job123").to_dict()

    assert job["spec"]["ttl_seconds_after_finished"] == DEFAULT_K8S_JOB_TTL_SECONDS_AFTER_FINISHED

    # Setting ttl_seconds_after_finished still works
    user_defined_cfg = UserDefinedDagsterK8sConfig(
        job_spec_config={"ttl_seconds_after_finished": 0},
    )
    job = construct_dagster_k8s_job(
        cfg, [], "job123", user_defined_k8s_config=user_defined_cfg
    ).to_dict()
    assert job["spec"]["ttl_seconds_after_finished"] == 0


def test_construct_dagster_k8s_job_with_ttl():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="test",
    )
    job = construct_dagster_k8s_job(cfg, [], "job123").to_dict()

    assert job["spec"]["ttl_seconds_after_finished"] == DEFAULT_K8S_JOB_TTL_SECONDS_AFTER_FINISHED

    user_defined_cfg = UserDefinedDagsterK8sConfig(
        job_spec_config={"ttlSecondsAfterFinished": 0},
    )
    job = construct_dagster_k8s_job(
        cfg, [], "job123", user_defined_k8s_config=user_defined_cfg
    ).to_dict()
    assert job["spec"]["ttl_seconds_after_finished"] == 0


def test_construct_dagster_k8s_job_with_sidecar_container():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="test",
    )
    job = construct_dagster_k8s_job(cfg, [], "job123").to_dict()

    assert job["spec"]["ttl_seconds_after_finished"] == DEFAULT_K8S_JOB_TTL_SECONDS_AFTER_FINISHED

    user_defined_cfg = UserDefinedDagsterK8sConfig(
        pod_spec_config={
            "containers": [{"command": ["echo", "HI"], "image": "sidecar:bar", "name": "sidecar"}]
        },
    )
    job = construct_dagster_k8s_job(
        cfg, [], "job123", user_defined_k8s_config=user_defined_cfg
    ).to_dict()

    containers = job["spec"]["template"]["spec"]["containers"]

    assert len(containers) == 2

    assert containers[0]["image"] == "test/foo:latest"

    assert containers[1]["image"] == "sidecar:bar"
    assert containers[1]["command"] == ["echo", "HI"]
    assert containers[1]["name"] == "sidecar"


def test_construct_dagster_k8s_job_with_invalid_key_raises_error():
    with pytest.raises(
        Exception, match="Unexpected keys in model class V1JobSpec: {'nonExistantKey'}"
    ):
        UserDefinedDagsterK8sConfig(
            job_spec_config={"nonExistantKey": "nonExistantValue"},
        )


def test_construct_dagster_k8s_job_with_labels():
    common_labels = {
        "app.kubernetes.io/name": "dagster",
        "app.kubernetes.io/instance": "dagster",
        "app.kubernetes.io/version": dagster_version,
        "app.kubernetes.io/part-of": "dagster",
    }

    job_config_labels = {
        "foo_label_key": "bar_label_value",
    }

    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="test",
        labels=job_config_labels,
    )
    job1 = construct_dagster_k8s_job(
        cfg,
        [],
        "job123",
        labels={
            "dagster/job": "some_job",
            "dagster/op": "some_op",
        },
    ).to_dict()
    expected_labels1 = dict(
        **common_labels,
        **{
            "dagster/job": "some_job",
            "dagster/op": "some_op",
        },
    )

    assert job1["metadata"]["labels"] == expected_labels1
    assert job1["spec"]["template"]["metadata"]["labels"] == merge_dicts(
        expected_labels1,
        job_config_labels,
    )

    job2 = construct_dagster_k8s_job(
        cfg,
        [],
        "job456",
        labels={
            "dagster/job": "long_job_name_64____01234567890123456789012345678901234567890123",
            "dagster/op": "long_op_name_64_____01234567890123456789012345678901234567890123",
        },
    ).to_dict()
    expected_labels2 = dict(
        **common_labels,
        **{
            # The last character should be truncated.
            "dagster/job": "long_job_name_64____0123456789012345678901234567890123456789012",
            "dagster/op": "long_op_name_64_____0123456789012345678901234567890123456789012",
        },
    )
    assert job2["metadata"]["labels"] == expected_labels2
    assert job2["spec"]["template"]["metadata"]["labels"] == merge_dicts(
        expected_labels2,
        job_config_labels,
    )


def test_sanitize_labels():
    cfg = DagsterK8sJobConfig(
        job_image="test/foo:latest",
        dagster_home="/opt/dagster/dagster_home",
        instance_config_map="test",
    )

    job = construct_dagster_k8s_job(
        cfg,
        [],
        "job456",
        labels={
            "dagster/op": "-get_f\o.o[bar-0]-",  # pylint: disable=anomalous-backslash-in-string
            "my_label": "_WhatsUP",
        },
    ).to_dict()

    assert job["metadata"]["labels"]["dagster/op"] == "get_f-o.o-bar-0"
    assert job["metadata"]["labels"]["my_label"] == "WhatsUP"
