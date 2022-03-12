from dagster import InputDefinition, Nothing, OutputDefinition
from dagster import _check as check
from dagster import op, solid

from .configs import define_spark_config


def create_spark_solid(
    name, main_class, description=None, required_resource_keys=frozenset(["spark"])
):
    return core_create_spark(
        dagster_decorator=solid,
        name=name,
        main_class=main_class,
        description=description,
        required_resource_keys=required_resource_keys,
    )


def create_spark_op(
    name, main_class, description=None, required_resource_keys=frozenset(["spark"])
):
    return core_create_spark(
        dagster_decorator=op,
        name=name,
        main_class=main_class,
        description=description,
        required_resource_keys=required_resource_keys,
    )


def core_create_spark(
    dagster_decorator,
    name,
    main_class,
    description=None,
    required_resource_keys=frozenset(["spark"]),
):
    check.str_param(name, "name")
    check.str_param(main_class, "main_class")
    check.opt_str_param(description, "description", "A parameterized Spark job.")
    check.set_param(required_resource_keys, "required_resource_keys")

    @dagster_decorator(
        name=name,
        description=description,
        config_schema=define_spark_config(),
        input_defs=[InputDefinition("start", Nothing)],
        output_defs=[OutputDefinition(Nothing)],
        tags={"kind": "spark", "main_class": main_class},
        required_resource_keys=required_resource_keys,
    )
    def spark_solid(context):  # pylint: disable=unused-argument
        context.resources.spark.run_spark_job(context.solid_config, main_class)

    return spark_solid
