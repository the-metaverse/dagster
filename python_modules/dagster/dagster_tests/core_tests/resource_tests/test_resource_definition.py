from contextlib import contextmanager
from unittest import mock

import pytest

from dagster import (
    DagsterEventType,
    DagsterInvariantViolationError,
    DagsterResourceFunctionError,
    Enum,
    EnumValue,
    Field,
    Int,
    ModeDefinition,
    PipelineDefinition,
    ResourceDefinition,
    String,
    build_op_context,
    configured,
    execute_pipeline,
    execute_pipeline_iterator,
    fs_io_manager,
    graph,
    job,
    op,
    reconstructable,
    resource,
    solid,
)
from dagster._core.definitions import pipeline
from dagster._core.definitions.pipeline_base import InMemoryPipeline
from dagster._core.definitions.resource_definition import make_values_resource
from dagster._core.errors import DagsterConfigMappingFunctionError, DagsterInvalidDefinitionError
from dagster._core.events.log import EventLogEntry, construct_event_logger
from dagster._core.execution.api import create_execution_plan, execute_plan, execute_run
from dagster._core.instance import DagsterInstance
from dagster._core.test_utils import instance_for_test
from dagster._core.utils import coerce_valid_log_level


def define_string_resource():
    return ResourceDefinition(
        config_schema=String, resource_fn=lambda init_context: init_context.resource_config
    )


def test_resource_decorator_no_context():
    @resource
    def _basic():
        pass

    # Document that it is possible to provide required resource keys and
    # config schema, and still not provide a context.
    @resource(required_resource_keys={"foo", "bar"}, config_schema={"foo": str})
    def _reqs_resources():
        pass


def assert_pipeline_runs_with_resource(resource_def, resource_config, expected_resource):
    called = {}

    @solid(required_resource_keys={"some_name"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.some_name == expected_resource

    pipeline_def = PipelineDefinition(
        name="with_a_resource",
        solid_defs=[a_solid],
        mode_defs=[ModeDefinition(resource_defs={"some_name": resource_def})],
    )

    run_config = (
        {"resources": {"some_name": {"config": resource_config}}} if resource_config else {}
    )

    result = execute_pipeline(pipeline_def, run_config)

    assert result.success
    assert called["yup"]


def test_basic_resource():
    called = {}

    @solid(required_resource_keys={"a_string"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.a_string == "foo"

    pipeline_def = PipelineDefinition(
        name="with_a_resource",
        solid_defs=[a_solid],
        mode_defs=[ModeDefinition(resource_defs={"a_string": define_string_resource()})],
    )

    result = execute_pipeline(pipeline_def, {"resources": {"a_string": {"config": "foo"}}})

    assert result.success
    assert called["yup"]


def test_resource_with_dependencies():
    called = {}

    @resource
    def foo_resource(_):
        called["foo_resource"] = True
        return "foo"

    @resource(required_resource_keys={"foo_resource"})
    def bar_resource(init_context):
        called["bar_resource"] = True
        return init_context.resources.foo_resource + "bar"

    @solid(required_resource_keys={"bar_resource"})
    def dep_solid(context):
        called["dep_solid"] = True
        assert context.resources.bar_resource == "foobar"

    pipeline_def = PipelineDefinition(
        name="with_dep_resource",
        solid_defs=[dep_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={"foo_resource": foo_resource, "bar_resource": bar_resource}
            )
        ],
    )

    result = execute_pipeline(pipeline_def)

    assert result.success
    assert called["foo_resource"]
    assert called["bar_resource"]
    assert called["dep_solid"]


def test_resource_cyclic_dependencies():
    called = {}

    @resource(required_resource_keys={"bar_resource"})
    def foo_resource(init_context):
        called["foo_resource"] = True
        return init_context.resources.bar_resource + "foo"

    @resource(required_resource_keys={"foo_resource"})
    def bar_resource(init_context):
        called["bar_resource"] = True
        return init_context.resources.foo_resource + "bar"

    @solid(required_resource_keys={"bar_resource"})
    def dep_solid(context):
        called["dep_solid"] = True
        assert context.resources.bar_resource == "foobar"

    pipeline_def = PipelineDefinition(
        name="with_dep_resource",
        solid_defs=[dep_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={"foo_resource": foo_resource, "bar_resource": bar_resource}
            )
        ],
    )

    with pytest.raises(
        DagsterInvariantViolationError,
        match='Resource key "(foo_resource|bar_resource)" transitively depends on itself.',
    ):
        execute_pipeline(pipeline_def)


def test_yield_resource():
    called = {}

    @solid(required_resource_keys={"a_string"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.a_string == "foo"

    def _do_resource(init_context):
        yield init_context.resource_config

    yield_string_resource = ResourceDefinition(config_schema=String, resource_fn=_do_resource)

    pipeline_def = PipelineDefinition(
        name="with_a_yield_resource",
        solid_defs=[a_solid],
        mode_defs=[ModeDefinition(resource_defs={"a_string": yield_string_resource})],
    )

    result = execute_pipeline(pipeline_def, {"resources": {"a_string": {"config": "foo"}}})

    assert result.success
    assert called["yup"]


def test_yield_multiple_resources():
    called = {}

    saw = []

    @solid(required_resource_keys={"string_one", "string_two"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.string_one == "foo"
        assert context.resources.string_two == "bar"

    def _do_resource(init_context):
        saw.append("before yield " + init_context.resource_config)
        yield init_context.resource_config
        saw.append("after yield " + init_context.resource_config)

    yield_string_resource = ResourceDefinition(config_schema=String, resource_fn=_do_resource)

    pipeline_def = PipelineDefinition(
        name="with_yield_resources",
        solid_defs=[a_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "string_one": yield_string_resource,
                    "string_two": yield_string_resource,
                }
            )
        ],
    )

    result = execute_pipeline(
        pipeline_def,
        {"resources": {"string_one": {"config": "foo"}, "string_two": {"config": "bar"}}},
    )

    assert result.success
    assert called["yup"]
    assert len(saw) == 4

    assert "before yield" in saw[0]
    assert "before yield" in saw[1]
    assert "after yield" in saw[2]
    assert "after yield" in saw[3]


def test_resource_decorator():
    called = {}

    saw = []

    @solid(required_resource_keys={"string_one", "string_two"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.string_one == "foo"
        assert context.resources.string_two == "bar"

    # API red alert. One has to wrap a type in Field because it is callable
    @resource(config_schema=Field(String))
    def yielding_string_resource(init_context):
        saw.append("before yield " + init_context.resource_config)
        yield init_context.resource_config
        saw.append("after yield " + init_context.resource_config)

    pipeline_def = PipelineDefinition(
        name="with_yield_resources",
        solid_defs=[a_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "string_one": yielding_string_resource,
                    "string_two": yielding_string_resource,
                }
            )
        ],
    )

    result = execute_pipeline(
        pipeline_def,
        {"resources": {"string_one": {"config": "foo"}, "string_two": {"config": "bar"}}},
    )

    assert result.success
    assert called["yup"]
    assert len(saw) == 4

    assert "before yield" in saw[0]
    assert "before yield" in saw[1]
    assert "after yield" in saw[2]
    assert "after yield" in saw[3]


def test_mixed_multiple_resources():
    called = {}

    saw = []

    @solid(required_resource_keys={"returned_string", "yielded_string"})
    def a_solid(context):
        called["yup"] = True
        assert context.resources.returned_string == "foo"
        assert context.resources.yielded_string == "bar"

    def _do_yield_resource(init_context):
        saw.append("before yield " + init_context.resource_config)
        yield init_context.resource_config
        saw.append("after yield " + init_context.resource_config)

    yield_string_resource = ResourceDefinition(config_schema=String, resource_fn=_do_yield_resource)

    def _do_return_resource(init_context):
        saw.append("before return " + init_context.resource_config)
        return init_context.resource_config

    return_string_resource = ResourceDefinition(
        config_schema=String, resource_fn=_do_return_resource
    )

    pipeline_def = PipelineDefinition(
        name="with_a_yield_resource",
        solid_defs=[a_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "yielded_string": yield_string_resource,
                    "returned_string": return_string_resource,
                }
            )
        ],
    )

    result = execute_pipeline(
        pipeline_def,
        {"resources": {"returned_string": {"config": "foo"}, "yielded_string": {"config": "bar"}}},
    )

    assert result.success
    assert called["yup"]
    # could be processed in any order in python 2
    assert "before yield bar" in saw[0] or "before return foo" in saw[0]
    assert "before yield bar" in saw[1] or "before return foo" in saw[1]
    assert "after yield bar" in saw[2]


def test_none_resource():
    called = {}

    @solid(required_resource_keys={"test_null"})
    def solid_test_null(context):
        assert context.resources.test_null is None
        called["yup"] = True

    the_pipeline = PipelineDefinition(
        name="test_none_resource",
        solid_defs=[solid_test_null],
        mode_defs=[ModeDefinition(resource_defs={"test_null": ResourceDefinition.none_resource()})],
    )

    result = execute_pipeline(the_pipeline)

    assert result.success
    assert called["yup"]


def test_string_resource():
    called = {}

    @solid(required_resource_keys={"test_string"})
    def solid_test_string(context):
        assert context.resources.test_string == "foo"
        called["yup"] = True

    the_pipeline = PipelineDefinition(
        name="test_string_resource",
        solid_defs=[solid_test_string],
        mode_defs=[
            ModeDefinition(resource_defs={"test_string": ResourceDefinition.string_resource()})
        ],
    )

    result = execute_pipeline(the_pipeline, {"resources": {"test_string": {"config": "foo"}}})

    assert result.success
    assert called["yup"]


def test_variables_resource():
    any_variable = 1
    single_variable = {"foo": "my_string"}
    multi_variables = {"foo": "my_string", "bar": 1}

    @solid(required_resource_keys={"any_variable", "single_variable", "multi_variables"})
    def my_solid(context):
        assert context.resources.any_variable == any_variable
        assert context.resources.single_variable == single_variable
        assert context.resources.multi_variables == multi_variables

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "any_variable": make_values_resource(),
                    "single_variable": make_values_resource(foo=str),
                    "multi_variables": make_values_resource(foo=str, bar=int),
                },
            )
        ]
    )
    def my_pipeline():
        my_solid()

    result = execute_pipeline(
        my_pipeline,
        run_config={
            "resources": {
                "any_variable": {"config": any_variable},
                "single_variable": {"config": single_variable},
                "multi_variables": {"config": multi_variables},
            }
        },
    )

    assert result.success


def test_hardcoded_resource():
    called = {}

    mock_obj = mock.MagicMock()

    @solid(required_resource_keys={"hardcoded"})
    def solid_hardcoded(context):
        assert context.resources.hardcoded("called")
        called["yup"] = True

    the_pipeline = PipelineDefinition(
        name="hardcoded_resource",
        solid_defs=[solid_hardcoded],
        mode_defs=[
            ModeDefinition(
                resource_defs={"hardcoded": ResourceDefinition.hardcoded_resource(mock_obj)}
            )
        ],
    )

    result = execute_pipeline(the_pipeline)

    assert result.success
    assert called["yup"]
    mock_obj.assert_called_with("called")


def test_mock_resource():
    called = {}

    @solid(required_resource_keys={"test_mock"})
    def solid_test_mock(context):
        assert context.resources.test_mock is not None
        called["yup"] = True

    the_pipeline = PipelineDefinition(
        name="test_mock_resource",
        solid_defs=[solid_test_mock],
        mode_defs=[ModeDefinition(resource_defs={"test_mock": ResourceDefinition.mock_resource()})],
    )

    result = execute_pipeline(the_pipeline)

    assert result.success
    assert called["yup"]


def test_no_config_resource_pass_none():
    called = {}

    @resource(None)
    def return_thing(_init_context):
        called["resource"] = True
        return "thing"

    @solid(required_resource_keys={"return_thing"})
    def check_thing(context):
        called["solid"] = True
        assert context.resources.return_thing == "thing"

    the_pipeline = PipelineDefinition(
        name="test_no_config_resource",
        solid_defs=[check_thing],
        mode_defs=[ModeDefinition(resource_defs={"return_thing": return_thing})],
    )

    execute_pipeline(the_pipeline)

    assert called["resource"]
    assert called["solid"]


def test_no_config_resource_no_arg():
    called = {}

    @resource()
    def return_thing(_init_context):
        called["resource"] = True
        return "thing"

    @solid(required_resource_keys={"return_thing"})
    def check_thing(context):
        called["solid"] = True
        assert context.resources.return_thing == "thing"

    the_pipeline = PipelineDefinition(
        name="test_no_config_resource",
        solid_defs=[check_thing],
        mode_defs=[ModeDefinition(resource_defs={"return_thing": return_thing})],
    )

    execute_pipeline(the_pipeline)

    assert called["resource"]
    assert called["solid"]


def test_no_config_resource_bare_no_arg():
    called = {}

    @resource
    def return_thing(_init_context):
        called["resource"] = True
        return "thing"

    @solid(required_resource_keys={"return_thing"})
    def check_thing(context):
        called["solid"] = True
        assert context.resources.return_thing == "thing"

    the_pipeline = PipelineDefinition(
        name="test_no_config_resource",
        solid_defs=[check_thing],
        mode_defs=[ModeDefinition(resource_defs={"return_thing": return_thing})],
    )

    execute_pipeline(the_pipeline)

    assert called["resource"]
    assert called["solid"]


def test_no_config_resource_definition():
    called = {}

    def _return_thing_resource_fn(_init_context):
        called["resource"] = True
        return "thing"

    @solid(required_resource_keys={"return_thing"})
    def check_thing(context):
        called["solid"] = True
        assert context.resources.return_thing == "thing"

    the_pipeline = PipelineDefinition(
        name="test_no_config_resource",
        solid_defs=[check_thing],
        mode_defs=[
            ModeDefinition(
                resource_defs={"return_thing": ResourceDefinition(_return_thing_resource_fn)}
            )
        ],
    )

    execute_pipeline(the_pipeline)

    assert called["resource"]
    assert called["solid"]


def test_resource_cleanup():
    called = {}

    def _cleanup_resource_fn(_init_context):
        called["creation"] = True
        yield True
        called["cleanup"] = True

    @solid(required_resource_keys={"resource_with_cleanup"})
    def check_resource_created(context):
        called["solid"] = True
        assert context.resources.resource_with_cleanup is True

    the_pipeline = PipelineDefinition(
        name="test_resource_cleanup",
        solid_defs=[check_resource_created],
        mode_defs=[
            ModeDefinition(
                resource_defs={"resource_with_cleanup": ResourceDefinition(_cleanup_resource_fn)}
            )
        ],
    )

    execute_pipeline(the_pipeline)

    assert called["creation"] is True
    assert called["solid"] is True
    assert called["cleanup"] is True


def test_stacked_resource_cleanup():
    called = []

    def _cleanup_resource_fn_1(_init_context):
        called.append("creation_1")
        yield True
        called.append("cleanup_1")

    def _cleanup_resource_fn_2(_init_context):
        called.append("creation_2")
        yield True
        called.append("cleanup_2")

    @solid(required_resource_keys={"resource_with_cleanup_1", "resource_with_cleanup_2"})
    def check_resource_created(context):
        called.append("solid")
        assert context.resources.resource_with_cleanup_1 is True
        assert context.resources.resource_with_cleanup_2 is True

    the_pipeline = PipelineDefinition(
        name="test_resource_cleanup",
        solid_defs=[check_resource_created],
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "resource_with_cleanup_1": ResourceDefinition(_cleanup_resource_fn_1),
                    "resource_with_cleanup_2": ResourceDefinition(_cleanup_resource_fn_2),
                }
            )
        ],
    )

    execute_pipeline(the_pipeline)

    assert called == ["creation_1", "creation_2", "solid", "cleanup_2", "cleanup_1"]


def test_incorrect_resource_init_error():
    @resource
    def _correct_resource(_):
        pass

    @resource
    def _correct_resource_no_context():
        pass

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="expects only a single positional required argument. Got required extra params _b, _c",
    ):

        @resource
        def _incorrect_resource_2(_a, _b, _c, _d=4):
            pass

    @resource
    def _correct_resource_2(_a, _b=1, _c=2):
        pass


def test_resource_init_failure():
    @resource
    def failing_resource(_init_context):
        raise Exception("Uh oh")

    @solid(required_resource_keys={"failing_resource"})
    def failing_resource_solid(_context):
        pass

    the_pipeline = PipelineDefinition(
        name="test_resource_init_failure",
        solid_defs=[failing_resource_solid],
        mode_defs=[ModeDefinition(resource_defs={"failing_resource": failing_resource})],
    )

    res = execute_pipeline(the_pipeline, raise_on_error=False)

    event_types = [event.event_type_value for event in res.event_list]
    assert DagsterEventType.PIPELINE_FAILURE.value in event_types

    instance = DagsterInstance.ephemeral()
    execution_plan = create_execution_plan(the_pipeline)
    pipeline_run = instance.create_run_for_pipeline(the_pipeline, execution_plan=execution_plan)

    with pytest.raises(
        DagsterResourceFunctionError,
        match="Error executing resource_fn on ResourceDefinition failing_resource",
    ):

        execute_plan(
            execution_plan,
            InMemoryPipeline(the_pipeline),
            pipeline_run=pipeline_run,
            instance=instance,
        )

    # Test the pipeline init failure event fires even if we are raising errors
    events = []
    try:
        for event in execute_pipeline_iterator(the_pipeline):
            events.append(event)
    except DagsterResourceFunctionError:
        pass

    event_types = [event.event_type_value for event in events]
    assert DagsterEventType.PIPELINE_FAILURE.value in event_types


def test_dagster_type_resource_decorator_config():
    @resource(Int)
    def dagster_type_resource_config(_):
        raise Exception("not called")

    assert dagster_type_resource_config.config_schema.config_type.given_name == "Int"

    @resource(int)
    def python_type_resource_config(_):
        raise Exception("not called")

    assert python_type_resource_config.config_schema.config_type.given_name == "Int"


def test_resource_init_failure_with_teardown():
    called = []
    cleaned = []

    @resource
    def resource_a(_):
        try:
            called.append("A")
            yield "A"
        finally:
            cleaned.append("A")

    @resource
    def resource_b(_):
        try:
            called.append("B")
            raise Exception("uh oh")
            yield "B"  # pylint: disable=unreachable
        finally:
            cleaned.append("B")

    @solid(required_resource_keys={"a", "b"})
    def resource_solid(_):
        pass

    the_pipeline = PipelineDefinition(
        name="test_resource_init_failure_with_cleanup",
        solid_defs=[resource_solid],
        mode_defs=[ModeDefinition(resource_defs={"a": resource_a, "b": resource_b})],
    )

    res = execute_pipeline(the_pipeline, raise_on_error=False)
    event_types = [event.event_type_value for event in res.event_list]
    assert DagsterEventType.PIPELINE_FAILURE.value in event_types

    assert called == ["A", "B"]
    assert cleaned == ["B", "A"]

    called = []
    cleaned = []

    events = []
    try:
        for event in execute_pipeline_iterator(the_pipeline):
            events.append(event)
    except DagsterResourceFunctionError:
        pass

    event_types = [event.event_type_value for event in events]
    assert DagsterEventType.PIPELINE_FAILURE.value in event_types
    assert called == ["A", "B"]
    assert cleaned == ["B", "A"]


def test_solid_failure_resource_teardown():
    called = []
    cleaned = []

    @resource
    def resource_a(_):
        try:
            called.append("A")
            yield "A"
        finally:
            cleaned.append("A")

    @resource
    def resource_b(_):
        try:
            called.append("B")
            yield "B"
        finally:
            cleaned.append("B")

    @solid(required_resource_keys={"a", "b"})
    def resource_solid(_):
        raise Exception("uh oh")

    the_pipeline = PipelineDefinition(
        name="test_solid_failure_resource_teardown",
        solid_defs=[resource_solid],
        mode_defs=[ModeDefinition(resource_defs={"a": resource_a, "b": resource_b})],
    )

    res = execute_pipeline(the_pipeline, raise_on_error=False)
    assert res.event_list[-1].event_type_value == "PIPELINE_FAILURE"
    assert called == ["A", "B"]
    assert cleaned == ["B", "A"]

    called = []
    cleaned = []

    events = []
    try:
        for event in execute_pipeline_iterator(the_pipeline):
            events.append(event)
    except DagsterResourceFunctionError:
        pass

    assert len(events) > 1
    assert events[-1].event_type_value == "PIPELINE_FAILURE"
    assert called == ["A", "B"]
    assert cleaned == ["B", "A"]


def test_solid_failure_resource_teardown_raise():
    """test that teardown is invoked in resources for tests that raise_on_error"""
    called = []
    cleaned = []

    @resource
    def resource_a(_):
        try:
            called.append("A")
            yield "A"
        finally:
            cleaned.append("A")

    @resource
    def resource_b(_):
        try:
            called.append("B")
            yield "B"
        finally:
            cleaned.append("B")

    @solid(required_resource_keys={"a", "b"})
    def resource_solid(_):
        raise Exception("uh oh")

    the_pipeline = PipelineDefinition(
        name="test_solid_failure_resource_teardown",
        solid_defs=[resource_solid],
        mode_defs=[ModeDefinition(resource_defs={"a": resource_a, "b": resource_b})],
    )

    with pytest.raises(Exception):
        execute_pipeline(the_pipeline)

    assert called == ["A", "B"]
    assert cleaned == ["B", "A"]

    called = []
    cleaned = []


def test_resource_teardown_failure():
    called = []
    cleaned = []

    @resource
    def resource_a(_):
        try:
            called.append("A")
            yield "A"
        finally:
            cleaned.append("A")

    @resource
    def resource_b(_):
        try:
            called.append("B")
            yield "B"
        finally:
            raise Exception("uh oh")
            cleaned.append("B")  # pylint: disable=unreachable

    @solid(required_resource_keys={"a", "b"})
    def resource_solid(_):
        pass

    the_pipeline = PipelineDefinition(
        name="test_resource_teardown_failure",
        solid_defs=[resource_solid],
        mode_defs=[ModeDefinition(resource_defs={"a": resource_a, "b": resource_b})],
    )

    result = execute_pipeline(the_pipeline, raise_on_error=False)
    assert result.success
    error_events = [
        event
        for event in result.event_list
        if event.is_engine_event and event.event_specific_data.error
    ]
    assert len(error_events) == 1
    assert called == ["A", "B"]
    assert cleaned == ["A"]

    called = []
    cleaned = []
    events = []
    try:
        for event in execute_pipeline_iterator(the_pipeline):
            events.append(event)
    except DagsterResourceFunctionError:
        pass

    assert called == ["A", "B"]
    assert cleaned == ["A"]


def define_resource_teardown_failure_pipeline():
    @resource
    def resource_a(_):
        try:
            yield "A"
        finally:
            pass

    @resource
    def resource_b(_):
        try:
            yield "B"
        finally:
            raise Exception("uh oh")

    @solid(required_resource_keys={"a", "b"})
    def resource_solid(_):
        pass

    return PipelineDefinition(
        name="resource_teardown_failure",
        solid_defs=[resource_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={"a": resource_a, "b": resource_b, "io_manager": fs_io_manager}
            )
        ],
    )


def test_multiprocessing_resource_teardown_failure():
    with instance_for_test() as instance:
        recon_pipeline = reconstructable(define_resource_teardown_failure_pipeline)
        result = execute_pipeline(
            recon_pipeline,
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
            raise_on_error=False,
        )
        assert result.success
        error_events = [
            event
            for event in result.event_list
            if event.is_engine_event and event.event_specific_data.error
        ]
        assert len(error_events) == 1


def test_single_step_resource_event_logs():
    # Test to attribute logs for single-step plans which are often the representation of
    # sub-plans in a multiprocessing execution environment. Most likely will need to be rewritten
    # with the refactor detailed in https://github.com/dagster-io/dagster/issues/2239
    USER_SOLID_MESSAGE = "I AM A SOLID"
    USER_RESOURCE_MESSAGE = "I AM A RESOURCE"
    events = []

    def event_callback(record):
        assert isinstance(record, EventLogEntry)
        events.append(record)

    @solid(required_resource_keys={"a"})
    def resource_solid(context):
        context.log.info(USER_SOLID_MESSAGE)

    @resource
    def resource_a(context):
        context.log.info(USER_RESOURCE_MESSAGE)
        return "A"

    the_pipeline = PipelineDefinition(
        name="resource_logging_pipeline",
        solid_defs=[resource_solid],
        mode_defs=[
            ModeDefinition(
                resource_defs={"a": resource_a},
                logger_defs={"callback": construct_event_logger(event_callback)},
            )
        ],
    )

    with instance_for_test() as instance:
        pipeline_run = instance.create_run_for_pipeline(
            the_pipeline,
            run_config={"loggers": {"callback": {}}},
            solids_to_execute={"resource_solid"},
        )

        result = execute_run(InMemoryPipeline(the_pipeline), pipeline_run, instance)

        assert result.success
        log_messages = [
            event
            for event in events
            if isinstance(event, EventLogEntry) and event.level == coerce_valid_log_level("INFO")
        ]
        assert len(log_messages) == 2

        resource_log_message = next(
            iter(
                [
                    message
                    for message in log_messages
                    if message.user_message == USER_RESOURCE_MESSAGE
                ]
            )
        )
        assert resource_log_message.step_key == "resource_solid"


def test_configured_with_config():
    str_resource = define_string_resource()
    configured_resource = str_resource.configured("foo")
    assert_pipeline_runs_with_resource(configured_resource, {}, "foo")


def test_configured_with_fn():
    str_resource = define_string_resource()
    configured_resource = str_resource.configured(lambda num: str(num + 1), Int)
    assert_pipeline_runs_with_resource(configured_resource, 2, "3")


def test_configured_decorator_with_fn():
    str_resource = define_string_resource()

    @configured(str_resource, Int)
    def configured_resource(num):
        return str(num + 1)

    assert_pipeline_runs_with_resource(configured_resource, 2, "3")


def test_configured_decorator_with_fn_and_user_code_error():
    str_resource = define_string_resource()

    @configured(str_resource, Int)
    def configured_resource(num):
        raise Exception("beep boop broke")

    with pytest.raises(
        DagsterConfigMappingFunctionError,
        match=(
            "The config mapping function on a `configured` ResourceDefinition has thrown an "
            "unexpected error during its execution."
        ),
    ) as user_code_exc:
        assert_pipeline_runs_with_resource(configured_resource, 2, "unreachable")

    assert user_code_exc.value.user_exception.args[0] == "beep boop broke"


def test_resource_with_enum_in_schema():
    from enum import Enum as PythonEnum

    class TestPythonEnum(PythonEnum):
        VALUE_ONE = 0
        OTHER = 1

    DagsterEnumType = Enum(
        "TestEnum",
        [
            EnumValue("VALUE_ONE", TestPythonEnum.VALUE_ONE),
            EnumValue("OTHER", TestPythonEnum.OTHER),
        ],
    )

    @resource(config_schema={"enum": DagsterEnumType})
    def enum_resource(context):
        return context.resource_config["enum"]

    assert_pipeline_runs_with_resource(
        enum_resource, {"enum": "VALUE_ONE"}, TestPythonEnum.VALUE_ONE
    )


def test_resource_with_enum_in_schema_configured():
    from enum import Enum as PythonEnum

    class TestPythonEnum(PythonEnum):
        VALUE_ONE = 0
        OTHER = 1

    DagsterEnumType = Enum(
        "TestEnum",
        [
            EnumValue("VALUE_ONE", TestPythonEnum.VALUE_ONE),
            EnumValue("OTHER", TestPythonEnum.OTHER),
        ],
    )

    @resource(config_schema={"enum": DagsterEnumType})
    def enum_resource(context):
        return context.resource_config["enum"]

    @configured(enum_resource, {"enum": DagsterEnumType})
    def passthrough_to_enum_resource(config):
        return {"enum": "VALUE_ONE" if config["enum"] == TestPythonEnum.VALUE_ONE else "OTHER"}

    assert_pipeline_runs_with_resource(
        passthrough_to_enum_resource, {"enum": "VALUE_ONE"}, TestPythonEnum.VALUE_ONE
    )


def test_resource_run_info_exists_during_execution():
    @resource
    def resource_checks_run_info(init_context):
        assert init_context.pipeline_run.run_id == init_context.run_id
        return 1

    assert_pipeline_runs_with_resource(resource_checks_run_info, {}, 1)


def test_resource_needs_resource():
    @resource(required_resource_keys={"bar_resource"})
    def foo_resource(init_context):
        return init_context.resources.bar_resource + "foo"

    with pytest.raises(DagsterInvalidDefinitionError, match="is required by resource"):

        @pipeline(
            mode_defs=[ModeDefinition(resource_defs={"foo_resource": foo_resource})],
        )
        def _fail():
            pass


def test_resource_op_subset():
    @resource(required_resource_keys={"bar"})
    def foo_resource(_):
        return "FOO"

    @resource()
    def bar_resource(_):
        return "BAR"

    @resource()
    def baz_resource(_):
        return "BAZ"

    @op(required_resource_keys={"baz"})
    def baz_op(_):
        pass

    @op(required_resource_keys={"foo"})
    def foo_op(_):
        pass

    @op(required_resource_keys={"bar"})
    def bar_op(_):
        pass

    @job(
        resource_defs={
            "foo": foo_resource,
            "baz": baz_resource,
            "bar": bar_resource,
        }
    )
    def nested():
        foo_op()
        bar_op()
        baz_op()

    assert set(nested.get_required_resource_defs_for_mode("default").keys()) == {
        "foo",
        "bar",
        "baz",
        "io_manager",
    }

    assert nested.get_job_def_for_op_selection(["foo_op"]).get_required_resource_defs_for_mode(
        "default"
    ).keys() == {"foo", "bar", "io_manager"}

    assert nested.get_job_def_for_op_selection(["bar_op"]).get_required_resource_defs_for_mode(
        "default"
    ).keys() == {"bar", "io_manager"}

    assert nested.get_job_def_for_op_selection(["baz_op"]).get_required_resource_defs_for_mode(
        "default"
    ).keys() == {"baz", "io_manager"}


def test_config_with_no_schema():
    @resource
    def my_resource(init_context):
        return init_context.resource_config

    @solid(required_resource_keys={"resource"})
    def my_solid(context):
        assert context.resources.resource == 5

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"resource": my_resource})])
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline, run_config={"resources": {"resource": {"config": 5}}})


def test_configured_resource_unused():
    # Ensure that if we do not use a resource on a mode definition, then we do not apply the config
    # schema.
    entered = []

    @resource
    def basic_resource(_):
        pass

    @configured(basic_resource)
    def configured_resource(_):
        entered.append("True")

    @solid(required_resource_keys={"bar"})
    def basic_solid(_):
        pass

    @pipeline(
        mode_defs=[
            ModeDefinition(resource_defs={"foo": configured_resource, "bar": basic_resource})
        ]
    )
    def basic_pipeline():
        basic_solid()

    execute_pipeline(basic_pipeline)

    assert not entered


def test_context_manager_resource():
    event_list = []

    @resource
    @contextmanager
    def cm_resource():
        try:
            event_list.append("foo")
            yield "foo"
        finally:
            event_list.append("finally")

    @op(required_resource_keys={"cm"})
    def basic(context):
        event_list.append("compute")
        assert context.resources.cm == "foo"

    with build_op_context(resources={"cm": cm_resource}) as context:
        basic(context)

    assert event_list == ["foo", "compute", "finally"]  # Ensures that we teardown after compute

    with pytest.raises(
        DagsterInvariantViolationError,
        match="At least one provided resource is a generator, but attempting to access resources "
        "outside of context manager scope.",
    ):
        basic(build_op_context(resources={"cm": cm_resource}))

    @graph
    def call_basic():
        basic()

    event_list = []

    assert call_basic.execute_in_process(resources={"cm": cm_resource}).success
    assert event_list == ["foo", "compute", "finally"]
