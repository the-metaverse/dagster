import pytest

from dagster import (
    DynamicOutput,
    DynamicOutputDefinition,
    Field,
    InputDefinition,
    Output,
    OutputDefinition,
    composite_solid,
    execute_pipeline,
    pipeline,
    reconstructable,
    solid,
)
from dagster._core.errors import DagsterExecutionStepNotFoundError
from dagster._core.execution.api import create_execution_plan, reexecute_pipeline
from dagster._core.execution.plan.state import KnownExecutionState
from dagster._core.test_utils import default_mode_def_for_test, instance_for_test
from dagster.utils import merge_dicts


@solid(tags={"third": "3"})
def multiply_by_two(context, y):
    context.log.info("multiply_by_two is returning " + str(y * 2))
    return y * 2


@solid(tags={"second": "2"})
def multiply_inputs(context, y, ten):
    context.log.info("multiply_inputs is returning " + str(y * ten))
    return y * ten


@solid
def emit_ten(_):
    return 10


@solid
def echo(_, x: int) -> int:
    return x


@solid(
    config_schema={
        "range": Field(int, is_required=False, default_value=3),
    }
)
def num_range(context) -> int:
    return context.solid_config["range"]


@solid(
    output_defs=[DynamicOutputDefinition()],
    config_schema={
        "fail": Field(bool, is_required=False, default_value=False),
    },
    tags={"first": "1"},
)
def emit(context, num: int = 3):
    if context.solid_config["fail"]:
        raise Exception("FAILURE")

    for i in range(num):
        yield DynamicOutput(value=i, mapping_key=str(i))


@solid
def sum_numbers(_, nums):
    return sum(nums)


@solid(output_defs=[DynamicOutputDefinition()])
def dynamic_echo(_, nums):
    for x in nums:
        yield DynamicOutput(value=x, mapping_key=str(x))


@pipeline(mode_defs=[default_mode_def_for_test])
def dynamic_pipeline():

    numbers = emit(num_range())
    dynamic = numbers.map(lambda num: multiply_by_two(multiply_inputs(num, emit_ten())))
    n = multiply_by_two.alias("double_total")(sum_numbers(dynamic.collect()))
    echo(n)  # test transitive downstream of collect


@pipeline(mode_defs=[default_mode_def_for_test])
def fan_repeat():
    one = emit(num_range()).map(multiply_by_two)
    two = dynamic_echo(one.collect()).map(multiply_by_two).map(echo)
    three = dynamic_echo(two.collect()).map(multiply_by_two)
    sum_numbers(three.collect())


def _run_configs():
    return [{}, {"execution": {"multiprocess": {}}}]


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_map(run_config):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=run_config,
        )
        assert result.success
        keys = result.events_by_step_key.keys()
        assert "multiply_inputs[0]" in keys
        assert "multiply_inputs[1]" in keys
        assert "multiply_inputs[2]" in keys
        assert result.result_for_solid("multiply_inputs").output_value() == {
            "0": 0,
            "1": 10,
            "2": 20,
        }
        assert result.result_for_solid("multiply_by_two").output_value() == {
            "0": 0,
            "1": 20,
            "2": 40,
        }
        assert result.result_for_solid("sum_numbers").output_value() == 60
        assert result.result_for_solid("double_total").output_value() == 120
        assert result.result_for_solid("echo").output_value() == 120


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_map_empty(run_config):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=merge_dicts({"solids": {"num_range": {"config": {"range": 0}}}}, run_config),
        )
        assert result.success
        assert result.result_for_solid("double_total").output_value() == 0


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_map_selection(run_config):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=merge_dicts({"solids": {"emit": {"inputs": {"num": 2}}}}, run_config),
            solid_selection=["emit*", "emit_ten"],
        )
        assert result.success
        assert result.result_for_solid("double_total").output_value() == 40


def test_composite_wrapping():
    # regression test from user report

    @composite_solid(input_defs=[InputDefinition("z", int)], output_defs=[OutputDefinition(int)])
    def do_multiple_steps(z):
        output = echo(z)
        return echo(output)

    @pipeline
    def shallow():
        emit().map(do_multiple_steps)

    result = execute_pipeline(shallow)
    assert result.success
    assert result.result_for_solid("do_multiple_steps").output_value() == {"0": 0, "1": 1, "2": 2}

    @composite_solid(input_defs=[InputDefinition("x", int)], output_defs=[OutputDefinition(int)])
    def inner(x):
        return echo(x)

    @composite_solid(input_defs=[InputDefinition("y", int)], output_defs=[OutputDefinition(int)])
    def middle(y):
        return inner(y)

    @composite_solid(input_defs=[InputDefinition("z", int)], output_defs=[OutputDefinition(int)])
    def outer(z):
        return middle(z)

    @pipeline
    def deep():
        emit().map(outer)

    result = execute_pipeline(deep)
    assert result.success
    assert result.result_for_solid("outer").output_value() == {"0": 0, "1": 1, "2": 2}


def test_tags():
    known_state = KnownExecutionState(
        {},
        {
            emit.name: {"result": ["0", "1", "2"]},
        },
    )
    plan = create_execution_plan(dynamic_pipeline, known_state=known_state)

    assert plan.get_step_by_key(emit.name).tags == {"first": "1"}

    for mapping_key in range(3):
        assert plan.get_step_by_key(f"{multiply_inputs.name}[{mapping_key}]").tags == {
            "second": "2"
        }
        assert plan.get_step_by_key(f"{multiply_by_two.name}[{mapping_key}]").tags == {"third": "3"}


def test_full_reexecute():
    with instance_for_test() as instance:
        result_1 = execute_pipeline(dynamic_pipeline, instance=instance)
        assert result_1.success

        result_2 = reexecute_pipeline(
            dynamic_pipeline, parent_run_id=result_1.run_id, instance=instance
        )
        assert result_2.success


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_partial_reexecute(run_config):
    with instance_for_test() as instance:
        result_1 = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=run_config,
        )
        assert result_1.success

        result_2 = reexecute_pipeline(
            reconstructable(dynamic_pipeline),
            parent_run_id=result_1.run_id,
            instance=instance,
            step_selection=["sum_numbers*"],
            run_config=run_config,
        )
        assert result_2.success

        result_3 = reexecute_pipeline(
            reconstructable(dynamic_pipeline),
            parent_run_id=result_1.run_id,
            instance=instance,
            step_selection=["multiply_by_two[1]*"],
            run_config=run_config,
        )
        assert result_3.success


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_fan_out_in_out_in(run_config):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(fan_repeat),
            instance=instance,
            run_config=run_config,
        )
        assert result.success
        assert (
            result.result_for_solid("sum_numbers").output_value() == 24
        )  # (0, 1, 2) x 2 x 2 x 2 = (0, 8, 16)

        empty_result = execute_pipeline(
            reconstructable(fan_repeat),
            instance=instance,
            run_config={"solids": {"num_range": {"config": {"range": 0}}}},
        )
        assert empty_result.success
        assert empty_result.result_for_solid("sum_numbers").output_value() == 0


def test_bad_step_selection():
    with instance_for_test() as instance:
        result_1 = execute_pipeline(dynamic_pipeline, instance=instance)
        assert result_1.success

        # this exact error could be improved, but it should fail if you try to select
        # both the dynamic outputting step key and something resolved by it in the previous run
        with pytest.raises(DagsterExecutionStepNotFoundError):
            reexecute_pipeline(
                dynamic_pipeline,
                parent_run_id=result_1.run_id,
                instance=instance,
                step_selection=["emit", "multiply_by_two[1]"],
            )


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_map_fail(run_config):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=merge_dicts({"solids": {"emit": {"config": {"fail": True}}}}, run_config),
            raise_on_error=False,
        )
        assert not result.success


@pytest.mark.parametrize(
    "run_config",
    _run_configs(),
)
def test_map_reexecute_after_fail(run_config):
    with instance_for_test() as instance:
        result_1 = execute_pipeline(
            reconstructable(dynamic_pipeline),
            instance=instance,
            run_config=merge_dicts(
                run_config,
                {"solids": {"emit": {"config": {"fail": True}}}},
            ),
            raise_on_error=False,
        )
        assert not result_1.success

        result_2 = reexecute_pipeline(
            reconstructable(dynamic_pipeline),
            parent_run_id=result_1.run_id,
            instance=instance,
            run_config=run_config,
        )
        assert result_2.success


def test_multi_collect():
    @solid
    def fan_in(_, x, y):
        return x + y

    @pipeline
    def double():
        nums_1 = emit()
        nums_2 = emit()
        fan_in(nums_1.collect(), nums_2.collect())

    result = execute_pipeline(double)
    assert result.success
    assert result.result_for_solid("fan_in").output_value() == [0, 1, 2, 0, 1, 2]


def test_fan_in_skips():
    @solid(
        output_defs=[
            OutputDefinition(name="nums"),
            OutputDefinition(name="empty"),
            OutputDefinition(name="skip", is_required=False),
        ]
    )
    def fork_logic():
        yield Output([1, 2, 3], output_name="nums")
        yield Output([], output_name="empty")

    @solid(output_defs=[DynamicOutputDefinition(int)])
    def emit_dyn(vector):
        for i in vector:
            yield DynamicOutput(value=i, mapping_key=f"input_{i}")

    @solid
    def total(items):
        return sum(items)

    @pipeline
    def dyn_fork():
        nums, empty, skip = fork_logic()
        total.alias("grand_total")(
            [
                total.alias("nums_total")(emit_dyn(nums).map(echo).collect()),
                total.alias("empty_total")(emit_dyn(empty).map(echo).collect()),
                total.alias("skip_total")(emit_dyn(skip).map(echo).collect()),
            ]
        )

    result = execute_pipeline(dyn_fork)
    assert result.success

    assert result.result_for_solid("nums_total").success
    assert result.result_for_solid("empty_total").success

    assert result.result_for_solid("skip_total").success  # arguably should be skip

    assert result.result_for_solid("grand_total").success
    assert result.result_for_solid("grand_total").output_value() == 6
