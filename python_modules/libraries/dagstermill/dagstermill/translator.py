import papermill

from dagster import _seven as seven

RESERVED_INPUT_NAMES = [
    "__dm_context",
    "__dm_dagstermill",
    "__dm_executable_dict",
    "__dm_json",
    "__dm_pipeline_run_dict",
    "__dm_solid_handle_kwargs",
    "__dm_instance_ref_dict",
    "__dm_step_key",
    "__dm_input_names",
]

INJECTED_BOILERPLATE = """
# Injected parameters
from dagster import _seven as seven as __dm_seven
import dagstermill as __dm_dagstermill
context = __dm_dagstermill._reconstitute_pipeline_context(
    **{{
        key: __dm_seven.json.loads(value)
        for key, value
        in {pipeline_context_args}.items()
    }}
)
"""


class DagsterTranslator(papermill.translators.PythonTranslator):
    @classmethod
    def codify(cls, parameters):  # pylint: disable=arguments-differ
        assert "__dm_context" in parameters
        assert "__dm_executable_dict" in parameters
        assert "__dm_pipeline_run_dict" in parameters
        assert "__dm_solid_handle_kwargs" in parameters
        assert "__dm_instance_ref_dict" in parameters
        assert "__dm_step_key" in parameters
        assert "__dm_input_names" in parameters

        context_args = parameters["__dm_context"]
        pipeline_context_args = dict(
            executable_dict=parameters["__dm_executable_dict"],
            pipeline_run_dict=parameters["__dm_pipeline_run_dict"],
            solid_handle_kwargs=parameters["__dm_solid_handle_kwargs"],
            instance_ref_dict=parameters["__dm_instance_ref_dict"],
            step_key=parameters["__dm_step_key"],
            **context_args,
        )

        for key in pipeline_context_args:
            pipeline_context_args[key] = seven.json.dumps(pipeline_context_args[key])

        content = INJECTED_BOILERPLATE.format(pipeline_context_args=pipeline_context_args)

        for input_name in parameters["__dm_input_names"]:
            dm_load_input_call = f"__dm_dagstermill._load_input_parameter('{input_name}')"
            content += "{}\n".format(cls.assign(input_name, dm_load_input_call))

        return content


papermill.translators.papermill_translators.register("python", DagsterTranslator)
