from dagster import check
from dagster._core.execution.api import execute_run
from dagster._core.launcher import LaunchRunContext, RunLauncher
from dagster.serdes import ConfigurableClass
from dagster.utils.hosted_user_process import recon_pipeline_from_origin


class SyncInMemoryRunLauncher(RunLauncher, ConfigurableClass):
    """This run launcher launches runs synchronously, in memory, and is intended only for test.

    Use the :py:class:`dagster.DefaultRunLauncher`.
    """

    def __init__(self, inst_data=None):
        self._inst_data = inst_data
        self._repository = None
        self._instance_ref = None

        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return SyncInMemoryRunLauncher(inst_data=inst_data)

    def launch_run(self, context: LaunchRunContext) -> None:
        recon_pipeline = recon_pipeline_from_origin(context.pipeline_code_origin)
        execute_run(recon_pipeline, context.pipeline_run, self._instance)

    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        check.not_implemented("Termination not supported.")
