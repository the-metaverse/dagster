from typing import Any, Dict, List

from dagster import _check as check, usable_as_dagster_type

from ..types import DbtOutput


@usable_as_dagster_type
class DbtCliOutput(DbtOutput):
    """The results of executing a dbt command, along with additional metadata about the dbt CLI
    process that was run.

    Note that users should not construct instances of this class directly. This class is intended
    to be constructed from the JSON output of dbt commands.

    Attributes:
        command (str): The full shell command that was executed.
        return_code (int): The return code of the dbt CLI process.
        raw_output (str): The raw output (``stdout``) of the dbt CLI process.
        logs (List[Dict[str, Any]]): List of parsed JSON logs produced by the dbt command.
        result (Optional[Dict[str, Any]]): Dictionary containing dbt-reported result information
            contained in run_results.json.  Some dbt commands do not produce results, and will
            therefore have result = None.
    """

    def __init__(
        self,
        command: str,
        return_code: int,
        raw_output: str,
        logs: List[Dict[str, Any]],
        result: Dict[str, Any],
    ):
        self._command = check.str_param(command, "command")
        self._return_code = check.int_param(return_code, "return_code")
        self._raw_output = check.str_param(raw_output, "raw_output")
        self._logs = check.list_param(logs, "logs", of_type=dict)
        super().__init__(result)

    @property
    def command(self) -> str:
        return self._command

    @property
    def return_code(self) -> int:
        return self._return_code

    @property
    def raw_output(self) -> str:
        return self._raw_output

    @property
    def logs(self) -> List[Dict[str, Any]]:
        return self._logs
