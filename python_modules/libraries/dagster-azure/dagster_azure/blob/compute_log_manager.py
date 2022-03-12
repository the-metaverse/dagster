import itertools
import os
from contextlib import contextmanager

from dagster import Field, StringSource, check, seven
from dagster._core.storage.compute_log_manager import (
    MAX_BYTES_FILE_READ,
    ComputeIOType,
    ComputeLogFileData,
    ComputeLogManager,
)
from dagster._core.storage.local_compute_log_manager import IO_TYPE_EXTENSION, LocalComputeLogManager
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils import ensure_dir, ensure_file

from .utils import create_blob_client, generate_blob_sas


class AzureBlobComputeLogManager(ComputeLogManager, ConfigurableClass):
    """Logs op compute function stdout and stderr to Azure Blob Storage.

    This is also compatible with Azure Data Lake Storage.

    Users should not instantiate this class directly. Instead, use a YAML block in ``dagster.yaml``
    such as the following:

    .. code-block:: YAML

        compute_logs:
          module: dagster_azure.blob.compute_log_manager
          class: AzureBlobComputeLogManager
          config:
            storage_account: my-storage-account
            container: my-container
            credential: sas-token-or-secret-key
            prefix: "dagster-test-"
            local_dir: "/tmp/cool"

    Args:
        storage_account (str): The storage account name to which to log.
        container (str): The container (or ADLS2 filesystem) to which to log.
        secret_key (str): Secret key for the storage account. SAS tokens are not
            supported because we need a secret key to generate a SAS token for a download URL.
        local_dir (Optional[str]): Path to the local directory in which to stage logs. Default:
            ``dagster.seven.get_system_temp_directory()``.
        prefix (Optional[str]): Prefix for the log file keys.
        inst_data (Optional[ConfigurableClassData]): Serializable representation of the compute
            log manager when newed up from config.
    """

    def __init__(
        self,
        storage_account,
        container,
        secret_key,
        local_dir=None,
        inst_data=None,
        prefix="dagster",
    ):
        self._storage_account = check.str_param(storage_account, "storage_account")
        self._container = check.str_param(container, "container")
        self._blob_prefix = check.str_param(prefix, "prefix")
        check.str_param(secret_key, "secret_key")

        self._blob_client = create_blob_client(storage_account, secret_key)
        self._container_client = self._blob_client.get_container_client(container)
        self._download_urls = {}

        # proxy calls to local compute log manager (for subscriptions, etc)
        if not local_dir:
            local_dir = seven.get_system_temp_directory()

        self.local_manager = LocalComputeLogManager(local_dir)
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

    @contextmanager
    def _watch_logs(self, pipeline_run, step_key=None):
        # proxy watching to the local compute log manager, interacting with the filesystem
        with self.local_manager._watch_logs(  # pylint: disable=protected-access
            pipeline_run, step_key
        ):
            yield

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "storage_account": StringSource,
            "container": StringSource,
            "secret_key": StringSource,
            "local_dir": Field(StringSource, is_required=False),
            "prefix": Field(StringSource, is_required=False, default_value="dagster"),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return AzureBlobComputeLogManager(inst_data=inst_data, **config_value)

    def get_local_path(self, run_id, key, io_type):
        return self.local_manager.get_local_path(run_id, key, io_type)

    def on_watch_start(self, pipeline_run, step_key):
        self.local_manager.on_watch_start(pipeline_run, step_key)

    def on_watch_finish(self, pipeline_run, step_key):
        self.local_manager.on_watch_finish(pipeline_run, step_key)
        key = self.local_manager.get_key(pipeline_run, step_key)
        self._upload_from_local(pipeline_run.run_id, key, ComputeIOType.STDOUT)
        self._upload_from_local(pipeline_run.run_id, key, ComputeIOType.STDERR)

    def is_watch_completed(self, run_id, key):
        return self.local_manager.is_watch_completed(run_id, key)

    def download_url(self, run_id, key, io_type):
        if not self.is_watch_completed(run_id, key):
            return self.local_manager.download_url(run_id, key, io_type)
        key = self._blob_key(run_id, key, io_type)
        if key in self._download_urls:
            return self._download_urls[key]
        blob = self._container_client.get_blob_client(key)
        sas = generate_blob_sas(
            self._storage_account,
            self._container,
            key,
            account_key=self._blob_client.credential.account_key,
        )
        url = blob.url + sas
        self._download_urls[key] = url
        return url

    def read_logs_file(self, run_id, key, io_type, cursor=0, max_bytes=MAX_BYTES_FILE_READ):
        if self._should_download(run_id, key, io_type):
            self._download_to_local(run_id, key, io_type)
        data = self.local_manager.read_logs_file(run_id, key, io_type, cursor, max_bytes)
        return self._from_local_file_data(run_id, key, io_type, data)

    def on_subscribe(self, subscription):
        self.local_manager.on_subscribe(subscription)

    def on_unsubscribe(self, subscription):
        self.local_manager.on_unsubscribe(subscription)

    def _should_download(self, run_id, key, io_type):
        local_path = self.get_local_path(run_id, key, io_type)
        if os.path.exists(local_path):
            return False
        blob_objects = self._container_client.list_blobs(self._blob_key(run_id, key, io_type))
        # Limit the generator to avoid paging since we only need one element
        # to return True
        limited_blob_objects = itertools.islice(blob_objects, 1)
        return len(list(limited_blob_objects)) > 0

    def _from_local_file_data(self, run_id, key, io_type, local_file_data):
        is_complete = self.is_watch_completed(run_id, key)
        path = (
            "https://{account}.blob.core.windows.net/{container}/{key}".format(
                account=self._storage_account,
                container=self._container,
                key=self._blob_key(run_id, key, io_type),
            )
            if is_complete
            else local_file_data.path
        )

        return ComputeLogFileData(
            path,
            local_file_data.data,
            local_file_data.cursor,
            local_file_data.size,
            self.download_url(run_id, key, io_type),
        )

    def _upload_from_local(self, run_id, key, io_type):
        path = self.get_local_path(run_id, key, io_type)
        ensure_file(path)
        key = self._blob_key(run_id, key, io_type)
        with open(path, "rb") as data:
            blob = self._container_client.get_blob_client(key)
            blob.upload_blob(data)

    def _download_to_local(self, run_id, key, io_type):
        path = self.get_local_path(run_id, key, io_type)
        ensure_dir(os.path.dirname(path))
        key = self._blob_key(run_id, key, io_type)
        with open(path, "wb") as fileobj:
            blob = self._container_client.get_blob_client(key)
            blob.download_blob().readinto(fileobj)

    def _blob_key(self, run_id, key, io_type):
        check.inst_param(io_type, "io_type", ComputeIOType)
        extension = IO_TYPE_EXTENSION[io_type]
        paths = [
            self._blob_prefix,
            "storage",
            run_id,
            "compute_logs",
            "{}.{}".format(key, extension),
        ]
        return "/".join(paths)  # blob path delimiter

    def dispose(self):
        self.local_manager.dispose()
