import dagstermill as dm

from dagster import InputDefinition, job
from dagster._utils import script_relative_path
from docs_snippets.legacy.data_science.download_file import download_file

k_means_iris = dm.define_dagstermill_op(
    "k_means_iris",
    script_relative_path("iris-kmeans_2.ipynb"),
    output_notebook_name="iris_kmeans_output",
    input_defs=[
        InputDefinition("path", str, description="Local path to the Iris dataset")
    ],
)


@job(
    resource_defs={
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    }
)
def iris_classify():
    k_means_iris(download_file())
