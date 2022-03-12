from dagster import check

from .dagster_type import PythonObjectDagsterType, make_python_type_usable_as_dagster_type


def usable_as_dagster_type(
    name=None,
    description=None,
    loader=None,
    materializer=None,
):
    """Decorate a Python class to make it usable as a Dagster Type.

    This is intended to make it straightforward to annotate existing business logic classes to
    make them dagster types whose typecheck is an isinstance check against that python class.

    Args:
        python_type (cls): The python type to make usable as python type.
        name (Optional[str]): Name of the new Dagster type. If ``None``, the name (``__name__``) of
            the ``python_type`` will be used.
        description (Optional[str]): A user-readable description of the type.
        loader (Optional[DagsterTypeLoader]): An instance of a class that
            inherits from :py:class:`DagsterTypeLoader` and can map config data to a value of
            this type. Specify this argument if you will need to shim values of this type using the
            config machinery. As a rule, you should use the
            :py:func:`@dagster_type_loader <dagster.dagster_type_loader>` decorator to construct
            these arguments.
        materializer (Optional[DagsterTypeMaterializer]): An instance of a class
            that inherits from :py:class:`DagsterTypeMaterializer` and can persist values of
            this type. As a rule, you should use the
            :py:func:`@dagster_type_materializer <dagster.dagster_type_materializer>`
            decorator to construct these arguments.

    Examples:

    .. code-block:: python

        # dagster_aws.s3.file_manager.S3FileHandle
        @usable_as_dagster_type
        class S3FileHandle(FileHandle):
            def __init__(self, s3_bucket, s3_key):
                self._s3_bucket = check.str_param(s3_bucket, 's3_bucket')
                self._s3_key = check.str_param(s3_key, 's3_key')

            @property
            def s3_bucket(self):
                return self._s3_bucket

            @property
            def s3_key(self):
                return self._s3_key

            @property
            def path_desc(self):
                return self.s3_path

            @property
            def s3_path(self):
                return 's3://{bucket}/{key}'.format(bucket=self.s3_bucket, key=self.s3_key)
    """

    def _with_args(bare_cls):
        check.class_param(bare_cls, "bare_cls")
        new_name = name if name else bare_cls.__name__

        make_python_type_usable_as_dagster_type(
            bare_cls,
            PythonObjectDagsterType(
                name=new_name,
                description=description,
                python_type=bare_cls,
                loader=loader,
                materializer=materializer,
            ),
        )
        return bare_cls

    # check for no args, no parens case
    if callable(name):
        bare_cls = name  # with no parens, name is actually the decorated class
        make_python_type_usable_as_dagster_type(
            bare_cls,
            PythonObjectDagsterType(python_type=bare_cls, name=bare_cls.__name__, description=None),
        )
        return bare_cls

    return _with_args
