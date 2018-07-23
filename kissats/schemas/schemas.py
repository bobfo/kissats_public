"""Schema definitions"""

from distutils.sysconfig import get_python_lib
import logging

from cerberus import Validator
import pathlib2 as pathlib

from kissats.common import load_data_file
from kissats.exceptions import SchemaMisMatch


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def load_schema(schema_location):
    """Load a schema from a .json or .yaml file

    Args:
        schema_location (string or pathlib.Path): Absolute location of the schema file
                                                  to load.

    Returns:
        (dict)

    """

    # TODO(BF): add check to make sure it is a valid schema format

    return load_data_file(schema_location)


def normalize_and_validate(dict_to_check, schema):
    # type: (dict, dict) -> (dict)
    """Normalize and validate a dictionary, will raise if invalid

    Args:
        dict_to_check(dict): dictionary to check
        schema(dict): schema to use

    Returns:
        (tuple):
            (dict): Normalized and valid dictionary

    Raises:
        SchemaMisMatch:

    """

    c_validator = Validator(schema, allow_unknown=True)
    valid_dict = c_validator.validated(dict_to_check, normalize=True)
    if valid_dict is None:
        raise SchemaMisMatch(c_validator.errors)

    return valid_dict


class MasterSchemaDirectory(object):
    """Master Schema directory"""

    def __init__(self):
        super(MasterSchemaDirectory, self).__init__()

        self._base_schema_location = None
        self._reporting_schema = None
        self._task_param_schema = None
        self._task_return_schema = None
        self._global_param_schema = None

    @property
    def base_schema_location(self):
        """location of the schema dir"""
        if self._base_schema_location is None:
            self._base_schema_location = pathlib.PurePath(get_python_lib(),
                                                          "kissats",
                                                          "schemas")
        return self._base_schema_location

    @property
    def reporting_schema(self):
        """The reporting schema"""

        if self._reporting_schema is None:
            self._reporting_schema = self._get_schema("reporting_schema.yaml")
        return self._reporting_schema

    @property
    def task_param_schema(self):
        """Task param schema"""

        if self._task_param_schema is None:
            self._task_param_schema = self._get_schema("task_param_schema.yaml")
        return self._task_param_schema

    @property
    def task_return_schema(self):
        """Task return schema"""

        if self._task_return_schema is None:
            self._task_return_schema = self._get_schema("task_return_schema.yaml")
        return self._task_return_schema

    @property
    def global_param_schema(self):
        """Global param schema"""

        if self._global_param_schema is None:
            self._global_param_schema = self._get_schema("global_param_in_schema.yaml")
        return self._global_param_schema

    def _get_schema(self, yaml_name):
        # type: (string) -> dict
        """Load the schema file from the schemas location"""

        full_path = self.base_schema_location.joinpath(yaml_name)
        return load_schema(full_path)
