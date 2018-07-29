"""Some common helper functions"""

import json
import logging
import subprocess
import sys

import pathlib2 as pathlib
import yaml

from kissats.exceptions import InvalidDataFile


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def pip_in_package(package_to_pip):
    """Pip install a package into the current environment"""

    try:
        subprocess.check_call([sys.executable,
                               '-m',
                               'pip',
                               'install',
                               package_to_pip])
    except subprocess.CalledProcessError as err:
        return err.returncode
    return 0


def load_data_file(file_location):
    """Load a schema from a .json or .yaml file

    Args:
        file_location (string or pathlib.Path): Absolute location of the data file
                                                to load.

    Returns:
        dict: data file contents

    """

    file_location = pathlib.Path(file_location)

    with file_location.open() as fh:
        if file_location.suffix.lower() == ".yaml":
            data_out = yaml.load(fh)
        elif file_location.suffix.lower() == ".json":
            data_out = json.load(fh)
        else:
            raise InvalidDataFile()

    return data_out
