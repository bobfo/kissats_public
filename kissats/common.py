"""
Some common helper functions

"""


import sys
import subprocess
import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def pip_in_package(package_to_pip):
    """
    Pip install a package into the current environment

    """

    try:
        subprocess.check_call([sys.executable,
                               '-m',
                               'pip',
                               'install',
                               package_to_pip])
    except subprocess.CalledProcessError as err:
        return err.returncode
    return 0
