"""
Setup for KISS ATS

"""

import os
from setuptools import (setup,
                        find_packages)

HERE = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(HERE, 'kissats', 'VERSION'), "r") as version_file:
    VERSION = version_file.read().strip()

setup(
    name="kissats",
    version=VERSION,
    url="https://github.com/bobfo/kissats_public",
    author="Bob Folkes",
    author_email="bob@nwcompnet.com",
    description=("A simple ATS"),
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Manufacturing",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Testing",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Utilities",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 2 :: Only",
        "Programming Language :: Python :: 2.7",
    ],
    keywords='ats simple test automation',
    packages=find_packages(exclude=['docs', 'tests*', '.git', '.vs']),
    install_requires=[],
    data_files=[("\\kissats\\", [".\\kissats\\VERSION"])]
)
