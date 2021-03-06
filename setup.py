"""Setup for KISS ATS"""

from setuptools import (setup,
                        find_packages)

setup(
    name="kissats",
    version="1.0.0a6",
    url="https://kissats-public.readthedocs.io/en/latest/index.html",
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
    package_data={"kissats": ["schemas/*.yaml"]},
    install_requires=["pathlib2",
                      "pyyaml",
                      "cerberus",
                      "six"]
)
