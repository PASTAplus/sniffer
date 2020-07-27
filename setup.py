#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: setup.py

:Synopsis:

:Author:
    servilla

:Created:
    5/18/2020
"""
from os import path
from setuptools import find_packages, setup


here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

with open(path.join(here, "LICENSE"), encoding="utf-8") as f:
    full_license = f.read()

setup(
    name="sniffer",
    version="2020.07.24",
    description=(
        "Data package sniffer framework to analyze aspects"
        " of the PASTA+ data package"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="PASTA+ project",
    url="https://github.com/PASTAplus/sniffer",
    license=full_license,
    packages=find_packages(where="src"),
    include_package_data=True,
    exclude_package_data={"": ["settings.py, properties.py, config.py"],},
    package_dir={"": "src"},
    python_requires=">3.8.*",
    install_requires=[
        "click >= 7.1.1",
        "daiquiri >= 2.1.1",
        "python-dateutil >= 2.8.1",
        "sqlalchemy >= 1.3.16",
        "pendulum >= 2.1.0",
        "psycopg2 >= 2.8.5",
        "pytest >= 5.4.3",
    ],
    entry_points={"console_scripts": ["sniffer=sniffer.sniff:main"]},
    classifiers=["License :: OSI Approved :: Apache Software License",],
)


def main():
    return 0


if __name__ == "__main__":
    main()
