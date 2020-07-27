#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_model

:Synopsis:

:Author:
    servilla

:Created:
    7/26/20
"""
from datetime import datetime
from dateutil import tz
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.model.sniffer_db import PackagePool, Packages


TEST_PACKAGE_DATA = (
    "edi.1.1",
    datetime(2016, 12, 1, 12, 55, 8, 778000),
    datetime(2018, 12, 1, 12, 55, 8, 778000),
    "doi:10.6073/pasta/a30d5b90676008cfb7987f31b4343a35",
)

ABQ_TZ = tz.gettz("America/Denver")


@pytest.fixture()
def pp():
    package_pool = PackagePool(Config.SNIFFER_TEST_DB)
    return package_pool


@pytest.fixture()
def clean_up():
    yield
    Path(Config.SNIFFER_TEST_DB).unlink(missing_ok=True)


def test_pasta_db_connection():
    pass


def test_resource_pool_create(pp, clean_up):
    assert Path(Config.SNIFFER_TEST_DB).exists()


def test_insert_get_package(pp, clean_up):
    pp.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    p = pp.get_package(TEST_PACKAGE_DATA[0])
    assert isinstance(p, Packages)
    assert p.pid == TEST_PACKAGE_DATA[0]


def test_package_not_found(pp, clean_up):
    p = pp.get_package(pid=TEST_PACKAGE_DATA[0])
    assert p is None


def test_package_count(pp, clean_up):
    pp.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    c = pp.get_count()
    assert c == 1
