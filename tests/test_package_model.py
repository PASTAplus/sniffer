#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_package_model

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
from sniffer.model.package_db import PackageDB, Package


TEST_PACKAGE_DATA = (
    "edi.1.1",
    datetime(2016, 12, 1, 12, 55, 8, 778000),
    datetime(2018, 12, 1, 12, 55, 8, 778000),
    "doi:10.6073/pasta/a30d5b90676008cfb7987f31b4343a35",
)
ABQ_TZ = tz.gettz("America/Denver")
Config.PATH = Config.TEST_PATH
db_path = Config.PATH + Config.PACKAGE_DB


@pytest.fixture()
def p_db():
    return PackageDB(db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(db_path).unlink(missing_ok=True)


def test_pasta_db_connection():
    pass


def test_resource_pool_create(p_db, clean_up):
    assert Path(db_path).exists()


def test_insert_get_package(p_db, clean_up):
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    p = p_db.get_package(TEST_PACKAGE_DATA[0])
    assert isinstance(p, Package)
    assert p.pid == TEST_PACKAGE_DATA[0]


def test_package_not_found(p_db, clean_up):
    p = p_db.get_package(pid=TEST_PACKAGE_DATA[0])
    assert p is None


def test_package_count(p_db, clean_up):
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    c = p_db.get_count()
    assert c == 1


def test_get_all_packages(p_db, clean_up):
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    c = p_db.get_count()
    assert c == 1

    packages = p_db.get_all_packages()
    for package in packages:
        assert package.pid == TEST_PACKAGE_DATA[0]
        assert package.date_created == TEST_PACKAGE_DATA[1]
        assert package.date_deactivated == TEST_PACKAGE_DATA[2]
        assert package.doi == TEST_PACKAGE_DATA[3]


def test_get_all_from_date(p_db, clean_up):
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[0],
        TEST_PACKAGE_DATA[1],
        TEST_PACKAGE_DATA[2],
        TEST_PACKAGE_DATA[3],
    )
    c = p_db.get_count()
    assert c == 1

    from_date = Config.START_DATE
    packages = p_db.get_all_from_date(from_date=from_date)
    for package in packages:
        assert package.pid == TEST_PACKAGE_DATA[0]
        assert package.date_created == TEST_PACKAGE_DATA[1]
        assert package.date_deactivated == TEST_PACKAGE_DATA[2]
        assert package.doi == TEST_PACKAGE_DATA[3]
