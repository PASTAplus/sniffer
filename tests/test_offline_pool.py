#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_offline_pool

:Synopsis:

:Author:
    servilla

:Created:
    7/29/20
"""
from datetime import datetime
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.offline.offline_pool import OfflinePool
from sniffer.model.package_db import PackageDB

TEST_PACKAGE_DATA = [
    (
        "knb-lter-nin.1.1",
        datetime(2013, 1, 10, 14, 45, 31, 1000),
        None,
        "doi:10.6073/pasta/0675d3602ff57f24838ca8d14d7f3961",
    ),
    (
        "edi.115.1",
        datetime(2018, 10, 17, 17, 8, 18, 734000),
        None,
        "doi:10.6073/pasta/4e1745ea523325bf35bedc88e7a9b4d0",
    ),
]
Config.PATH = Config.TEST_PATH
o_db_path = Config.PATH + Config.OFFLINE_DB
p_db_path = Config.PATH + Config.PACKAGE_DB
o_date_path = Config.PATH + Config.OFFLINE_DATE


@pytest.fixture()
def offline_pool():
    return OfflinePool()


@pytest.fixture()
def p_db():
    return PackageDB(p_db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(o_db_path).unlink(missing_ok=True)
    Path(p_db_path).unlink(missing_ok=True)
    Path(o_date_path).unlink(missing_ok=True)


def test_offline_pool(offline_pool, p_db, clean_up):
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[0][0],
        TEST_PACKAGE_DATA[0][1],
        TEST_PACKAGE_DATA[0][2],
        TEST_PACKAGE_DATA[0][3],
    )
    assert pk == TEST_PACKAGE_DATA[0][0]
    pk = p_db.insert_package(
        TEST_PACKAGE_DATA[1][0],
        TEST_PACKAGE_DATA[1][1],
        TEST_PACKAGE_DATA[1][2],
        TEST_PACKAGE_DATA[1][3],
    )
    assert pk == TEST_PACKAGE_DATA[1][0]

    c = offline_pool.add_new_offline_resources()
    assert c == 0
