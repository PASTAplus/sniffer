#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_package_pool

:Synopsis:

:Author:
    servilla

:Created:
    7/26/20
"""
from dateutil import tz
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.package.package_pool import PackagePool


ABQ_TZ = tz.gettz("America/Denver")
Config.PATH = Config.TEST_PATH
db_path = Config.PATH + Config.PACKAGE_DB


@pytest.fixture()
def package_pool():
    return PackagePool()


@pytest.fixture()
def clean_up():
    yield
    Path(db_path).unlink(missing_ok=True)


def test_add_new_packages(package_pool, clean_up):
    limit = 5
    c = package_pool.add_new_packages(limit=limit)
    assert c == limit
    p = package_pool.get_all_packages()
    assert len(p) == limit
    c = package_pool.add_new_packages(limit=limit)
    assert c == limit
    p = package_pool.get_all_packages()
    assert len(p) == limit * 2
