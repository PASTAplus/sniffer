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
from datetime import datetime, timezone
from dateutil import tz
from pathlib import Path

import pytest

from sniffer.config import Config
import sniffer.package_pool
from sniffer.model.package_db import PackageDB


ABQ_TZ = tz.gettz("America/Denver")
package_db = Config.TEST_DB_PATH + Config.PACKAGE_DB


@pytest.fixture()
def p_db():
    package_pool = PackageDB(package_db)
    return package_pool


@pytest.fixture()
def clean_up():
    yield
    Path(package_db).unlink(missing_ok=True)


def test_add_new_packages(p_db, clean_up):
    limit = 5
    c = sniffer.package_pool.add_new_packages(p_db=p_db, limit=limit)
    assert c == limit
    p = p_db.get_all_packages()
    assert len(p) == limit
    c = sniffer.package_pool.add_new_packages(p_db=p_db, limit=limit)
    assert c == limit
    p = p_db.get_all_packages()
    assert len(p) == limit * 2
