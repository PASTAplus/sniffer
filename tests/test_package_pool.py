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
from sniffer.model.sniffer_db import PackagePool


ABQ_TZ = tz.gettz("America/Denver")


@pytest.fixture()
def pp():
    package_pool = PackagePool(Config.SNIFFER_TEST_DB)
    return package_pool


@pytest.fixture()
def clean_up():
    yield
    Path(Config.SNIFFER_TEST_DB).unlink(missing_ok=True)


def test_add_new_packages(pp, clean_up):
    limit = 5
    c = sniffer.package_pool.add_new_packages(pp=pp, limit=limit)
    assert c == limit
    p = pp.get_all_packages()
    assert len(p) == limit
    c = sniffer.package_pool.add_new_packages(pp=pp, limit=limit)
    assert c == limit
    p = pp.get_all_packages()
    assert len(p) == limit * 2
