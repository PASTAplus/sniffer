#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_embargo_pool

:Synopsis:

:Author:
    servilla

:Created:
    8/2/20
"""
from datetime import datetime
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.model.embargo_db import EmbargoDB
import sniffer.embargo.embargo_pool as ep
from sniffer.embargo.embargo_pool import EmbargoPool


TEST_EMBARGO_METADATA_RESOURCE = (
    [
        "https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1210/3",
        "knb-lter-fce.1210.3",
    ],
    [
        "https://pasta.lternet.edu/package/metadata/eml/knb-lter-kbs/140/5",
        "knb-lter-kbs.140.5",
    ],
)

TEST_EMBARGO_DATA_RESOURCE = (
    [
        "https://pasta.lternet.edu/package/data/eml/edi/512/1/bb87318745d9b83f102aa0a58e9b5386",
        "edi.512.1",
        "2020-05-13 13:44:42.444000",
    ],
    [
        "https://pasta.lternet.edu/package/data/eml/edi/512/1/c858be23bac4b5f93b830bcbdac6ba2c",
        "edi.512.1",
        "2020-05-13 13:44:42.444000",
    ],
    [
        "https://pasta.lternet.edu/package/data/eml/knb-lter-fce/1210/4/c7d09464082a09ec5c232ee314a4f42a",
        "knb-lter-fce.1210.4",
    ],
)
Config.PATH = Config.TEST_PATH
db_path = Config.PATH + Config.EMBARGO_DB


@pytest.fixture()
def embargo_pool():
    return EmbargoPool()

@pytest.fixture()
def e_db():
    return EmbargoDB(db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(db_path).unlink(missing_ok=True)


def test_embargo_pool_instance(embargo_pool, e_db, clean_up):
    pass


def test_add_new_embargoed_resources(embargo_pool, e_db, clean_up):
    a = embargo_pool.add_new_embargoed_resources()
    c = e_db.get_count()
    assert a == c


def test_generate_pid(embargo_pool, e_db, clean_up):
    pid = ep.generate_pid(TEST_EMBARGO_METADATA_RESOURCE[0][0])
    assert pid == TEST_EMBARGO_METADATA_RESOURCE[0][1]

    pid = ep.generate_pid(TEST_EMBARGO_DATA_RESOURCE[0][0])
    assert pid == TEST_EMBARGO_DATA_RESOURCE[0][1]


def test_verify_metadata_embargo(embargo_pool, e_db, clean_up):
    pk = e_db.insert(
        TEST_EMBARGO_METADATA_RESOURCE[0][0],
        TEST_EMBARGO_METADATA_RESOURCE[0][1],
    )
    assert pk == 1

    pk = e_db.insert(
        TEST_EMBARGO_METADATA_RESOURCE[1][0],
        TEST_EMBARGO_METADATA_RESOURCE[1][1],
    )
    assert pk == 2

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[0][0],
        TEST_EMBARGO_DATA_RESOURCE[0][1],
    )
    assert pk == 3

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[1][0],
        TEST_EMBARGO_DATA_RESOURCE[1][1],
    )
    assert pk == 4

    embargo_pool.verify_metadata_embargo()
    pass
