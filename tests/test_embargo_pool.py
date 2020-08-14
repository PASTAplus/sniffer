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
import requests

from sniffer.config import Config
from sniffer.model.embargo_db import EmbargoDB
import sniffer.embargo.embargo_pool as ep
from sniffer.embargo.embargo_pool import EmbargoPool
from sniffer.model.package_db import PackageDB

TEST_PACKAGE_DATA = (
    [
        "edi.512.1",
        datetime(2020, 5, 13, 13, 44, 42, 444000),
        None,
        "doi:10.6073/pasta/27dc02fe1655e3896f20326fed5cb95f"
    ],
    [
        "edi.50.1",
        datetime(2017, 8, 5, 16, 53, 58, 820000),
        None,
        "doi:10.6073/pasta/66c1cd4965e81612e93bf87f0dd0f33e"
    ],
    [
        "knb-lter-kbs.140.5",
        datetime(2018, 2, 14, 9, 4, 5, 131000),
        None,
        None
    ],
    [
        "knb-lter-knz.201.1",
        datetime(2018, 2, 14, 9, 4, 5, 131000),
        None,
        None
    ],
)

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
    [
        "https://pasta.lternet.edu/package/data/eml/edi/50/1/a2adee56fd9c7285e546c53f50c368b4",
        "edi.50.1",
    ],
)
Config.PATH = Config.TEST_PATH
e_db_path = Config.PATH + Config.EMBARGO_DB
p_db_path = Config.PATH + Config.PACKAGE_DB
embargo_date_path = Config.PATH + Config.EMBARGO_DATE


@pytest.fixture()
def embargo_pool():
    return EmbargoPool()


@pytest.fixture()
def e_db():
    return EmbargoDB(e_db_path)


@pytest.fixture()
def p_db():
    return PackageDB(p_db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(e_db_path).unlink(missing_ok=True)
    Path(p_db_path).unlink(missing_ok=True)
    Path(embargo_date_path).unlink(missing_ok=True)


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

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[2][0],
        TEST_EMBARGO_DATA_RESOURCE[2][1],
    )
    assert pk == 5

    embargo_pool.verify_metadata_embargo()


def test_identify_ephemeral_embargoes(embargo_pool, e_db, clean_up):
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

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[2][0],
        TEST_EMBARGO_DATA_RESOURCE[2][1],
    )
    assert pk == 5

    c = embargo_pool.identify_ephemeral_embargoes()
    assert c == 2


def test_embargo_pool(p_db, embargo_pool, e_db, clean_up):
    c = embargo_pool.add_new_embargoed_resources()
    assert c != 0



