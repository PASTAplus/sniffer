#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_embargo_model

:Synopsis:

:Author:
    servilla

:Created:
    7/31/20
"""
from datetime import datetime
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.model.embargo_db import EmbargoDB, EmbargoedResource

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
def e_db():
    return EmbargoDB(db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(db_path).unlink(missing_ok=True)


def test_pasta_db_connection():
    pass


def test_resource_pool_create(e_db, clean_up):
    assert Path(db_path).exists()
    embargoes = e_db.get_all()
    assert embargoes is not None


def test_get_by_id(e_db, clean_up):
    pk = e_db.insert(
        TEST_EMBARGO_METADATA_RESOURCE[0][0],
        TEST_EMBARGO_METADATA_RESOURCE[0][1],
    )
    assert pk == 1

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[0][0], TEST_EMBARGO_DATA_RESOURCE[0][1]
    )
    assert pk == 2

    e = e_db.get_by_id(id=1)
    assert isinstance(e, EmbargoedResource)
    assert e.rid == TEST_EMBARGO_METADATA_RESOURCE[0][0]
    assert e.pid == TEST_EMBARGO_METADATA_RESOURCE[0][1]


def test_insert_embargoed_resource(e_db, clean_up):
    pk = e_db.insert(
        TEST_EMBARGO_METADATA_RESOURCE[0][0],
        TEST_EMBARGO_METADATA_RESOURCE[0][1],
    )
    assert pk == 1

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[0][0], TEST_EMBARGO_DATA_RESOURCE[0][1]
    )
    assert pk == 2


def test_ephemeral_date(e_db, clean_up):
    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[0][0],
        TEST_EMBARGO_DATA_RESOURCE[0][1],
        datetime.fromisoformat(TEST_EMBARGO_DATA_RESOURCE[0][2]),
    )
    assert pk == 1
    e = e_db.get_by_id(pk)
    assert e.date_ephemeral == datetime.fromisoformat(
        TEST_EMBARGO_DATA_RESOURCE[0][2]
    )

    pk = e_db.insert(
        TEST_EMBARGO_DATA_RESOURCE[1][0],
        TEST_EMBARGO_DATA_RESOURCE[1][1],
        datetime.fromisoformat(TEST_EMBARGO_DATA_RESOURCE[1][2]),
    )
    assert pk == 2
    e = e_db.get_by_id(pk)
    assert e.date_ephemeral == datetime.fromisoformat(
        TEST_EMBARGO_DATA_RESOURCE[1][2]
    )


def test_distinct_pids(e_db, clean_up):
    test_pids = (
        TEST_EMBARGO_METADATA_RESOURCE[0][1],
        TEST_EMBARGO_METADATA_RESOURCE[1][1],
        TEST_EMBARGO_DATA_RESOURCE[1][1],
    )

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

    pids = e_db.get_distinct_pids()
    for pid in pids:
        assert pid[0] in test_pids


def test_package_level_embargoes(e_db, clean_up):
    test_pids = (
        TEST_EMBARGO_METADATA_RESOURCE[0][1],
        TEST_EMBARGO_METADATA_RESOURCE[1][1],
    )

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

    embargoes = e_db.get_all_package_level_embargoes()
    for embargo in embargoes:
        assert embargo.pid in test_pids
