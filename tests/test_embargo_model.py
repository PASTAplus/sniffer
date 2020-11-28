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
from sniffer.model.embargo_db import (
    EmbargoDB,
    Ephemeral,
)

TEST_EMBARGO_METADATA_RESOURCE = (
    [
        "https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1210/3",
        "knb-lter-fce.1210.3",
        0,
        False
    ],
    [
        "https://pasta.lternet.edu/package/metadata/eml/knb-lter-kbs/140/5",
        "knb-lter-kbs.140.5",
        1,
        True
    ],
)

TEST_EMBARGO_DATA_RESOURCE = (
    [
        "https://pasta.lternet.edu/package/data/eml/edi/512/1/bb87318745d9b83f102aa0a58e9b5386",
        "edi.512.1",
        "2020-05-13 13:44:41.369000",
         1,
        True
   ],
    [
        "https://pasta.lternet.edu/package/data/eml/edi/512/1/c858be23bac4b5f93b830bcbdac6ba2c",
        "edi.512.1",
        "2020-05-13 13:44:40.776000",
         1,
        True
   ],
    [
        "https://pasta.lternet.edu/package/data/eml/knb-lter-fce/1210/4/c7d09464082a09ec5c232ee314a4f42a",
        "knb-lter-fce.1210.4",
         0,
        False
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


def test_embargo_db_connection(e_db, clean_up):
    pass


def test_insert_authenticated(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_METADATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_METADATA_RESOURCE[0][1],
        type=TEST_EMBARGO_METADATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_METADATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 2

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[1][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[1][3],
    )
    assert pk == 3


def test_insert_ephemeral(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[1][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[1][3],
    )
    assert pk == 2


def test_insert_explicit(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_METADATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_METADATA_RESOURCE[0][1],
        type=TEST_EMBARGO_METADATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_METADATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 2

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[1][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[1][3],
    )
    assert pk == 3


def test_insert_implicit(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_METADATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_METADATA_RESOURCE[0][1],
        type=TEST_EMBARGO_METADATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_METADATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 2

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[1][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[1][3],
    )
    assert pk == 3


def test_get_resources(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_METADATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_METADATA_RESOURCE[0][1],
        type=TEST_EMBARGO_METADATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_METADATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 2

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[1][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[1][3],
    )
    assert pk == 3

    resources = e_db.get_all()
    for resource in resources:
        assert resource.rid in (
            TEST_EMBARGO_METADATA_RESOURCE[0][0],
            TEST_EMBARGO_DATA_RESOURCE[0][0],
            TEST_EMBARGO_DATA_RESOURCE[1][0],
        )

    resources = e_db.get_all_metadata()
    for resource in resources:
        assert resource.rid == TEST_EMBARGO_METADATA_RESOURCE[0][0]

    resources = e_db.get_all_data()
    for resource in resources:
        assert resource.rid in (
            TEST_EMBARGO_DATA_RESOURCE[0][0],
            TEST_EMBARGO_DATA_RESOURCE[1][0],
        )

    resources = e_db.get_distinct_pids()
    for resource in resources:
        assert resource.pid in (
            TEST_EMBARGO_METADATA_RESOURCE[0][1],
            TEST_EMBARGO_DATA_RESOURCE[1][1],
        )

    count = e_db.get_count()
    assert count == 3

    resource = e_db.get_by_id(id=2,)
    assert resource is not None
    assert resource.id == 2
    assert resource.rid == TEST_EMBARGO_DATA_RESOURCE[0][0]

    resources = e_db.get_by_pid(pid="edi.512.1")
    for resource in resources:
        assert resource.rid in (
            TEST_EMBARGO_DATA_RESOURCE[0][0],
            TEST_EMBARGO_DATA_RESOURCE[1][0],
        )

    resource = e_db.get_by_rid(rid=TEST_EMBARGO_DATA_RESOURCE[0][0])
    assert resource.rid == TEST_EMBARGO_DATA_RESOURCE[0][0]


def test_delete_all(e_db, clean_up):
    pk = e_db.insert(
        rid=TEST_EMBARGO_METADATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_METADATA_RESOURCE[0][1],
        type=TEST_EMBARGO_METADATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_METADATA_RESOURCE[0][3],
    )
    assert pk == 1

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[0][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[0][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
    )
    assert pk == 2

    pk = e_db.insert(
        rid=TEST_EMBARGO_DATA_RESOURCE[1][0],
        pid=TEST_EMBARGO_DATA_RESOURCE[1][1],
        type=TEST_EMBARGO_DATA_RESOURCE[0][2],
        auth=TEST_EMBARGO_DATA_RESOURCE[0][3],
   )
    assert pk == 3

    count = e_db.get_count()
    assert count == 3
    e_db.delete_all()
    count = e_db.get_count()
    assert count == 0
