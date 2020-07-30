#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_offline_model

:Synopsis:

:Author:
    servilla

:Created:
    7/27/20
"""
from pathlib import Path

import pytest

from sniffer.config import Config
from sniffer.model.offline_db import OfflineDB, OfflineResource

TEST_OFFLINE_RESOURCE_DATA = ("edi.1.1", "test_object", "local_disk")
Config.PATH = Config.TEST_PATH
db_path = Config.PATH + Config.OFFLINE_DB


@pytest.fixture()
def o_db():
    return OfflineDB(db_path)


@pytest.fixture()
def clean_up():
    yield
    Path(db_path).unlink(missing_ok=True)


def test_pasta_db_connection():
    pass


def test_resource_pool_create(o_db, clean_up):
    assert Path(db_path).exists()


def test_insert_offline_resource(o_db, clean_up):
    oid = o_db.insert_offline_resource(
        TEST_OFFLINE_RESOURCE_DATA[0],
        TEST_OFFLINE_RESOURCE_DATA[1],
        TEST_OFFLINE_RESOURCE_DATA[2],
    )
    assert oid is not None


def test_get_offline_resource_by_oid(o_db, clean_up):
    oid = o_db.insert_offline_resource(
        TEST_OFFLINE_RESOURCE_DATA[0],
        TEST_OFFLINE_RESOURCE_DATA[1],
        TEST_OFFLINE_RESOURCE_DATA[2],
    )
    assert oid is not None
    o = o_db.get_offline_resource_by_oid(oid=oid)
    assert o is not None
    assert o.pid == TEST_OFFLINE_RESOURCE_DATA[0]
    assert o.object_name == TEST_OFFLINE_RESOURCE_DATA[1]
    assert o.medium == TEST_OFFLINE_RESOURCE_DATA[2]


def test_get_all_offline_resources(o_db, clean_up):
    oid = o_db.insert_offline_resource(
        TEST_OFFLINE_RESOURCE_DATA[0],
        TEST_OFFLINE_RESOURCE_DATA[1],
        TEST_OFFLINE_RESOURCE_DATA[2],
    )
    assert oid is not None
    oid = o_db.insert_offline_resource(
        TEST_OFFLINE_RESOURCE_DATA[0],
        TEST_OFFLINE_RESOURCE_DATA[1],
        TEST_OFFLINE_RESOURCE_DATA[2],
    )
    assert oid is not None

    resources = o_db.get_all_offline_resources()
    for resource in resources:
        assert resource.pid == TEST_OFFLINE_RESOURCE_DATA[0]
        assert resource.object_name == TEST_OFFLINE_RESOURCE_DATA[1]
        assert resource.medium == TEST_OFFLINE_RESOURCE_DATA[2]


def test_get_offline_resource_by_pid(o_db, clean_up):
    oid = o_db.insert_offline_resource(
        TEST_OFFLINE_RESOURCE_DATA[0],
        TEST_OFFLINE_RESOURCE_DATA[1],
        TEST_OFFLINE_RESOURCE_DATA[2],
    )
    assert oid is not None

    resources = o_db.get_offline_resource_by_pid(TEST_OFFLINE_RESOURCE_DATA[0])
    for resource in resources:
        assert resource.pid == TEST_OFFLINE_RESOURCE_DATA[0]
        assert resource.object_name == TEST_OFFLINE_RESOURCE_DATA[1]
        assert resource.medium == TEST_OFFLINE_RESOURCE_DATA[2]
