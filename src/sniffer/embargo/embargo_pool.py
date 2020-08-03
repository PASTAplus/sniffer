#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: embargo_pool

:Synopsis:

:Author:
    servilla

:Created:
    7/31/20
"""
from pathlib import Path

import daiquiri
from lxml import etree
import requests
from sqlalchemy.exc import IntegrityError

from sniffer.config import Config
import sniffer.last_date as last_date
from sniffer.model.embargo_db import EmbargoDB
from sniffer.model import pasta_data_package_manager_db
from sniffer.package.package_pool import PackagePool


logger = daiquiri.getLogger(__name__)

SQL_METADATA_RESOURCES = (
    "SELECT datapackagemanager.access_matrix.resource_id "
    "FROM datapackagemanager.access_matrix WHERE "
    "resource_id LIKE '%%/metadata/eml/%%'  AND principal='public' "
    " AND access_type='deny'"
)

SQL_DATA_RESOURCES = (
    "SELECT datapackagemanager.access_matrix.resource_id "
    "FROM datapackagemanager.access_matrix WHERE "
    "resource_id LIKE '%%/data/eml/%%'  AND principal='public' "
    " AND access_type='deny'"
)


def entity_embargo_parse(eml: str):
    resources = list()
    tree = etree.fromstring(eml.encode("utf-8"))
    phys = tree.findall("./dataset//physical")

    return resources


def generate_pid(resource: str) -> str:
    metadata = "https://pasta.lternet.edu/package/metadata/eml/"
    data = "https://pasta.lternet.edu/package/data/eml/"
    _ = resource.replace(metadata, "")
    _ = _.replace(data, "")
    _ = _.split("/")
    pid = f"{_[0]}.{_[1]}.{_[2]}"
    return pid


class EmbargoPool:
    def __init__(self):
        db_path = Config.PATH + Config.EMBARGO_DB
        Path(db_path).unlink(missing_ok=True)  # Start fresh each execution
        self._e_db = EmbargoDB(db_path)
        self._package_pool = PackagePool()

    def add_new_embargoed_resources(self) -> int:
        count = 0

        metadata_resources = pasta_data_package_manager_db.query(
            SQL_METADATA_RESOURCES
        )
        for metadata_resource in metadata_resources:
            try:
                pid = generate_pid(metadata_resource[0])
                self._e_db.insert(rid=metadata_resource[0], pid=pid)
                count += 1
            except IntegrityError as ex:
                msg = f"Ignoring resource '{metadata_resource}"
                logger.warning(msg)

        data_resources = pasta_data_package_manager_db.query(
            SQL_DATA_RESOURCES
        )
        for data_resource in data_resources:
            try:
                pid = generate_pid(data_resource[0])
                self._e_db.insert(rid=data_resource[0], pid=pid)
                count += 1
            except IntegrityError as ex:
                msg = f"Ignoring resource '{data_resource}"
                logger.warning(msg)

        return count

    def verify_metadata_embargo(self):
        pass
