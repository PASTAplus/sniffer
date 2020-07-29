#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: package_pool

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""
import daiquiri

from sqlalchemy.exc import IntegrityError

from sniffer.config import Config
from sniffer.model.package_db import PackageDB
from sniffer.package import pasta_resource_registry


logger = daiquiri.getLogger(__name__)

SQL_PACKAGES = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created, "
    "datapackagemanager.resource_registry.date_deactivated, "
    "datapackagemanager.resource_registry.doi "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND date_created > '<DATE>' "
    "ORDER BY date_created ASC"
)


def add_new_packages(limit: int = None) -> int:
    p_db_path = Config.DB_PATH + Config.PACKAGE_DB
    p_db = PackageDB(p_db_path)

    d = p_db.get_most_recent_create_date()
    if d is None:
        iso = Config.START_DATE.isoformat()
    else:
        iso = d.isoformat()

    if limit is not None:
        sql = SQL_PACKAGES.replace("<DATE>", iso) + " LIMIT " + str(limit)
    else:
        sql = SQL_PACKAGES.replace("<DATE>", iso)

    packages = pasta_resource_registry.query(sql)
    count = 0
    for package in packages:
        count += 1
        try:
            p_db.insert_package(package[0], package[1], package[2], package[3])
            logger.debug(f"Inserting package: {package[0]}")
        except IntegrityError as e:
            msg = f"Ignoring package '{package[0]}"
            logger.warning(msg)

    return count
