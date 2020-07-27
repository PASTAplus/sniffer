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

from sqlalchemy.orm.query import Query
from sqlalchemy.exc import IntegrityError

from sniffer import pasta_resource_registry
from sniffer.config import Config


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


def add_new_packages(pp, limit: int = None) -> int:
    d = pp.get_most_recent_create_date()
    if d is None:
        iso = Config.START_DATE.isoformat()
    else:
        iso = d.isoformat()

    if limit is not None:
        sql = SQL_PACKAGES.replace("<DATE>", iso) + " LIMIT " + str(limit)
    else:
        sql = SQL_PACKAGES.replace("<DATE>", iso)

    packages = pasta_resource_registry.query(sql)
    c = 0
    for package in packages:
        c += 1
        try:
            pp.insert_package(package[0], package[1], package[2], package[3])
            logger.debug(f"Inserting package: {package[0]}")
        except IntegrityError as e:
            msg = f"Ignoring package '{package[0]}"
            logger.warning(msg)

    return c
