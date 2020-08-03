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
from datetime import datetime

import daiquiri
from sqlalchemy.exc import IntegrityError

from sniffer.config import Config
from sniffer.model.package_db import PackageDB
from sniffer.model import pasta_data_package_manager_db

logger = daiquiri.getLogger(__name__)

SQL_PACKAGES = ("SELECT datapackagemanager.resource_registry.package_id, "
                "datapackagemanager.resource_registry.date_created, "
                "datapackagemanager.resource_registry.date_deactivated, "
                "datapackagemanager.resource_registry.doi "
                "FROM datapackagemanager.resource_registry WHERE "
                "resource_type='dataPackage' AND date_created > '<DATE>' "
                "ORDER BY date_created ASC")


class PackagePool:
    def __init__(self):
        db_path = Config.PATH + Config.PACKAGE_DB
        self._p_db = PackageDB(db_path)

    def add_new_packages(self, limit: int = None) -> int:
        d = self._p_db.get_most_recent_create_date()
        if d is None:
            iso = Config.START_DATE.isoformat()
        else:
            iso = d.isoformat()

        if limit is not None:
            sql = SQL_PACKAGES.replace("<DATE>", iso) + " LIMIT " + str(limit)
        else:
            sql = SQL_PACKAGES.replace("<DATE>", iso)

        packages = pasta_data_package_manager_db.query(sql)
        count = 0
        for package in packages:
            count += 1
            try:
                self._p_db.insert(package[0], package[1], package[2],
                                  package[3])
                logger.debug(f"Inserting package: {package[0]}")
            except IntegrityError as e:
                msg = f"Ignoring package '{package[0]}"
                logger.warning(msg)

        return count

    def get_all_packages(self, from_date: datetime = None):
        return self._p_db.get_all(from_date=from_date)

    @property
    def count(self):
        return self._p_db.get_count()
