#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: offline_pool

:Synopsis:

:Author:
    servilla

:Created:
    7/28/20
"""
from datetime import datetime
from pathlib import Path

import daiquiri

from sniffer.config import Config
from sniffer.model.offline_db import OfflineDB
from sniffer.model.package_db import PackageDB


logger = daiquiri.getLogger(__name__)


def read_last_date(file_path: str) -> datetime:
    if Path(file_path).exists():
        with open(file_path, "r") as f:
            iso = datetime.fromisoformat(f.readline().strip())
    else:
        iso = Config.START_DATE
    return iso


def write_last_date(file_path: str, iso: datetime):
    with open(file_path, "w") as f:
        f.write(str(iso.isoformat()))


def add_new_offline_resources():
    o_db_path = Config.DB_PATH + Config.OFFLINE_DB
    o_db = OfflineDB(o_db_path)
    p_db_path = Config.DB_PATH + Config.PACKAGE_DB
    p_db = PackageDB(o_db_path)

    last_date = read_last_date(Config.PATH + "offline_date.txt")
    packages = p_db.get_all_from_date(last_date)
    for package in packages:
        pid = package.pid
