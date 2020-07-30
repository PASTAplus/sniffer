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
from lxml import etree
import requests

from sniffer.config import Config
import sniffer.last_date as last_date
from sniffer.model.offline_db import OfflineDB
from sniffer.package.package_pool import PackagePool


logger = daiquiri.getLogger(__name__)


def offline_parse(eml: str):
    resources = list()
    tree = etree.fromstring(eml.encode("utf-8"))
    phys = tree.findall("./dataset//physical")
    for phy in phys:
        distributions = phy.findall(".//distribution")
        for distribution in distributions:
            offline_elem = distribution.find(".//offline")
            if offline_elem is not None:
                medium_name = offline_elem.find(".//mediumName").text
                object_name = phy.find(".//objectName").text
                resources.append([object_name, medium_name])
    return resources


class OfflinePool:
    def __init__(self):
        db_path = Config.PATH + Config.OFFLINE_DB
        self._o_db = OfflineDB(db_path)
        self._package_pool = PackagePool()

    def add_new_offline_resources(self) -> int:
        dn = Config.DN
        pw = Config.PASSWORD
        r = requests.get(Config.PASTA_URL, auth=(dn, pw))
        r.raise_for_status()
        token = r.cookies["auth-token"]
        cookies = {"auth-token": token}

        offline_date_path = Config.PATH + Config.OFFLINE_DATE
        from_date = last_date.read(offline_date_path)
        packages = self._package_pool.get_all_packages(from_date=from_date)
        for package in packages:
            pid = package.pid.strip()
            scope, identifier, revision = pid.split(".")
            if scope not in (
                "ecotrends",
                "lter-landsat",
                "lter-landsat-ledaps",
            ):
                metadata_url = (
                    Config.METADATA_URL.replace("<SCOPE>", scope)
                    .replace("<IDENTIFIER>", identifier)
                    .replace("<REVISION>", revision)
                )
                r = requests.get(metadata_url, cookies=cookies)
                if r.status_code == requests.codes.ok:
                    logger.info(f"Parsing {pid}")
                    resources = offline_parse(r.text)
                    for resource in resources:
                        self._o_db.insert_offline_resource(
                            pid, resource[0], resource[1]
                        )
                        logger.info(
                            f"Adding offline resource: {pid}, {resource[0]}, {resource[1]}"
                        )
                else:
                    logger.warn(
                        f"Ignoring {pid}: status code is {r.status_code}"
                    )
                last_date.write(offline_date_path, package.date_created)

        count = 0
        return count
