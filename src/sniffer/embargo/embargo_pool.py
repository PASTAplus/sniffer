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
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import daiquiri
from lxml import etree
import requests
from sqlalchemy.exc import IntegrityError

from sniffer.config import Config
import sniffer.last_date as last_date
from sniffer.model.embargo_db import (
    EmbargoDB,
    Authenticated,
    Ephemeral,
    Explicit,
    Implicit,
)
from sniffer.model import pasta_data_package_manager_db
from sniffer.package.package_pool import PackagePool


logger = daiquiri.getLogger(__name__)

SQL_EXPLICIT = (
    "SELECT resource_id FROM datapackagemanager.access_matrix WHERE "
    "principal='public' AND access_type='deny' AND permission='read' "
    "AND (resource_id NOT LIKE '%%/ecotrends/%%' AND resource_id NOT LIKE "
    "'%%/lter-landsat/%%' AND resource_id NOT LIKE "
    "'%%/lter-landsat-ledaps/%%') AND resource_id LIKE '%%/%%data/eml/%%'"
)

SQL_AUTHENTICATED = (
    "SELECT resource_id FROM datapackagemanager.access_matrix WHERE "
    "principal='authenticated' AND access_type='allow' AND permission='read' "
    "AND (resource_id NOT LIKE '%%/ecotrends/%%' AND resource_id NOT LIKE "
    "'%%/lter-landsat/%%' AND resource_id NOT LIKE "
    "'%%/lter-landsat-ledaps/%%') AND resource_id LIKE '%%/%%data/eml/%%'"
)

SQL_RESOURCE_CREATE_DATE = (
    "SELECT datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_id='<RID>'"
)

SQL_ENTITY_LIST = (
    "SELECT datapackagemanager.resource_regsistry.resource_id FROM "
    "datapackagemanager.resource_registry WHERE package_id='<PID>' "
    "AND resource_type='data'"
)


def entity_parse_for_explicit_public_deny(eml: str) -> List:
    resources = list()
    tree = etree.fromstring(eml.encode("utf-8"))
    phys = tree.findall("./dataset//physical")
    for p in phys:
        distributions = p.findall("./distribution")
        for distribution in distributions:
            url = distribution.find("./online/url")
            if url is not None:
                denys = distribution.findall("./access/deny")
                for deny in denys:
                    principals = deny.findall("./principal")
                    for principal in principals:
                        if principal.text.strip() == "public":
                            permissions = deny.findall("./permission")
                            for permission in permissions:
                                if permission.text.strip() == "read":
                                    resources.append(url.text.strip())
    return resources


def entity_parse_for_implicit_public_deny(eml: str) -> List:
    resources = list()
    tree = etree.fromstring(eml.encode("utf-8"))
    phys = tree.findall("./dataset//physical")
    for p in phys:
        distributions = p.findall("./distribution")
        for distribution in distributions:
            url = distribution.find("./online/url")
            if url is not None:
                access = distribution.find("./access")
                if access is not None:
                    implicit = True
                    allows = access.findall("./allow")
                    for allow in allows:
                        principals = allow.findall("./principal")
                        for principal in principals:
                            if principal.text.strip() == "public":
                                permissions = allow.findall("./permission")
                                for permission in permissions:
                                    if permission.text.strip() == "read":
                                        implicit = False
                else:
                    implicit = False
                if implicit:
                    resources.append(url.text.strip())

    return resources


def generate_pid(resource: str) -> str:
    metadata = "https://pasta.lternet.edu/package/metadata/eml/"
    data = "https://pasta.lternet.edu/package/data/eml/"
    _ = resource.replace(metadata, "")
    _ = _.replace(data, "")
    _ = _.split("/")
    pid = f"{_[0]}.{_[1]}.{_[2]}"
    return pid


def pid_metadata(pid: str, authenticated=False) -> str:
    scope, identifier, revision = pid.split(".")
    url = f"{Config.PASTA_URL}metadata/eml/{scope}/{identifier}/{revision}"
    if authenticated:
        dn = Config.DN
        pw = Config.PASSWORD
        r = requests.get(url, auth=(dn, pw))
    else:
        r = requests.get(url)
    if r.status_code == requests.codes.ok:
        eml = r.text
    elif r.status_code == requests.codes.unauthorized:
        eml = None
    else:
        msg = f"Error accessing {pid} metadata - response code: {r.status_code}"
        raise ConnectionError(msg)
    return eml


class EmbargoPool:
    def __init__(self):
        db_path = Config.PATH + Config.EMBARGO_DB
        self._e_db = EmbargoDB(db_path)
        self._package_pool = PackagePool()

    def add_new_embargoed_resources(self) -> int:
        """
        Add embargoed PASTA+ resources to the Embargo Database

        :return:
            Count of embargoed resources
        """
        count = 0
        count += self.add_explicit_resources()
        count += self.add_ephemeral_resources()
        count += self.add_authenticated_resources()
        count += self.add_implicit_resources()
        return count

    def add_authenticated_resources(self) -> int:
        count = 0
        self._e_db.delete_all(table=Authenticated)

        authenticated_resources = pasta_data_package_manager_db.query(
            SQL_AUTHENTICATED
        )
        for authenticated_resource in authenticated_resources:
            try:
                pid = generate_pid(authenticated_resource[0])
                self._e_db.insert(
                    rid=authenticated_resource[0], pid=pid, table=Authenticated
                )
                count += 1
            except IntegrityError as ex:
                msg = f"Ignoring resource '{authenticated_resource[0]}"
                logger.warning(msg)

        logger.info(f"Found {count} authenticated embargoes")
        return count

    def add_ephemeral_resources(self) -> int:
        """
        Identify embargoed resources that are classified as ephemeral; these
        resources are identified by not having a corresponding metadata access
        control "deny-read" rule for the "public" user.

        :return:
            Count of identified resources
        """
        count = 0
        self._e_db.delete_all(table=Ephemeral)

        package_embargoes = self._e_db.get_all_metadata(table=Explicit)
        ignore_packages = list()
        for package_embargo in package_embargoes:
            ignore_packages.append(package_embargo.pid)

        verified_embargoes = self.verify_metadata_embargo()
        embargoes = self._e_db.get_all(table=Explicit)
        for embargo in embargoes:
            if (
                embargo.pid not in ignore_packages
                and embargo.rid not in verified_embargoes
            ):
                count += 1
                dates = pasta_data_package_manager_db.query(
                    SQL_RESOURCE_CREATE_DATE.replace("<RID>", embargo.rid)
                )

                msg = f"Found ephemeral embargo: {embargo.pid} - {embargo.rid}"
                logger.debug(msg)

                for date in dates:
                    self._e_db.insert(
                        rid=embargo.rid,
                        pid=embargo.pid,
                        table=Ephemeral,
                        date_ephemeral=date[0],
                    )

        logger.info(f"Found {count} ephemeral embargoes")
        return count

    def add_explicit_resources(self) -> int:
        count = 0
        self._e_db.delete_all(table=Explicit)

        explicit_resources = pasta_data_package_manager_db.query(SQL_EXPLICIT)
        for explicit_resource in explicit_resources:
            try:
                pid = generate_pid(explicit_resource[0])
                self._e_db.insert(
                    rid=explicit_resource[0], pid=pid, table=Explicit
                )
                count += 1
            except IntegrityError as ex:
                msg = f"Ignoring resource '{explicit_resource[0]}"
                logger.warning(msg)

        logger.info(f"Found {count} explicit embargoes")
        return count

    def add_implicit_resources(self) -> int:
        embargo_date_path = Config.PATH + Config.EMBARGO_DATE
        from_date = last_date.read(embargo_date_path)
        packages = self._package_pool.get_all_packages(from_date=from_date)
        count = 0
        for package in packages:
            pid = package.pid.strip()
            scope, identifier, revision = pid.split(".")
            if scope not in (
                "ecotrends",
                "lter-landsat",
                "lter-landsat-ledaps",
            ):
                eml = pid_metadata(pid)
                msg = f"Testing package for implicit entity embargo(s): {pid}"
                logger.info(msg)
                if eml is not None:
                    resources = entity_parse_for_implicit_public_deny(eml)
                    for resource in resources:
                        self._e_db.insert(rid=resource, pid=pid, table=Implicit)
                        msg = (
                                f"Adding implicit resource: {pid}, "
                                f"{resource}, {resource[1]}"
                        )
                        logger.warning(msg)
                        count += 1
                else:
                    logger.warn(f"Package is not public read: {pid}")
                    entities = pasta_data_package_manager_db.query(
                        SQL_ENTITY_LIST
                    )
                    for entity in entities:
                        self._e_db.insert(
                            rid=entity[0],
                            pid=pid,
                            table=Implicit
                        )

                last_date.write(embargo_date_path, package.date_created)

        return count

    def verify_metadata_embargo(self) -> list:
        """
        Verify that the embargoed resource is identified in the metadata.

        :return:
            List of verified resources
        """
        resources = list()
        embargoed_pids = self._e_db.get_distinct_pids(table=Explicit)
        for embargoed_pid in embargoed_pids:
            pid = embargoed_pid.pid
            eml = pid_metadata(pid, authenticated=True)
            if eml is None:
                msg = f"ACL for package {pid} does not permit public read"
                logger.warning(msg)
            else:
                resources += entity_parse_for_explicit_public_deny(eml)
        return resources

