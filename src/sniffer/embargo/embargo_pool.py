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
from typing import List, Tuple, Set

import daiquiri
from lxml import etree
import requests
from sqlalchemy.exc import IntegrityError

from sniffer.config import Config
import sniffer.last_date as last_date
from sniffer.model.embargo_db import EmbargoDB, Ephemeral
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
    "SELECT datapackagemanager.resource_registry.resource_id FROM "
    "datapackagemanager.resource_registry WHERE package_id='<PID>' "
    "AND resource_type='data'"
)

SQL_NEWEST_PIDS = (
    "SELECT CONCAT(scope, '.', identifier, '.', MAX(revision)) FROM "
    "datapackagemanager.resource_registry WHERE scope<>'ecotrends' AND "
    "scope<>'lter-landsat' AND scope<>'lter-landsat-ledaps' "
    "GROUP BY scope, identifier"
)


def newest_pids() -> Set:
    newest = set()
    pids = pasta_data_package_manager_db.query(SQL_NEWEST_PIDS)
    for pid in pids:
        newest.add(pid[0])
    return newest


def pid_from_resource(resource: str) -> str:
    metadata = "https://pasta.lternet.edu/package/metadata/eml/"
    data = "https://pasta.lternet.edu/package/data/eml/"
    _ = resource.replace(metadata, "")
    _ = _.replace(data, "")
    _ = _.split("/")
    pid = f"{_[0]}.{_[1]}.{_[2]}"
    return pid


def pasta_metadata(pid: str) -> str:
    scope, identifier, revision = pid.split(".")
    url = f"{Config.PASTA_URL}metadata/eml/{scope}/{identifier}/{revision}"
    dn = Config.DN
    pw = Config.PASSWORD
    r = requests.get(url, auth=(dn, pw))
    if r.status_code == requests.codes.ok:
        eml = r.text
    elif r.status_code == requests.codes.unauthorized:
        eml = None
    else:
        msg = (
            f"Error accessing {pid} metadata - response code: {r.status_code}"
        )
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
                msg = f"Testing package for embargo(s): {pid}"
                logger.info(msg)
                p = Package(pid)
                count += p.count_embargoed_resources
                for resource in p.embargoed_resources:
                    self._e_db.insert(
                        rid=resource[0],
                        pid=resource[1],
                        type=resource[2],
                        auth=resource[3],
                    )
                last_date.write(embargo_date_path, package.date_created)

        self._e_db.delete_all_newest()
        n_pids = newest_pids()
        embargoed_pids = self._e_db.get_distinct_pids()
        for embargoed_pid in embargoed_pids:
            if embargoed_pid[0] in n_pids:
                embargoed_resources = self._e_db.get_by_pid(embargoed_pid[0])
                for embargoed_resource in embargoed_resources:
                    self._e_db.insert_newest(
                        rid=embargoed_resource.rid,
                        pid=embargoed_resource.pid,
                        type=embargoed_resource.type,
                        auth=embargoed_resource.auth
                    )

        self._add_ephemeral_resources()

        return count

    def _add_ephemeral_resources(self) -> int:
        """
        Identify embargoed resources that are classified as ephemeral; these
        resources are identified by not having a corresponding metadata access
        control "deny-read" rule for the "public" user.

        :return:
            Count of identified resources
        """
        count = 0
        self._e_db.delete_all_ephemeral()

        embargoed_resources = pasta_data_package_manager_db.query(SQL_EXPLICIT)
        for embargoed_resource in embargoed_resources:
            resource = self._e_db.get_by_rid(embargoed_resource[0])
            if resource is None:
                sql = SQL_RESOURCE_CREATE_DATE.replace(
                    "<RID>",
                    embargoed_resource[0]
                )
                dt = pasta_data_package_manager_db.query(sql)[0][0]
                pid = pid_from_resource(embargoed_resource[0])
                self._e_db.insert_ephemeral(
                    rid=embargoed_resource[0],
                    pid=pid,
                    dt=dt
                )
                count += 1
                msg = f"Adding ephemeral embargo for: {embargoed_resource[0]}"
                logger.info(msg)

        return count


class Package:
    def __init__(self, pid: str):
        self._embargoed_resources = list()
        self._pid = pid
        scope, identifier, revision = self._pid.split(".")
        metadata_resource = (
            Config.METADATA_URL.replace("<SCOPE>", scope)
            .replace("<IDENTIFIER>", identifier)
            .replace("<REVISION>", revision)
        )
        metadata = pasta_metadata(self._pid)
        if metadata is None:
            msg = f"Failed to access package metadata: {pid}"
            logger.warning(msg)
            self._embargoed_resources.append(
                (
                    metadata_resource,
                    self._pid,
                    Config.EXPLICIT,
                    False
                )
            )
            sql = SQL_ENTITY_LIST.replace("<PID>", self._pid)
            resources = pasta_data_package_manager_db.query(sql)
            for resource in resources:
                self._embargoed_resources.append(
                    (
                        resource[0],
                        self._pid,
                        Config.EXPLICIT,
                        False
                    )
                )
        else:
            self._eml = etree.fromstring(metadata.encode("utf-8"))
            self._package_embargo_type = None
            self._package_embargo_type, allows_auth = self._package_embargo()
            if self._package_embargo_type is not None:
                self._embargoed_resources.append(
                    (
                        metadata_resource,
                        self._pid,
                        self._package_embargo_type,
                        allows_auth
                    )
                )
            self._embargoed_resources += self._entity_embargoes()

    @property
    def embargoed_resources(self) -> List:
        return self._embargoed_resources

    @property
    def count_embargoed_resources(self) -> int:
        return len(self._embargoed_resources)

    def _allows_authenticated(self, access) -> bool:
        allow_authenticated = False
        if access is not None:
            allows = access.findall("./allow")
            for allow in allows:
                principals = allow.findall("./principal")
                for principal in principals:
                    if principal.text.strip() == "authenticated":
                        permissions = allow.findall("./permission")
                        for permission in permissions:
                            if permission.text.strip() in (
                                "read",
                                "write",
                                "all",
                                "changePermission",
                            ):
                                allow_authenticated = True
                                if permission.text.strip() != "read":
                                    pid = self._eml.get("packageId")
                                    msg = (
                                        "Authenticated permission too high for "
                                        f"package: {pid}"
                                    )
                                    logger.warning(msg)
        return allow_authenticated

    def _entity_embargoes(self) -> List:
        entity_resources = list()
        phys = self._eml.findall("./dataset//physical")
        for p in phys:
            distributions = p.findall("./distribution")
            for distribution in distributions:
                url = distribution.find("./online/url")
                if url is not None:
                    embargo_type = None
                    access = distribution.find("./access")
                    if self._is_explicitly_denied(access):
                        embargo_type = Config.EXPLICIT
                    elif self._is_implicitly_denied(access):
                        embargo_type = Config.IMPLICIT
                    if embargo_type is not None:
                        allows_auth = self._allows_authenticated(access)
                        entity_resources.append(
                            (
                                url.text.strip(),
                                self._pid,
                                embargo_type,
                                allows_auth,
                            )
                        )
        return entity_resources

    def _is_explicitly_denied(self, access) -> bool:
        if self._package_embargo_type == Config.EXPLICIT:
            is_denied = True
        else:
            is_denied = False
            if access is not None:
                denies = access.findall("./deny")
                for deny in denies:
                    principals = deny.findall("./principal")
                    for principal in principals:
                        if principal.text.strip() == "public":
                            permissions = deny.findall("./permission")
                            for permission in permissions:
                                if permission.text.strip() == "read":
                                    is_denied = True
        return is_denied

    def _is_implicitly_denied(self, access) -> bool:
        if self._package_embargo_type == Config.IMPLICIT:
            is_denied = True
        else:
            is_denied = False
            if access is not None:
                is_denied = True
                allows = access.findall("./allow")
                for allow in allows:
                    principals = allow.findall("./principal")
                    for principal in principals:
                        if principal.text.strip() == "public":
                            permissions = allow.findall("./permission")
                            for permission in permissions:
                                if permission.text.strip() in (
                                    "read",
                                    "write",
                                    "all",
                                    "changePermission",
                                ):
                                    is_denied = False
                                    if permission.text.strip() != "read":
                                        pid = self._eml.get("packageId")
                                        msg = (
                                            "Authenticated permission too high "
                                            f"for package: {pid}"
                                        )
                                        logger.warning(msg)
        return is_denied

    def _package_embargo(self) -> Tuple:
        embargo_type = None
        allows_auth = None
        access = self._eml.find("./access")
        if self._is_explicitly_denied(access):
            embargo_type = Config.EXPLICIT
        elif self._is_implicitly_denied(access):
            embargo_type = Config.IMPLICIT
        if embargo_type is not None:
            allows_auth = self._allows_authenticated(access)
        return embargo_type, allows_auth
