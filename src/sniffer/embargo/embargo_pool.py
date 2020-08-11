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
from sniffer.model.embargo_db import EmbargoDB
from sniffer.model import pasta_data_package_manager_db
from sniffer.package.package_pool import PackagePool


logger = daiquiri.getLogger(__name__)

SQL_METADATA_ACLS = (
    "SELECT datapackagemanager.access_matrix.resource_id "
    "FROM datapackagemanager.access_matrix WHERE "
    "resource_id LIKE '%%/metadata/eml/%%' AND principal='public' "
    " AND access_type='deny' AND permission='read'"
)

SQL_RESOURCE_ACLS = (
    "SELECT datapackagemanager.access_matrix.resource_id, "
    "datapackagemanager.access_matrix.principal, "
    "datapackagemanager.access_matrix.access_type, "
    "datapackagemanager.access_matrix.permission "
    "FROM datapackagemanager.access_matrix WHERE "
    "resource_id='<RESOURCE_ID>'"
)


SQL_DATA_RESOURCES = (
    "SELECT datapackagemanager.resource_registry.resource_id "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_id LIKE '%%/data/eml/<SCOPE>/<IDENTIFIER>/<REVISION>/%%'"
)


SQL_DATA_ACLS = (
    "SELECT datapackagemanager.access_matrix.resource_id "
    "FROM datapackagemanager.access_matrix WHERE "
    "resource_id LIKE '%%/data/eml/%%' AND principal='public' "
    " AND access_type='deny' AND permission='read'"
)

SQL_RESOURCE_CREATE_DATE = (
    "SELECT datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_id='<RID>'"
)


def entity_embargo_parse_for_explicit_public_deny(eml: str):
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


def generate_pid(resource: str) -> str:
    metadata = "https://pasta.lternet.edu/package/metadata/eml/"
    data = "https://pasta.lternet.edu/package/data/eml/"
    _ = resource.replace(metadata, "")
    _ = _.replace(data, "")
    _ = _.split("/")
    pid = f"{_[0]}.{_[1]}.{_[2]}"
    return pid


def embargo_state(resource_id: str) -> Tuple:
    sql = SQL_RESOURCE_ACLS.replace("<RESOURCE_ID>", resource_id)
    acls = pasta_data_package_manager_db.query(sql)
    explicit = False
    implicit = True
    authenticated = False
    for acl in acls:
        if acl[1] == "public" and acl[2] == "deny" and acl[3] == "read":
            explicit = True
            implicit = False
        elif acl[1] == "public" and acl[2] == "allow" and acl[3] == "read":
            implicit = False
        elif (
            acl[1] == "authenticated"
            and acl[2] == "allow"
            and acl[3] == "read"
        ):
            authenticated = True
    if explicit or implicit:
        state = (explicit, authenticated)
    else:
        state = None
    return state


def pid_metadata(pid: str) -> str:
    scope, identifier, revision = pid.split(".")
    url = f"{Config.PASTA_URL}/metadata/eml/{scope}/{identifier}/{revision}"
    r = requests.get(url)
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
        dn = Config.DN
        pw = Config.PASSWORD
        r = requests.get(Config.PASTA_URL, auth=(dn, pw))
        r.raise_for_status()
        token = r.cookies["auth-token"]
        cookies = {"auth-token": token}

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
                metadata_resource = (
                    Config.METADATA_URL.replace("<SCOPE>", scope)
                    .replace("<IDENTIFIER>", identifier)
                    .replace("<REVISION>", revision)
                )
                state = embargo_state(metadata_resource)
                if state is not None:
                    self._e_db.insert(
                        rid=metadata_resource,
                        pid=pid,
                        is_explicit=state[0],
                        allows_authenticated=state[1],
                    )
                    count += 1
                    msg = f"Adding embargoed resource: {metadata_resource}"
                    logger.info(msg)

                sql = (
                    SQL_DATA_RESOURCES.replace("<SCOPE>", scope)
                    .replace("<IDENTIFIER>", identifier)
                    .replace("<REVISION>", revision)
                )
                data_resources = pasta_data_package_manager_db.query(sql)
                for data_resource in data_resources:
                    state = embargo_state(data_resource[0])
                    if state is not None:
                        self._e_db.insert(
                            rid=data_resource[0],
                            pid=pid,
                            is_explicit=state[0],
                            allows_authenticated=state[1],
                        )
                        msg = f"Adding embargoed resource: {data_resource}"
                        logger.info(msg)
                        count += 1

                last_date.write(embargo_date_path, package.date_created)

        # self.identify_ephemeral_embargoes()

        return count

    def verify_metadata_embargo(self) -> list:
        """
        Verify that the embargoed resource is identified in the metadata.

        :return:
            List of verified resources
        """
        resources = list()
        embargoed_pids = self._e_db.get_distinct_pids()
        for pid in embargoed_pids:
            eml = pid_metadata(pid[0])
            if eml is None:
                msg = f"ACL for package {pid[0]} does not permit public read"
                logger.warning(msg)
            else:
                resources += entity_embargo_parse_for_explicit_public_deny(eml)
        return resources

    def identify_ephemeral_embargoes(self) -> int:
        """
        Identify embargoed resources that are classified as ephemeral; these
        resources are identified by not having a corresponding metadata access
        control "deny-read" rule for the "public" user.

        :return:
            Count of identified resources
        """
        verified_embargoes = self.verify_metadata_embargo()

        package_embargoes = self._e_db.get_all_package_embargoes()
        ignore_packages = list()
        for package_embargo in package_embargoes:
            ignore_packages.append(package_embargo.pid)

        count = 0
        embargoes = self._e_db.get_all()
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
                logger.info(msg)

                for date in dates:
                    self._e_db.update_ephemeral_date(embargo.rid, date[0])

        return count
