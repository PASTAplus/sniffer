#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: sniff.py

:Synopsis:

:Author:
    servilla

:Created:
    7/24/20
"""
import logging
import os

import click
import daiquiri

from sniffer.config import Config
from sniffer.lock import Lock
from sniffer.offline.offline_pool import OfflinePool
from sniffer.package.package_pool import PackagePool


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/sniffer.log"
daiquiri.setup(
    level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",)
)
logger = daiquiri.getLogger(__name__)

help_limit = "Query limit to PASTA+ resource registry."
help_offline = "Sniff for offline data resources."
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("-l", "--limit", default=100, help=help_limit)
@click.option("-o", "--offline", default=False, is_flag=True, help=help_offline)
def main(limit: int, offline: bool):
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error("Lock file {} exists, exiting...".format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.info("Lock file {} acquired".format(lock.lock_file))

    package_pool = PackagePool()
    c = package_pool.add_new_packages(limit=limit)
    logger.info(f"Packages acquired: {c}, Pool count: {package_pool.count}")
    while c > 0:
        c = package_pool.add_new_packages(limit=limit)
        logger.info(f"Packages acquired: {c}, Pool count: {package_pool.count}")

    if offline:
        offline_pool = OfflinePool()
        offline_pool.add_new_offline_resources()

    lock.release()
    logger.info("Lock file {} released".format(lock.lock_file))

    return 0


if __name__ == "__main__":
    main()
