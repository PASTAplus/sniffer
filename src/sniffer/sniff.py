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
import sniffer.offline.offline_pool as offline_pool
import sniffer.package.package_pool as package_pool
from sniffer.model.package_db import PackageDB


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/sniffer.log"
daiquiri.setup(
    level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",)
)
logger = daiquiri.getLogger(__name__)

help_limit = "Query limit to PASTA+ resource registry."
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("-l", "--limit", default=100, help=help_limit)
def main(limit: int):
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error("Lock file {} exists, exiting...".format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.info("Lock file {} acquired".format(lock.lock_file))

    c = package_pool.add_new_packages(limit=limit)
    logger.info(f"Packages acquired: {c}")
    while c > 0:
        c = package_pool.add_new_packages(limit=limit)
        logger.info(f"Packages acquired: {c}")

    lock.release()
    logger.info("Lock file {} released".format(lock.lock_file))

    return 0


if __name__ == "__main__":
    main()
