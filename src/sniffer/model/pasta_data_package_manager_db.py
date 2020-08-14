#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: pasta_resource_registry

:Synopsis:

:Author:
    servilla

:Created:
    5/12/20
"""
import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import NoResultFound

from sniffer.config import Config


logger = daiquiri.getLogger(__name__)


def query(sql: str):
    rs = None
    db = (
        Config.DB_DRIVER
        + "://"
        + Config.DB_USER
        + ":"
        + Config.DB_PW
        + "@"
        + Config.DB_HOST
        + "/"
        + Config.DB_DB
    )
    engine = create_engine(db)
    try:
        with engine.connect() as connection:
            rs = connection.execute(sql).fetchall()
    except NoResultFound as e:
        logger.warning(e)
        rs = list()
    except Exception as e:
        logger.error(e)
        raise e
    return rs
