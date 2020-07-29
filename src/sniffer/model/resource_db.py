#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: resource_db

:Synopsis:

:Author:
    servilla

:Created:
    7/28/20
"""
from datetime import datetime

import daiquiri
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    desc,
    asc,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import not_

from sniffer.config import Config

logger = daiquiri.getLogger(__name__)
Base = declarative_base()


class Resources(Base):
    __tablename__ = "resources"

    id = Column(String(), primary_key=True)
    pid = Column(String(), nullable=False)
    type = Column(String(), nullable=False)
    entity_id = Column(String(), nullable=True)
    md5 = Column(String(), nullable=False)
    sha1 = Column(String(), nullable=False)
    checked_count = Column(Integer(), nullable=False, default=0)
    checked_last_date = Column(DateTime(), nullable=True)
    checked_last_status = Column(Boolean(), nullable=True)
    validated = Column(Boolean(), nullable=False, default=False)


