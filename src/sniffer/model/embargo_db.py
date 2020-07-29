#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: embargo_db

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


class PublicEmbargo(Base):
    __tablename__ = "public_embargos"

    rid = Column(String(), primary_key=True)
    pid = Column(String(), nullable=False)
    acl = Column(String(), nullable=False)

