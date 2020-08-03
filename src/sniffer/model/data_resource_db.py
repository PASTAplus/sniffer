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


class DataResource(Base):
    __tablename__ = "data_resources"

    id = Column(Integer(), primary_key=True)
    pid = Column(String(), nullable=False)
    owner = Column(String(), nullable=False)
    entity_id = Column(String(), nullable=False)
    entity_name = Column(String(), nullable=False)
    md5 = Column(String(), nullable=False)
    sha1 = Column(String(), nullable=False)
    size = Column(Integer(), nullable=False)
    date_created = Column(DateTime(), nullable=False)
    date_deactivated = Column(DateTime(), nullable=True)


class DataResourcePool:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def insert(
            self,
            pid: str,
            owner: str,
            entity_id: str,
            entity_name: str,
            md5: str,
            sha1: str,
            size: int,
            date_created: datetime,
            date_deactivated: datetime = None
    ) -> int:
        pk = None
        d = DataResource(
            pid=pid,
            owner=owner,
            entity_id=entity_id,
            entity_name=entity_name,
            md5=md5,
            sha1=sha1,
            size=size,
            date_created=date_created,
            date_deactivated=date_deactivated
        )
        try:
            self.session.add(d)
            self.session.commit()
            pk = d.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk
