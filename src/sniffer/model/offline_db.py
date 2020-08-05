#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: offline_db

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


class OfflineResource(Base):
    __tablename__ = "offline_resources"

    id = Column(Integer(), primary_key=True)
    pid = Column(String(), nullable=False)
    object_name = Column(String(), nullable=False)
    medium = Column(String(), nullable=False)


class OfflineDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_all(self):
        try:
            o = (
                self.session.query(OfflineResource)
                .order_by(OfflineResource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return o

    def get_by_id(self, id: int) -> Query:
        o = None
        try:
            o = (
                self.session.query(OfflineResource)
                .filter(OfflineResource.id == id)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return o

    def get_by_pid(self, pid: str) -> Query:
        try:
            o = (
                self.session.query(OfflineResource)
                .filter(OfflineResource.pid == pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return o

    def insert(self, pid: str, object_name: str, medium: str) -> int:
        pk = None
        o = OfflineResource(pid=pid, object_name=object_name, medium=medium)
        try:
            self.session.add(o)
            self.session.commit()
            pk = o.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk
