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
    update,
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


class EmbargoedResource(Base):
    __tablename__ = "embargoed_resources"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    date_ephemeral = Column(DateTime(), nullable=True, default=None)


class EmbargoDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_all(self) -> Query:
        try:
            e = self.session.query(EmbargoedResource).all()
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_package_level_embargoes(self) -> Query:
        try:
            e = (
                self.session.query(EmbargoedResource)
                .filter(EmbargoedResource.rid.like("%/metadata/eml/%"))
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_count(self) -> int:
        c = 0
        try:
            c = self.session.query(EmbargoedResource).count()
        except NoResultFound as ex:
            logger.error(ex)
        return c

    def get_distinct_pids(self):
        try:
            e = self.session.query(EmbargoedResource.pid).distinct().all()
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_id(self, id: int) -> Query:
        e = None
        try:
            e = (
                self.session.query(EmbargoedResource)
                .filter(EmbargoedResource.id == id)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_pid(self, pid: str) -> Query:
        try:
            e = (
                self.session.query(EmbargoedResource)
                .filter(EmbargoedResource.pid == pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_rid(self, rid: str) -> Query:
        try:
            e = (
                self.session.query(EmbargoedResource)
                .filter(EmbargoedResource.rid == rid)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def insert(self, rid: str, pid: str, date_ephemeral: datetime = None,) -> int:
        pk = None
        e = EmbargoedResource(rid=rid, pid=pid, date_ephemeral=date_ephemeral)
        try:
            self.session.add(e)
            self.session.commit()
            pk = e.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk

    def update_ephemeral_date(self, rid: str, date_ephemeral: datetime) -> Query:
        e = self.get_by_rid(rid)
        if e is not None:
            e.date_ephemeral = date_ephemeral
            self.session.commit()
        return e
