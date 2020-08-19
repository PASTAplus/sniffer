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
from dateutil import tz
from typing import Type

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
ABQ_TZ = tz.gettz("America/Denver")


class Resource(Base):
    __tablename__ = "resources"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    type = Column(Integer(), nullable=False)
    auth = Column(Boolean(), nullable=False)


class Ephemeral(Base):
    __tablename__ = "ephemeral"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    date_ephemeral = Column(DateTime(), nullable=True, default=None)
    days_ephemeral = Column(Integer(), nullable=True, default=None)


class Newest(Base):
    __tablename__ = "newest"
    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    type = Column(Integer(), nullable=False)
    auth = Column(Boolean(), nullable=False)


class EmbargoDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def delete_all(self):
        try:
            e = self.session.query(Resource).delete()
            self.session.commit()
        except NoResultFound as ex:
            logger.error(ex)

    def get_all(self) -> Query:
        try:
            e = (
                self.session.query(Resource)
                .order_by(Resource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data(self):
        try:
            e = (
                self.session.query(Resource)
                .filter(
                    Resource.rid.like("%/data/eml/%"),
                )
                .order_by(Resource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_metadata(self):
        try:
             e = (
                self.session.query(Resource)
                .filter(Resource.rid.like("%/metadata/eml/%"))
                .order_by(Resource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_count(self) -> int:
        c = 0
        try:
            c = self.session.query(Resource).count()
        except NoResultFound as ex:
            logger.error(ex)
        return c

    def get_distinct_pids(self):
        try:
            e = self.session.query(Resource.pid).distinct().all()
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_id(self, id: int) -> Query:
        e = None
        try:
            e = (
                self.session.query(Resource)
                .filter(Resource.id == id)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_pid(self, pid: str) -> Query:
        try:
            e = (
                self.session.query(Resource)
                .filter(Resource.pid == pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_rid(self, rid: str) -> Query:
        e = None
        try:
            e = (
                self.session.query(Resource)
                .filter(Resource.rid == rid)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_type(self, type: int) -> Query:
        try:
            e = (
                self.session.query(Resource)
                .filter(Resource.type == type)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def insert(self, rid: str, pid: str, type: int, auth: bool) -> int:
        e = Resource(rid=rid, pid=pid, type=type, auth=auth)
        try:
            self.session.add(e)
            self.session.commit()
            pk = e.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk

    def delete_all_ephemeral(self):
        try:
            e = self.session.query(Ephemeral).delete()
            self.session.commit()
        except NoResultFound as ex:
            logger.error(ex)

    def get_all_ephemeral(self):
        try:
            e = (
                self.session.query(Ephemeral)
                .order_by(Resource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def insert_ephemeral(self, rid: str, pid: str, dt: datetime) -> int:
        dt_then = dt.astimezone(tz=ABQ_TZ)
        dt_now = datetime.now(tz=ABQ_TZ)
        days = (dt_now - dt_then).days
        e = Ephemeral(rid=rid, pid=pid, date_ephemeral=dt, days_ephemeral=days)
        try:
            self.session.add(e)
            self.session.commit()
            pk = e.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk

    def delete_all_newest(self):
        try:
            e = self.session.query(Newest).delete()
            self.session.commit()
        except NoResultFound as ex:
            logger.error(ex)

    def get_all_newest(self):
        try:
            e = (
                self.session.query(Newest)
                .order_by(Newest.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_newest_data(self):
        try:
            e = (
                self.session.query(Newest)
                .filter(
                    Newest.rid.like("%/data/eml/%"),
                )
                .order_by(Newest.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_newest_metadata(self):
        try:
             e = (
                self.session.query(Newest)
                .filter(Newest.rid.like("%/metadata/eml/%"))
                .order_by(Newest.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_newest_count(self) -> int:
        c = 0
        try:
            c = self.session.query(Newest).count()
        except NoResultFound as ex:
            logger.error(ex)
        return c

    def insert_newest(self, rid: str, pid: str, type: int, auth: bool) -> int:
        e = Newest(rid=rid, pid=pid, type=type, auth=auth)
        try:
            self.session.add(e)
            self.session.commit()
            pk = e.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk
