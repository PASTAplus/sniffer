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


class Authenticated(Base):
    __tablename__ = "authenticated"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)


class Ephemeral(Base):
    __tablename__ = "ephemeral"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    date_ephemeral = Column(DateTime(), nullable=True, default=None)
    days_ephemeral = Column(Integer(), nullable=True, default=None)


class Explicit(Base):
    __tablename__ = "explicit"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)


class Implicit(Base):
    __tablename__ = "implicit"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)


class EmbargoDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def delete_all(self, table):
        try:
            e = self.session.query(table).delete()
            self.session.commit()
        except NoResultFound as ex:
            logger.error(ex)

    def get_all(self, table) -> Query:
        try:
            e = (
                self.session.query(table)
                .order_by(table.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data(self, table):
        try:
            e = (
                self.session.query(table)
                .filter(
                    table.rid.like("%/data/eml/%"),
                )
                .order_by(table.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_metadata(self, table):
        try:
             e = (
                self.session.query(table)
                .filter(table.rid.like("%/metadata/eml/%"))
                .order_by(table.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_count(self, table) -> int:
        c = 0
        try:
            c = self.session.query(table).count()
        except NoResultFound as ex:
            logger.error(ex)
        return c

    def get_distinct_pids(self, table):
        try:
            e = self.session.query(table.pid).distinct().all()
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_id(self, id: int, table) -> Query:
        e = None
        try:
            e = (
                self.session.query(table)
                .filter(table.id == id)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_pid(self, pid: str, table) -> Query:
        try:
            e = (
                self.session.query(table)
                .filter(table.pid == pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_by_rid(self, rid: str, table) -> Query:
        try:
            e = (
                self.session.query(table)
                .filter(table.rid == rid)
                .one()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def insert(
        self,
        rid: str,
        pid: str,
        table,
        date_ephemeral: datetime = None,
    ) -> int:
        if table is Ephemeral:
            dt_then = date_ephemeral.astimezone(tz=ABQ_TZ)
            dt_now = datetime.now(tz=ABQ_TZ)
            days_ephemeral = (dt_now - dt_then).days
            e = table(
                rid=rid,
                pid=pid,
                date_ephemeral=date_ephemeral,
                days_ephemeral=days_ephemeral,
            )
        else:
            e = table(
                rid=rid,
                pid=pid,
            )
        try:
            self.session.add(e)
            self.session.commit()
            pk = e.id
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex
        return pk