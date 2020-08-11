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


class EmbargoedResource(Base):
    __tablename__ = "embargoed_resources"

    id = Column(Integer(), primary_key=True)
    rid = Column(String(), unique=True)
    pid = Column(String(), nullable=False)
    is_explicit = Column(Boolean(), nullable=False, default=False)
    allows_authenticated = Column(Boolean(), nullable=False, default=False)
    date_ephemeral = Column(DateTime(), nullable=True, default=None)
    days_ephemeral = Column(Integer(), nullable=True, default=None)


class EmbargoDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_all(self) -> Query:
        try:
            e = (
                self.session.query(EmbargoedResource)
                .order_by(EmbargoedResource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data_embargoes(self, ephemeral: bool = False):
        try:
            if ephemeral:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(EmbargoedResource.rid.like("%/data/eml/%"))
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
            else:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.date_ephemeral == None,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data_embargoes_allows_authenticated(self, ephemeral: bool = False):
        try:
            if ephemeral:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.allows_authenticated,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
            else:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.allows_authenticated,
                        EmbargoedResource.date_ephemeral == None,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data_embargoes_explicit(self, ephemeral: bool = False):
        try:
            if ephemeral:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.is_explicit,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
            else:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.is_explicit,
                        EmbargoedResource.date_ephemeral == None,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_data_embargoes_implicit(self, ephemeral: bool = False):
        try:
            if ephemeral:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.is_explicit == False,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
            else:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(
                        EmbargoedResource.rid.like("%/data/eml/%"),
                        EmbargoedResource.is_explicit == False,
                        EmbargoedResource.date_ephemeral == None,
                    )
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_ephemeral_embargoes(self):
        try:
            e = (
                self.session.query(EmbargoedResource)
                .filter(EmbargoedResource.date_ephemeral != None)
                .order_by(EmbargoedResource.pid)
                .all()
            )
        except NoResultFound as ex:
            logger.error(ex)
        return e

    def get_all_package_embargoes(self, ephemeral: bool = False):
        try:
            if ephemeral:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(EmbargoedResource.rid.like("%/metadata/eml/%"))
                    .order_by(EmbargoedResource.pid)
                    .all()
                )
            else:
                e = (
                    self.session.query(EmbargoedResource)
                    .filter(EmbargoedResource.rid.like("%/metadata/eml/%"))
                    .filter(EmbargoedResource.date_ephemeral == None)
                    .order_by(EmbargoedResource.pid)
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

    def insert(
        self,
        rid: str,
        pid: str,
        is_explicit: bool = False,
        allows_authenticated: bool = False,
        date_ephemeral: datetime = None,
        days_ephemeral: int = None,
    ) -> int:
        e = EmbargoedResource(
            rid=rid,
            pid=pid,
            is_explicit=is_explicit,
            allows_authenticated=allows_authenticated,
            date_ephemeral=date_ephemeral,
            days_ephemeral=days_ephemeral,
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

    def update_ephemeral_date(
        self, rid: str, date_ephemeral: datetime
    ) -> Query:
        dt_then = date_ephemeral.astimezone(tz=ABQ_TZ)
        dt_now = datetime.now(tz=ABQ_TZ)
        days = (dt_now - dt_then).days
        e = self.get_by_rid(rid)
        if e is not None:
            e.date_ephemeral = date_ephemeral
            e.days_ephemeral = days
            self.session.commit()
        return e
