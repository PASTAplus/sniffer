#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: paclage_db

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


class Package(Base):
    __tablename__ = "packages"

    pid = Column(String(), primary_key=True)
    date_created = Column(DateTime(), nullable=False)
    date_deactivated = Column(DateTime(), nullable=True)
    doi = Column(String(), nullable=True)


class PackageDB:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_all(self, from_date: datetime = None) -> Query:
        try:
            if from_date is None:
                p = (
                    self.session.query(Package)
                    .order_by(Package.date_created.asc())
                    .all())
            else:
                p = (
                    self.session.query(Package)
                    .filter(Package.date_created > from_date)
                    .order_by(Package.date_created.asc())
                    .all()
                )
        except NoResultFound as ex:
            logger.error(ex)
        return p

    def get_count(self) -> int:
        c = 0
        try:
            c = self.session.query(Package).count()
        except NoResultFound as ex:
            logger.error(ex)
        return c

    def get_most_recent_create_date(self) -> datetime:
        d = None
        try:
            p = (
                self.session.query(Package)
                .order_by(Package.date_created.desc())
                .first()
            )
            if p is not None:
                d = p.date_created
        except NoResultFound as ex:
            logger.error(ex)
        return d

    def get(self, pid: str) -> Query:
        p = None
        try:
            p = self.session.query(Package).filter(Package.pid == pid).one()
        except NoResultFound as ex:
            logger.error(ex)
        return p

    def insert(
        self,
        pid: str,
        date_created: datetime,
        date_deactivate: datetime,
        doi: str,
    ):
        pk = None
        p = Package(
            pid=pid,
            date_created=date_created,
            date_deactivated=date_deactivate,
            doi=doi,
        )
        try:
            self.session.add(p)
            self.session.commit()
            pk = p.pid
        except IntegrityError as ex:
            logger.error(ex)
            self.session.rollback()
            raise ex

        return pk
