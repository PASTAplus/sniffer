#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: sniffer_db

:Synopsis:
    The resource database model.

:Author:
    servilla

:Created:
    7/26/20
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


class Packages(Base):
    __tablename__ = "packages"

    pid = Column(String(), primary_key=True)
    date_created = Column(DateTime(), nullable=False)
    doi = Column(String(), nullable=True)


class OfflineResource(Base):
    __tablename__ = "offline_resource"

    oid = Column(String(), primary_key=True)
    pid = Column(String(), nullable=False)
    type = Column(String(), nullable=False)


class PublicEmbargoPackage(Base):
    __tablename__ = "public_embargo_package"

    pid = Column(String(), primary_key=True)
    acl = Column(String(), nullable=False)


class PublicEmbargoResource(Base):
    __tablename__ = "public_embargo_resource"

    rid = Column(String(), primary_key=True)
    pid = Column(String(), nullable=False)
    acl = Column(String(), nullable=False)


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


class PackagePool:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_all_packages(self) -> Query:
        try:
            p = self.session.query(Packages).all()
        except NoResultFound as e:
            logger.error(e)
        return p

    def get_count(self) -> int:
        c = 0
        try:
            c = self.session.query(Packages).count()
        except NoResultFound as e:
            logger.error(e)
        return c

    def get_most_recent_create_date(self) -> datetime:
        d = None
        try:
            p = (
                self.session.query(Packages)
                .order_by(Packages.date_created.desc())
                .first()
            )
            if p is not None:
                d = p.date_created
        except NoResultFound as e:
            logger.error(e)
        return d

    def get_package(self, pid: str) -> Query:
        p = None
        try:
            p = self.session.query(Packages).filter(Packages.pid == pid).one()
        except NoResultFound as e:
            logger.error(e)
        return p

    def insert_package(self, pid: str, date_created: datetime, doi: str):  #
        p = Packages(pid=pid, date_created=date_created, doi=doi)
        try:
            self.session.add(p)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e
