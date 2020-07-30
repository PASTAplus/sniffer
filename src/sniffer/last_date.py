#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: last_date

:Synopsis:

:Author:
    servilla

:Created:
    7/29/20
"""
from datetime import datetime
from pathlib import Path

from sniffer.config import Config


def read(file_path: str) -> datetime:
    if Path(file_path).exists():
        with open(file_path, "r") as f:
            iso = datetime.fromisoformat(f.readline().strip())
    else:
        iso = Config.START_DATE
    return iso


def write(file_path: str, iso: datetime):
    with open(file_path, "w") as f:
        f.write(str(iso.isoformat()))
