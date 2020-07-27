#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: lock

:Synopsis:
    Create a simple file-based mutex lock.
 
:Author:
    servilla

:Created:
    3/31/17
"""
import logging
import string
import random
import os

import daiquiri


logger = daiquiri.getLogger(__name__)


class Lock(object):
    def __init__(self, file_name=None):

        if file_name is None:
            random_str = lambda n: "".join(
                [random.choice(string.ascii_letters) for i in range(n)]
            )
            self._file_name = random_str(10) + ".lock"
        else:
            self._file_name = file_name

    def acquire(self):
        fp = open(self._file_name, "wb")
        fp.close()

    def release(self):
        os.remove(self._file_name)

    @property
    def locked(self):
        return os.path.isfile(self._file_name)

    @property
    def lock_file(self):
        return self._file_name


def main():
    return 0


if __name__ == "__main__":
    main()
