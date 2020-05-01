# This file is part of dbb_buffmngrs_endpoint.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import hashlib
import logging
import os
import re
import time
from sqlalchemy.exc import SQLAlchemyError
from .actions import Null
from .declaratives import Message


__all__ = ["Finder"]


logger = logging.getLogger(__name__)


class Finder(object):

    def __init__(self, config):
        required = {"buffer", "storage", "session"}
        missing = required - set(config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.critical(msg)
            raise ValueError(msg)

        self.session = config["session"]

        self.search = scan

        self.buffer = os.path.abspath(config["buffer"])
        self.storage = os.path.abspath(config["storage"])
        for path in (self.buffer, self.storage):
            if not os.path.isdir(path):
                raise ValueError(f"directory '{path}' not found.")

        noop = Null(dict())
        self.action = config.get("action", noop)
        self.blacklist = config.get("blacklist", [])
        self.pause = config.get("pause", 1)

    def run(self):
        while True:
            for path in self.search(self.buffer):

                # Execute pre-defined action for the file.
                try:
                    self.action.execute(path)
                except RuntimeError as ex:
                    logger.error(f"Action failed: {ex}.")
                    continue
                else:
                    loc = self.action.path

                # Update database entry for that file
                entry = Message(url=loc)
                try:
                    self.session.add(entry)
                except SQLAlchemyError as ex:
                    logger.error(f"Sending message failed: {ex}.")
                    self.action.undo()
                else:
                    try:
                        self.session.commit()
                    except SQLAlchemyError as ex:
                        logger.error(f"{ex}")
            time.sleep(self.pause)


def get_checksum(path, method='blake2', block_size=4096):
    """Calculate checksum for a file using BLAKE2 cryptographic hash function.

    Parameters
    ----------
    path : `str`
        Path to the file.
    method : `str`
        An algorithm to use for calculating file's hash. Supported algorithms
        include:
        * _blake2_: BLAKE2 cryptographic hash,
        * _md5_: traditional MD5 algorithm,
        * _sha1_: SHA-1 cryptographic hash.
        By default or if unsupported method is provided, BLAKE2 algorithm wil
        be used.
    block_size : `int`, optional
        Size of the block

    Returns
    -------
    `str`
        File's hash calculated using a given method.
    """
    methods = {
        'blake2': hashlib.blake2b,
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
    }
    hasher = methods.get(method, hashlib.blake2b)()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def scan(directory, blacklist=None):
    """

    Parameters
    ----------
    directory : `str`
    blacklist : `list` of `str`

    Returns
    -------
    filename :
    """
    if blacklist is None:
        blacklist = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for fn in filenames:
            path = os.path.abspath(os.path.join(dirpath, fn))
            if any(re.search(patt, path) for patt in blacklist):
                continue
            yield path


def parse(directory, blacklist=None):
    pass
