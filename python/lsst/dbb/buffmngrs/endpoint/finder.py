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
import datetime
import hashlib
import logging
import os
import re
import time
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from .actions import Null
from .declaratives import File


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
        self.std_action = config.get("standard", noop)
        self.alt_action = config.get("alternative", noop)

        self.blacklist = config.get("blacklist", [])
        self.pause = config.get("pause", 1)

    def run(self):
        while True:
            for path in self.search(self.buffer):

                # Check if it is not a duplicate of an already existing file.
                checksum = get_checksum(path)
                try:
                    query = self.session.query(File).\
                        filter(File.checksum == checksum)
                except SQLAlchemyError as ex:
                    logger.error(f"Cannot check for duplicates: {ex}.")
                else:
                    is_duplicate = False
                    try:
                        row = query.one()
                    except MultipleResultsFound:
                        # Uh, oh, internal records indicate that there are
                        # multiple duplicates of the file in the storage area.
                        # It is should-not-happen kind of thing.
                        dups = ", ".join(str(r.id) for r in query)
                        logger.warning(f"Possible duplicates in rows: {dups}.")
                        is_duplicate = True
                    except NoResultFound:
                        # According to internal records, the file is genuinely
                        # new.  That's good, actually!
                        pass
                    else:
                        # There is already a file with that checksum in the
                        # storage area. Not good, but not tragic either.
                        # Due to various glitches duplicates may pop up in
                        # the buffer, but there are no redundant copies in
                        # the storage area.
                        logger.warning(f"File '{path}' already in the storage "
                                       f"area, see '{rec.url}', row {rec.id}.")
                        is_duplicate = True
                    if is_duplicate:
                        self.alt_action.execute(path)
                        logger.info(f"Duplicate '{path}' removed.")
                        continue

                # Execute pre-defined action for the file.
                try:
                    self.std_action.execute(path)
                except RuntimeError as ex:
                    logger.error(f"Action failed: {ex}.")
                    continue

                # Insert data about file into the database.
                ts = datetime.datetime.now()
                entry = File(url=self.std_action.path,
                             checksum=checksum,
                             added_at=ts.isoformat(timespec="milliseconds"))
                try:
                    self.session.add(entry)
                except SQLAlchemyError as ex:
                    logger.error(f"Adding {self.std_action.path} failed: {ex}.")
                    self.std_action.undo()
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
    """Generate the file names in a directory tree.

    Generates the file paths in a given directory tree excluding those files
    that were blacklisted.

    Parameters
    ----------
    directory : `str`
        The root of the directory tree which need to be searched.
    blacklist : `list` of `str`, optional
        List of regular expressions file names should be match against. If a
        filename matches any of the patterns in the list, file will be ignored.
        By default, no file is ignored.

    Returns
    -------
    generator object
        An iterator over file paths.
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
