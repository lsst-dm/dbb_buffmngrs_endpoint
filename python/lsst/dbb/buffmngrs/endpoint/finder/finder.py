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
import sys
import time
from sqlalchemy.exc import SQLAlchemyError
from .actions import Null
from .declaratives import File


__all__ = ["Finder"]


logger = logging.getLogger(__name__)


class Finder(object):

    def __init__(self, config):
        required = {"buffer", "storage", "session"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.session = config["session"]

        method = config.get("search_method", "scan")
        try:
            self.search = getattr(sys.modules[__name__], method)
        except AttributeError as ex:
            msg = f"unknown search method: '{method}'"
            logger.error(msg)
            raise ValueError(msg)

        self.buffer = os.path.abspath(config["buffer"])
        self.storage = os.path.abspath(config["storage"])
        for path in (self.buffer, self.storage):
            if not os.path.isdir(path):
                msg = f"directory '{path}' not found"
                logger.error(msg)
                raise ValueError()

        noop = Null(dict())
        self.dispatch = {
            "std": config.get("standard", noop),
            "alt": config.get("alternative", noop)
        }

        self.blacklist = config.get("blacklist", [])
        self.pause = config.get("pause", 1)

    def run(self):
        while True:
            for path in self.search(self.buffer):
                logger.debug(f"starting processing a new file: '{path}'")
                action_type = "std"

                logger.debug(f"checking if not already in storage area")
                checksum = get_checksum(path)
                try:
                    records = self.session.query(File).\
                        filter(File.checksum == checksum).all()
                except SQLAlchemyError as ex:
                    logger.error(f"cannot check for duplicates: {ex}")
                else:
                    if len(records) != 0:
                        dups = ", ".join(str(rec.id) for rec in records)
                        logger.error(f"file '{path}' already in the storage "
                                     f"area (see row(s): {dups}), "
                                     f"removing from buffer")
                        action_type = "alt"

                action = self.dispatch[action_type]
                logger.debug(f"executing action: {action}")
                try:
                    action.execute(path)
                except RuntimeError as ex:
                    logger.error(f"action failed: {ex}")
                    logger.debug(f"terminating processing of '{path}'")
                    continue

                if action_type == "alt":
                    continue

                logger.debug(f"updating database entries: {action}")
                ts = datetime.datetime.now()
                entry = File(url=action.path,
                             checksum=checksum,
                             added_at=ts.isoformat(timespec="milliseconds"))
                try:
                    self.session.add(entry)
                except SQLAlchemyError as ex:
                    logger.error(f"adding {action.path} failed: {ex}")
                    action.undo()
                else:
                    try:
                        self.session.commit()
                    except SQLAlchemyError as ex:
                        logger.error(f"cannot commit changes: {ex}")
                logger.debug(f"processing of '{path}' completed")

            logger.debug(f"no new files, next check in {self.pause} sec.")
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
    """Generate the file names in a directory tree.

    Generate the file paths in a given directory tree excluding those files
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
        manifests = [os.path.join(dirpath, f)
                     for f in filenames if re.match("^rsync*log$", f)]
        for manifest in manifests:
            with open(manifest) as f:
                for line in f:
                    if not re.match("<f+++++++++", line):
                        continue
                    op, chng, fn, *rest = line.strip().split()
                    if any(re.match(patt, fn) for patt in blacklist):
                        continue
                    yield fn
