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
from ..declaratives import file_creator


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

        required = {"file"}
        missing = required - set(config["orms"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.File = file_creator(config["orms"])

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
                    records = self.session.query(self.File).\
                        filter(self.File.checksum == checksum).all()
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
                entry = self.File(
                    url=action.path,
                    checksum=checksum,
                    added_at=ts.isoformat(timespec="milliseconds")
                )
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


def parse(directory, blacklist=None, isodate=None, timespan=1):
    """Generate the file names based on the content of the rsync logs.

    This is a specialized search method for finding files which where
    transferred to the storage area by ``rsync``. It identifies these
    files by parsing log files created by it.

    It assumes that:

    1. The logs are stored in a centralized location.
    2. Logs from different days are kept in different subdirectories in that
       location.
    3. The name of each subdirectory represents the date when
       files where transferred and that date is expressed as ``YYYYMMDD``.
    4. Log files contain file paths relative to the root of the storage area.

    After the function completed parsing a given logfile, it creates an
    empty file ``<logfile>.done`` which act as a guard preventing it from
    parsing it again.

    Parameters
    ----------
    directory : `str`
        The directory where the all log files reside.
    blacklist : `list` of `str`, optional
        List of regular expressions file names should be match against. If a
        filename matches any of the patterns in the list, file will be ignored.
        By default, no file is ignored.
    isodate : `str`
        A date in ISO format corresponding the directory which the function
        should monitor for new logs. If None (default), it will be set to
        the current date.
    timespan : `int`
        The number of previous days to add to the list of monitored
        directories. Defaults to 1 which means that the function will
        monitor log files in a subdirectory corresponding to whatever day
        the``isodate`` was set to and the day before (if it exists).

    Returns
    -------
    generator object
        An iterator over file paths extracted from the log files.
    """
    if blacklist is None:
        blacklist = []
    end = datetime.date.today()
    if isodate is not None:
        end = datetime.date.fromisoformat(isodate)
    dates = [end - datetime.timedelta(days=n) for n in range(timespan+1)]
    for date in dates:
        top = os.path.join(directory, date.isoformat().replace("-", ""))
        if not os.path.exists(top):
            continue
        for dirpath, dirnames, filenames in os.walk(top):
            manifests = [os.path.join(dirpath, fn) for fn in filenames
                         if re.match(r"^rsync.*log$", fn)]
            for manifest in manifests:
                if os.path.exists(manifest + ".done"):
                    continue
                with open(manifest) as f:
                    for line in f:
                        if "<f+++++++++" not in line:
                            continue
                        op, chng, loc, *rest = line.strip().split()
                        if any(re.match(patt, loc) for patt in blacklist):
                            continue
                        yield loc
                os.mknod(manifest + ".done")
