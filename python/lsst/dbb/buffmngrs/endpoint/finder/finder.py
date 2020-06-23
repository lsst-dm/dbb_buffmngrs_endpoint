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
from ..declaratives import event_creator, file_creator


__all__ = ["Finder"]


logger = logging.getLogger(__name__)


class Finder(object):
    """Framework responsible for file discovery.

    Finder constantly monitors a designated source location for new files.
    Currently it provides two methods of finding new files:

    **scan**
        Finds file names in a given directory tree by walking it top-down.
        Appropriate to monitor an actual buffer -- a directory where
        files are being written to.

    **parse**
        Finds file names by parsing rsync log files in a given directory.
        Appropriate to monitor file transfers done by rsync.

    Parameters
    ----------
    config : `dict`
        Finder configuration.

    Raises
    ------
    ValueError
        If a required setting is missing.
    """

    def __init__(self, config):
        # Check if configuration is valid, i.e., all required settings are
        # provided; complain if not.
        required = {"orms", "search", "session", "source", "storage"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        self.session = config["session"]

        # Create necessary object-relational mappings. We are doing it
        # dynamically as RDBMS tables to use are determined at runtime.
        required = {"event", "file"}
        missing = required - set(config["orms"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.Event = event_creator(config["orms"])
        self.File = file_creator(config["orms"])

        # Check if provided source and storage location exists.
        self.source = os.path.abspath(config["source"])
        self.storage = os.path.abspath(config["storage"])
        for path in (self.source, self.storage):
            if not os.path.isdir(path):
                msg = f"directory '{path}' not found"
                logger.error(msg)
                raise ValueError()

        # Set standard and alternative actions based on provided configuration.
        self.dispatch = dict(std=config["standard"], alt=config["alternative"])

        # Configure method responsible for file discovery.
        search = config["search"]
        method = search["method"]
        try:
            self.search = getattr(sys.modules[__name__], method)
        except AttributeError:
            msg = f"unknown search method: '{method}'"
            logger.error(msg)
            raise ValueError(msg)
        self.search_opts = dict(blacklist=search.get("blacklist", None),
                                date=search.get("date", None),
                                timespan=search.get("timespan", 1))

        # If we are monitoring rsync transfers, files are already in the
        # storage area. Otherwise they are still in the buffer.
        self.location = self.storage if method is "parse" else self.storage

        # Initialize various optional settings.
        self.pause = config.get("pause", 1)

    def run(self):
        """Start the framework.
        """
        while True:
            for relpath in self.search(self.source, **self.search_opts):
                abspath = os.path.abspath(os.path.join(self.location, relpath))
                logger.debug(f"starting processing a new file: '{abspath}'")

                action_type = "std"

                logger.debug(f"checking if not already in storage area")
                try:
                    checksum = get_checksum(abspath)
                except FileNotFoundError:
                    logger.error(f"{abspath}: no such file")
                    logger.debug(f"terminating processing of '{abspath}'")
                    continue
                try:
                    records = self.session.query(self.File).\
                        filter(self.File.checksum == checksum).all()
                except SQLAlchemyError as ex:
                    logger.error(f"cannot check for duplicates: {ex}")
                    logger.debug(f"terminating processing of '{abspath}'")
                    continue
                if len(records) != 0:
                    dups = ", ".join(str(rec.id) for rec in records)
                    logger.error(f"file '{abspath}' already in the "
                                 f"storage area '{self.storage}: "
                                 f"(see row(s): {dups}), removing")
                    action_type = "alt"

                action = self.dispatch[action_type]
                logger.debug(f"executing action: {action}")
                try:
                    action.execute(abspath)
                except RuntimeError as ex:
                    logger.error(f"action failed: {ex}")
                    logger.debug(f"terminating processing of '{abspath}'")
                    continue

                if action_type == "alt":
                    continue

                logger.debug(f"updating database entries")
                dirname, basename = os.path.split(action.path)
                entry = self.File(
                    relpath=os.path.relpath(dirname, start=self.storage),
                    filename=basename,
                    checksum=checksum,
                )
                try:
                    self.session.add(entry)
                    self.session.commit()
                except Exception as ex:
                    logger.error(f"creating a database entry for {relpath} "
                                 f"failed: {ex}; rolling back the changes")
                    try:
                        action.undo()
                    except RuntimeError as ex:
                        logger.error(f"cannot undo action: {ex}")
                    logger.debug(f"terminating processing of '{abspath}'")
                else:
                    logger.debug(f"processing of '{relpath}' completed")

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


def scan(directory, blacklist=None, **kwargs):
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
        An iterator over file paths relative to the ``directory``.
    """
    if blacklist is None:
        blacklist = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for fn in filenames:
            path = os.path.relpath(os.path.join(dirpath, fn), start=directory)
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
