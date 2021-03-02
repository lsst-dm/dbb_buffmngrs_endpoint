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
"""Component responsible for file discovery.
"""
import logging
import os
import time

from sqlalchemy.exc import SQLAlchemyError

from ..declaratives import file_creator
from ..search import search_methods
from ..utils import get_file_attributes


__all__ = ["Finder"]


logger = logging.getLogger(__name__)


class Finder:
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
        required = {"tablenames", "search", "session", "source", "storage"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        self.session = config["session"]

        # Create necessary object-relational mappings. We are doing it
        # dynamically as RDBMS tables to use are determined at runtime.
        required = {"file"}
        missing = required - set(config["tablenames"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.File = file_creator(config["tablenames"])

        # Check if provided source and storage location exists.
        self.source = os.path.abspath(config["source"])
        self.storage = os.path.abspath(config["storage"])
        for path in (self.source, self.storage):
            if not os.path.isdir(path):
                msg = f"directory '{path}' not found"
                logger.error(msg)
                raise ValueError(msg)

        # Set standard and alternative actions based on provided configuration.
        self.dispatch = dict(std=config["standard"], alt=config["alternative"])

        # Configure method responsible for file discovery.
        search = config["search"]
        method_name = search["method"]
        try:
            self.search = search_methods[method_name]
        except KeyError:
            msg = f"unknown search method: '{method_name}'"
            logger.error(msg)
            raise ValueError(msg)
        self.search_opts = dict(exclude_list=search.get("exclude_list", None),
                                isodate=search.get("date", None),
                                past_days=search.get("past_days", 1),
                                future_days=search.get("future_days", 1),
                                delay=search.get("delay", 60))

        # If we are monitoring rsync transfers, files are already in the
        # storage area. Otherwise they are still in the buffer.
        self.location = self.storage if "parse" in method_name else self.source

        # Initialize various optional settings.
        self.pause = config.get("pause", 1)

    def run(self):
        """Start the framework.
        """
        while True:
            for relpath in self.search(self.source, **self.search_opts):
                abspath = os.path.abspath(os.path.join(self.location, relpath))
                logger.debug("%s: starting processing", abspath)

                action_type = "std"

                logger.debug("%s: checking if already tracked", abspath)
                try:
                    checksum, status = get_file_attributes(abspath)
                except FileNotFoundError:
                    logger.error("%s: no such file", abspath)
                    logger.debug("%s: terminating processing", abspath)
                    continue
                filename = os.path.basename(relpath)
                try:
                    records = self.session.query(self.File).\
                        filter(self.File.checksum == checksum,
                               self.File.filename == filename).all()
                except SQLAlchemyError as ex:
                    logger.error("%s: cannot check if tracked: %s",
                                 abspath, ex)
                    logger.debug("%s: terminating processing", abspath)
                    self.session.rollback()
                    continue
                if records:
                    dups = ", ".join(str(rec.id) for rec in records)
                    logger.error("%s: file already tracked (see row(s): %s)",
                                 abspath, dups)
                    action_type = "alt"

                action = self.dispatch[action_type]
                logger.debug("%s: executing action: %s", abspath, action)
                try:
                    action.execute(abspath)
                except RuntimeError as ex:
                    logger.error("%s: action failed: %s", abspath, ex)
                    logger.debug("%s: terminating processing", abspath)
                    continue

                if action_type == "alt":
                    logger.debug("%s: terminating processing", abspath)
                    continue

                logger.debug("updating database entries")
                dirname, basename = os.path.split(action.path)
                entry = self.File(
                    relpath=os.path.relpath(dirname, start=self.storage),
                    filename=basename,
                    checksum=checksum,
                    size_bytes=status.st_size,
                )
                self.session.add(entry)
                try:
                    self.session.commit()
                except Exception as ex:
                    logger.error("%s: creating a database entry failed; %s",
                                 abspath, ex)
                    logger.debug("%s: rolling the changes back", abspath)
                    try:
                        action.undo()
                    except RuntimeError as ex:
                        logger.error("%s: cannot undo action: %s", abspath, ex)
                    self.session.rollback()
                    logger.debug("%s: terminating processing", abspath)
                else:
                    logger.debug("%s: processing completed", abspath)

            logger.debug("no new files, next check in %i sec.", self.pause)
            time.sleep(self.pause)
