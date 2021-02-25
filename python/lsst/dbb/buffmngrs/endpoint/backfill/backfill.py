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
"""Component responsible for backfilling historical files.
"""
import datetime
import logging
import os
from glob import glob
from itertools import chain
from sqlalchemy.exc import SQLAlchemyError
from ..declaratives import event_creator, file_creator
from ..search import scan
from ..status import Status
from ..utils import get_file_attributes


__all__ = ["Backfill"]


logger = logging.getLogger(__name__)


class Backfill:
    """A backfill tool allowing one to deal with historical files.

    It creates database entries for historical files, i.e., files which were
    present in the storage area *before* the DBB endpoint manager was
    deployed.

    The status of backfilled files is set to ``BACKFILL``.

    At the moment, it does *not* verify if any historical file it creates
    entries for was actually successfully ingested to a Butler repository.

    Parameters
    ----------
    config : `dict`
        Backfill configuration.

    Raises
    ------
    ValueError
        If a required setting is missing.
    """

    def __init__(self, config):
        # Check if configuration is valid, i.e., all required settings are
        # provided; complain if not.
        required = {"session", "sources", "storage", "tablenames"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        self.session = config["session"]

        # Create necessary object-relational mappings. We are doing it
        # dynamically as RDBMS tables to use are determined at runtime.
        required = {"event", "file"}
        missing = required - set(config["tablenames"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.Event = event_creator(config["tablenames"])
        self.File = file_creator(config["tablenames"])

        self.storage = os.path.abspath(config["storage"])
        if not os.path.isdir(self.storage):
            msg = f"directory '{self.storage}' not found"
            logger.error(msg)
            raise ValueError(msg)

        self.sources = config["sources"]
        for src in [path for path in self.sources if path.startswith("/")]:
            logger.warning("%s is absolute, should be relative", src)
            if os.path.commonpath([self.storage, src]) != self.storage:
                msg = f"{src} is not located in the storage area"
                logger.error(msg)
                raise ValueError(msg)

        search = config["search"]
        self.search_opts = dict(exclude_list=search.get("exclude_list", None))

    def run(self):
        """Start the backfill process.
        """
        counts = dict(failure=0, notfound=0, success=0, tracked=0)

        # Expand source specifications into actual sources. A source is an
        # absolute path to either a file or a directory.  Sources
        # corresponding to a given specification remain grouped together.
        sources = [glob(os.path.join(self.storage, s)) for s in self.sources]
        for source in chain(*sources):
            logger.debug("%s: setting as file source", source)
            if not os.path.exists(source):
                logger.warning("%s: no such file or directory", source)
                counts["notfound"] += 1
                continue

            # If a source is a directory construct an generator which will
            # traverse it to find files matching the search criteria.  All
            # return file paths will be relative to that directory.
            paths = scan(source, **self.search_opts) if os.path.isdir(source) \
                else [source]
            for path in paths:
                abspath = os.path.join(source, path)
                relpath = os.path.relpath(abspath, start=self.storage)
                dirname, filename = os.path.split(relpath)
                logger.debug("%s: starting processing", relpath)

                logger.debug("checking if already tracked")
                try:
                    record = self.session.query(self.File). \
                        filter(self.File.relpath == dirname,
                               self.File.filename == filename).first()
                except SQLAlchemyError as ex:
                    logger.error("%s: cannot check if tracked: %s", relpath, ex)
                    logger.debug("%s: terminating processing", relpath)
                    counts["failure"] += 1
                    continue
                if record is not None:
                    logger.info("file already tracked (id: %s)", record.id)
                    logger.debug("%s: terminating processing", relpath)
                    counts["tracked"] += 1
                    continue

                logger.debug("getting file attributes")
                try:
                    checksum, status = get_file_attributes(abspath)
                except FileNotFoundError:
                    logger.error("%s: no such file", relpath)
                    logger.debug("%s: terminating processing", relpath)
                    counts["notfound"] += 1
                    continue

                # Always make BOTH inserts in a single transaction!
                # Otherwise new entry in file table may be picked up by
                # an Ingester daemon running in the background.
                logger.debug("updating database entries")

                # Add file record (starts the transaction).
                file = self.File(
                    relpath=dirname,
                    filename=filename,
                    checksum=checksum,
                    size_bytes=status.st_size
                )
                self.session.add(file)

                # Flush the changes to get the id for that record (does NOT
                # end the transaction).
                try:
                    self.session.flush()
                except SQLAlchemyError as ex:
                    self.session.rollback()
                    logger.error("%s: cannot create file entry: %s",
                                 relpath, ex)
                    logger.debug("%s: terminating processing", relpath)
                    counts["failure"] += 1
                    continue

                # Add the corresponding event record.
                event = self.Event(
                    status=Status.BACKFILL.value,
                    start_time=datetime.datetime.now(),
                    files_id=file.id,
                )
                self.session.add(event)

                # Finally, commit the changes or roll the changes back if
                # any errors were encountered (ends the transaction).
                try:
                    self.session.commit()
                except Exception as ex:
                    self.session.rollback()
                    logger.error("%s: cannot create database entries: "
                                 "%s", relpath, ex)
                    counts["failure"] += 1
                else:
                    logger.debug("%s: processing completed", relpath)
                    counts["success"] += 1

        # Use this list to collect any errors, if any, for future reference.
        errors = []
        fails, notfound, successes, tracked = counts.values()
        total = sum(val for key, val in counts.items() if key != "notfound")
        if total != 0:
            logger.info("files meeting the search criteria: %i", total)
            logger.info("failed backfill attempts: %i", fails)
            logger.info("files already tracked: %i", tracked)
            logger.info("files backfilled successfully: %i", successes)
            if fails == 0:
                logger.info("all files matching search criteria were "
                            "backfilled successfully")
            else:
                msg = f"{fails} out of {total} backfill attempts failed"
                logger.warning(msg)
                errors.append(msg)
        else:
            logger.warning("no files meeting search criteria found")
        if notfound != 0:
            msg = f"{notfound} sources became inaccessible during backfilling"
            logger.warning(msg)
            errors.append(msg)
        if errors:
            raise RuntimeError("; ".join(errors))
