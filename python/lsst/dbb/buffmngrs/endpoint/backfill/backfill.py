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
from ..utils import get_checksum


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

        search = config["search"]
        self.search_opts = dict(blacklist=search.get("blacklist", None))

    def run(self):
        """Start the backfill process.
        """
        acc = dict(failure=0, success=0)
        sources = [glob(os.path.join(self.storage, s)) for s in self.sources]
        for source in chain(*sources):
            logger.debug(f"{source}: setting as file source")
            if not os.path.exists(source):
                logger.warning(f"{source}: no such file or directory")
                acc["failure"] += 1
                continue

            paths = scan(source, **self.search_opts) if os.path.isdir(source) \
                else [source]
            for path in paths:
                abspath = os.path.join(source, path)
                relpath = os.path.relpath(abspath, start=self.storage)
                dirname, filename = os.path.split(relpath)
                logger.debug(f"{relpath}: starting processing")

                logger.debug("checking if already tracked")
                try:
                    record = self.session.query(self.File). \
                        filter(self.File.relpath == dirname,
                               self.File.filename == filename).first()
                except SQLAlchemyError as ex:
                    logger.error(f"{relpath}: cannot check if tracked: {ex}")
                    logger.debug(f"{relpath}: terminating processing")
                    acc["failure"] += 1
                    continue
                if record is not None:
                    logger.warning(f"file already tracked (id: {record.id})")
                    logger.debug(f"{relpath}: terminating processing")
                    acc["failure"] += 1
                    continue

                logger.debug(f"calculating checksum")
                try:
                    checksum = get_checksum(abspath)
                except FileNotFoundError:
                    logger.error(f"{relpath}: no such file")
                    logger.debug(f"{relpath}: terminating processing")
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
                )
                self.session.add(file)

                # Flush the changes to get the id for that record (does NOT
                # end the transaction).
                try:
                    self.session.flush()
                except SQLAlchemyError as ex:
                    self.session.rollback()
                    logger.error(f"{relpath}: cannot create file entry: {ex}")
                    logger.debug(f"{relpath}: terminating processing")
                    acc["failure"] += 1
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
                    logger.error(f"{relpath}: cannot create database entries: "
                                 f"{ex}")
                    acc["failure"] += 1
                else:
                    logger.debug(f"{relpath}: processing completed")
                    acc["success"] += 1
        total = sum(x for x in acc.values())
        if total == 0:
            logger.warning("no backfill attempts were made")
        if acc["failure"] != 0:
            logger.warning("some backfill attempts failed")
        else:
            logger.info("all found files were backfilled successfully")
