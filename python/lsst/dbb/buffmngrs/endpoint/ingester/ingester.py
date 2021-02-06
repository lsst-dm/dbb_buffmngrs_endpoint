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
import logging
import os
import queue
import re
import threading
import time
import traceback
from dataclasses import dataclass
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import exists, func
from ..declaratives import event_creator, file_creator
from ..status import Status


__all__ = ["Ingester"]


logger = logging.getLogger(__name__)


@dataclass
class Request:
    """Message representing a request to make an ingest attempt for a file.

    At minimum, it should contain all pieces of information required to
    make an ingest attempt for a given file (e.g. file location).
    """

    filepath: str = None
    """Absolute path to the file.
    """

    id: int = None
    """Id of the database record associated with the file.
    """


@dataclass
class Reply:
    """Message representing results of an ingest attempt.

    In general, it should contain any pieces of information that may be
    needed to make a database entry describing the ingest attempt.
    """

    id: int = None
    """Id of the request the result corresponds to.
    """

    version: str = None
    """Version of the LSST ingest software.
    """

    timestamp: datetime.datetime = None
    """Point in time when the ingest attempt was made.
    """

    duration: datetime.timedelta = None
    """Duration of the ingest attempt.
    """

    message: str = None
    """Any info to be associated with an ingest attempt (e.g. error message).
    """

    status: Status = None
    """Status of the ingest attempt.
    """


class Ingester:
    """Framework managing image ingest process.

    Ingester manages the process of ingesting images to different database
    systems.

    It may operate in one of two modes: daemonic and non-daemonic. In the
    default daemonic mode, it waits for new files and when they arrive it
    attempts to ingest each file to a given database system.  In the
    non-daemonic mode, it will terminate once it process all files with a
    specified status.

    Parameters
    ----------
    config : `dict`
        Ingester configuration.

    Raises
    ------
    ValueError
        If required setting is missing.
    """

    def __init__(self, config):
        required = {"plugin", "session", "storage", "tablenames"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        section = config["plugin"]
        self.plugin_cls = section["class"]
        self.plugin_cfg = section["config"]

        self.session = config["session"]
        self.storage = config["storage"]

        required = {"event", "file"}
        missing = required - set(config["tablenames"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.Event = event_creator(config["tablenames"])
        self.File = file_creator(config["tablenames"])

        self.includelist = config.get("includelist")
        if self.includelist is None:
            self.includelist = []
        self.excludelist = config.get("excludelist")
        if self.excludelist is None:
            self.excludelist = []
        self.batch_size = config.get("batch_size", 10)
        self.daemon = config.get("daemon", True)
        self.pause = config.get("pause", 1)
        self.num_threads = config.get("num_threads", 1)
        self.status = config.get("file_status", Status.UNTRIED.value)
        if self.status == Status.SUCCESS.value:
            msg = f"invalid status: {self.status}"
            logger.error(msg)
            raise ValueError(msg)

    def run(self):
        """Start the framework.
        """
        requests = queue.Queue()
        replies = queue.Queue()
        while True:

            # When in normal mode of operation, check for new files.
            if self.status == Status.UNTRIED.value:
                self._fetch()

            # Grab a bunch of files which need to be (re)ingested.
            records = self._grab()
            if not records:
                msg = f"no files with status '{self.status}' to process, "
                if not self.daemon:
                    logger.debug(msg + "terminating.")
                    break
                logger.debug(msg + f"next check in {self.pause} sec.")
                time.sleep(self.pause)
                continue

            # Schedule an ingest attempt for each file, except when a file:
            # (a) is not on the includelist,
            # (b) is excluded,
            # (c) is an empty file
            for rec in records:
                path = os.path.join(rec.relpath, rec.filename)

                message, status = None, None
                if not any(re.search(patt, path) for patt in self.includelist):
                    message = "doesn't match search criteria: not allowed"
                    status = Status.IGNORED
                elif any(re.search(patt, path) for patt in self.excludelist):
                    message = "doesn't match search criteria: excluded"
                    status = Status.IGNORED
                else:
                    try:
                        sz = os.stat(os.path.join(self.storage, path)).st_size
                    except FileNotFoundError:
                        message = "no such file in the storage area"
                    else:
                        if sz == 0:
                            message = f"file has {sz} bytes"
                            status = Status.INVALID
                if message is not None:
                    logger.warning(f"cannot process '{path}': " + message)
                    rep = Reply(
                        id=rec.id,
                        timestamp=datetime.datetime.now(),
                        duration=datetime.timedelta(),
                        message=message,
                        status=status,
                    )
                    replies.put(rep)
                    continue

                # Create a request to make an ingest attempt for the file
                # which passed all search criteria and is not empty.
                req = Request(
                    filepath=os.path.join(self.storage, path),
                    id=rec.id,
                )
                requests.put(req)

            # Create a pool of workers to ingest the files. The pool will be
            # freed once processing is completed.
            threads = []
            num_threads = min(self.num_threads, requests.qsize())
            for _ in range(num_threads):
                t = threading.Thread(target=worker,
                                     args=(requests, replies),
                                     kwargs={"plugin_cls": self.plugin_cls,
                                             "plugin_cfg": self.plugin_cfg})
                t.start()
                threads.append(t)
            for _ in range(len(threads)):
                requests.put(None)
            for t in threads:
                t.join()
            del threads[:]

            # Create the events related to completed ingest attempts.
            events = []
            while not replies.empty():
                rep = replies.get()
                event = self.Event(
                    ingest_ver=rep.version,
                    start_time=rep.timestamp,
                    duration=rep.duration,
                    err_message=rep.message,
                    status=rep.status,
                    files_id=rep.id,
                )
                events.append(event)

            # Add events related to ingests attempts that couldn't be accounted
            # for, e.g. a worker was killed by an external process.
            known = set(rec.id for rec in records)
            processed = set(res.files_id for res in events)
            for id_ in known - processed:
                event = self.Event(
                    start_time=datetime.datetime.now(),
                    status=Status.UNKNOWN,
                    files_id=id_,
                )
                events.append(event)

            # Commit all changes to the database.
            self.session.add_all(events)
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                self.session.rollback()
                logger.error(f"cannot commit updates: {ex}")

            time.sleep(self.pause)

    def _fetch(self):
        """Fetch new files, if any.

        Queries the database for new files.  A file is considered new if
        there are no events associated with it.

        For each selected file, a new event is recorded in the database with
        the status set to UNTRIED.

        No such events will be created if any errors are encountered.
        """
        # Find new files that is files for which there are no recorded events:
        #
        #     SELECT id
        #     FROM <file table> AS f
        #     WHERE NOT EXISTS
        #         (SELECT *
        #         FROM <event table> AS e
        #         WHERE e.files_id = f.id);
        query = self.session.query(self.File.id).\
            filter(~exists().where(self.Event.files_id == self.File.id))
        try:
            for (id_,) in query:
                event = self.Event(status=Status.UNTRIED,
                                   start_time=datetime.datetime.now(),
                                   files_id=id_)
                self.session.add(event)
        except Exception as ex:
            logger.error(f"failed to check for new files: {ex}")
        else:
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                self.session.rollback()
                logger.error(f"failed to add new files: {ex}")

    def _grab(self):
        """Select a group of files for ingestion with a given status.

        Status of the files which will be selected is set during class
        instantiation through a configuration option 'file_status'.

        For each selected file, a new event is recorded in the database with
        the status set to PENDING.

        No such events will be created if any errors are encountered.

        Returns
        -------
        `list`
            List of database records associated with select files; empty if
            no records were found or any errors were encountered.
        """
        # Find files for which the most recent event has a requested status.
        #
        #     SELECT r.*
        #     FROM <files> AS r
        #     INNER JOIN (SELECT s.files_id, s.status
        #                 FROM <events> AS s
        #                 INNER JOIN (SELECT files_id, MAX(start_time) AS last
        #                             FROM <events>
        #                             GROUP BY files_is) AS t
        #                 ON s.files_id = t.files_id
        #                 WHERE s.start_time = t.last AND s.status = '<>') AS u
        #     ON r.id = u.files_id
        #     LIMIT <batch_size>;
        stmt = self.session.query(self.Event.files_id,
                                  func.max(self.Event.start_time).
                                  label("last")).\
            group_by(self.Event.files_id).subquery()
        recent = self.session.query(self.Event.files_id, self.Event.status).\
            join(stmt, stmt.c.files_id == self.Event.files_id).\
            filter(stmt.c.last == self.Event.start_time).\
            filter(self.Event.status == self.status).\
            subquery()
        query = self.session.query(self.File).\
            join(recent, recent.c.files_id == self.File.id).\
            limit(self.batch_size)
        records = []
        try:
            records = list(query)
        except SQLAlchemyError as ex:
            logger.error(f"failed to retrieve files for processing: {ex}")
        else:
            for rec in records:
                event = self.Event(status=Status.PENDING,
                                   start_time=datetime.datetime.now(),
                                   files_id=rec.id)
                self.session.add(event)
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                records.clear()
                self.session.rollback()
                logger.error(f"cannot commit updates: {ex}")
        return records


def worker(inp, out, plugin_cls, plugin_cfg):
    """Perform a given task for incoming inputs.

    Function representing a thread worker.  It takes requests from the input
    channel, process them, and enqueues corresponding replies in the output
    channel.

    Parameters
    ----------
    inp : queue.Queue()
        Channel with ingest requests.
    out : queue.Queue()
        Channel for gathering the results of the ingest requests.
    plugin_cls : `Plugin`, optional
        An ingest plugin to use.
    plugin_cfg : `dict`, optional
        Plugin specific configuration.
    """
    # Instantiate the ingest plugin.
    #
    # We are doing it here, in the worker, to make sure that any database
    # connections used in the data management system ingestion code will not
    # be shared between threads.
    plugin = plugin_cls(plugin_cfg)

    while True:
        req = inp.get()
        if req is None:
            break
        start = datetime.datetime.now()
        try:
            plugin.execute(req.filepath)
        except Exception as exc:
            # Find the root cause of the exception as it seems that for both
            # Gen2 and Gen3 Butler the most meaningful error messages tend
            # to be located at the very bottom of the stack trace.
            while exc.__cause__ is not None:
                exc = exc.__cause__
            exc_msg = traceback.format_exception_only(type(exc), exc)[0]
            message = exc_msg.strip()
            status = Status.FAILURE
            logger.error(message)
        else:
            message = None
            status = Status.SUCCESS
        rep = Reply(
            id=req.id,
            version=plugin.version(),
            timestamp=start,
            duration=datetime.datetime.now() - start,
            message=message,
            status=status
        )
        out.put(rep)
