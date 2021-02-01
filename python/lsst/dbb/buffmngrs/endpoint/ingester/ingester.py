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
import threading
import time
import traceback
from collections import namedtuple
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import exists, func
from .plugins import NullIngest
from ..declaratives import event_creator, file_creator
from ..status import Status


__all__ = ["Ingester"]


logger = logging.getLogger(__name__)


field_names = ['abspath', 'timestamp', 'duration', 'message', 'version']
Result = namedtuple('Result', field_names)


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
        # Check if we are connected to the right database that is if the
        # required tables are available.

        inp = queue.Queue()
        out = queue.Queue()
        err = queue.Queue()
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

            # Schedule an ingest attempt for each file, except when a file
            # does not exist or is empty.
            for path, rec in records.items():
                msg = ""
                try:
                    sz = os.stat(path).st_size
                except FileNotFoundError:
                    msg = "no such file in the storage area"
                else:
                    if sz == 0:
                        msg = f"file has {sz} bytes"
                if msg:
                    logger.warning(f"cannot process '{path}': " + msg)
                    ts = datetime.datetime.now()
                    fields = {
                        "abspath": path,
                        "timestamp": ts,
                        "duration": datetime.timedelta(),
                        "message": msg,
                        "version": "N/A"
                    }
                    err.put(Result(**fields))
                    continue
                inp.put(path)

            # Create a pool of workers to ingest the files. The pool will be
            # freed once processing is completed.
            threads = []
            num_threads = min(self.num_threads, inp.qsize())
            for _ in range(num_threads):
                t = threading.Thread(target=worker,
                                     args=(inp, out, err),
                                     kwargs={"plugin_cls": self.plugin_cls,
                                             "plugin_cfg": self.plugin_cfg})
                t.start()
                threads.append(t)
            for _ in range(len(threads)):
                inp.put(None)
            for t in threads:
                t.join()
            del threads[:]

            # Process the results of the ingest attempts.
            processed = {}

            # Update statuses of the files for which ingest attempt was made
            # and succeeded.
            events = self._process(out)
            for url in events:
                rec, evt = records[url], events[url]
                evt.status = Status.SUCCESS
                evt.files_id = rec.id
            processed.update(events)

            # Update statuses of the files for which ingest attempt was made
            # but failed.
            events = self._process(err)
            for url in events:
                rec, evt = records[url], events[url]
                evt.status = Status.FAILURE
                evt.files_id = rec.id
            processed.update(events)

            # Update statuses of the files for which the ingest attempt failed
            # for other reasons, e.g. a worker was killed by an external
            # process.
            events = {}
            for url in set(records) - set(processed):
                rec = records[url]
                evt = self.Event(status=Status.UNKNOWN,
                                 start_time=datetime.datetime.now(),
                                 files_id=rec.id)
                events[url] = evt
            processed.update(events)

            # Commit all changes to the database.
            self.session.add_all(processed.values())
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"cannot commit updates: {ex}")

            time.sleep(self.pause)

    def _fetch(self):
        """Fetch new files, if any.

        Queries the database for newly discovered files and enqueues them for
        processing.
        """
        # Find new files that is files for which there is no recorded events:
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
                rec = self.Event(status=Status.UNTRIED.value,
                                 start_time=datetime.datetime.now(),
                                 files_id=id_)
                self.session.add(rec)
        except Exception as ex:
            logger.error(f"failed to check for new files: {ex}")
        else:
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"failed to add new files: {ex}")

    def _grab(self):
        """Select a group of files for ingestion.

        Returns
        -------
        `dict`
            Files' absolute paths mapped to their database records.
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
                                  func.max(self.Event.start_time).\
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
        records = {}
        try:
            for rec in query:
                event = self.Event(status=Status.PENDING,
                                   start_time=datetime.datetime.now(),
                                   files_id=rec.id)
                self.session.add(event)
                key = os.path.join(self.storage, rec.relpath, rec.filename)
                records[key] = rec
        except Exception as ex:
            logger.error(f"failed to retrieve files for processing: {ex}")
        try:
            self.session.commit()
        except SQLAlchemyError as ex:
            logger.error(f"cannot commit updates: {ex}")
            records.clear()
        return records

    def _process(self, channel):
        """Process results of ingest attempts from a given channel.

        Parameters
        ----------
        channel : queue.Queue
            Communication channel with the results of the ingest attempts to
            process.

        Returns
        -------
        `dict`
            Files' absolute paths mapped to the results of their ingest
            attempt.
        """
        events = {}
        while not channel.empty():
            abspath, timestamp, duration, message, version = channel.get()
            fields = {
                "ingest_ver": version,
                "start_time": timestamp,
                "duration": duration,
                "err_message": message,
            }
            events[abspath] = self.Event(**fields)
        return events


def worker(inp, out, err, plugin_cls=None, plugin_cfg=None):
    """Perform a given task for incoming inputs.

    Function representing a thread worker.  It takes a file name from its input
    channel and performs an operation on it.

    Parameters
    ----------
    inp : queue.Queue()
        Input channel, source of file names for which a task needs to be
        performed.
    out : queue.Queue()
        Output channel for results of the tasks which completed successfully.
    err : queue.Queue()
        Error channel for results of the tasks which failed.
    plugin_cls : `Plugin`, optional
        An ingest plugin to use. If None (default), nothing will be
        done, effectively a no-op.
    plugin_cfg : `dict`, optional
        Plugin specific configuration. It will be ignored in ``plugin_cls``
        is None.
    """
    # Instantiate the ingest plugin.
    #
    # We are doing it here, in the worker, to make sure that any database
    # connections used in the data management system ingestion code will not
    # be shared between threads.
    if plugin_cls is None:
        plugin_cls = NullIngest
        plugin_cfg = dict()
    plugin = plugin_cls(plugin_cfg)

    while True:
        filename = inp.get()
        if filename is None:
            break
        start = datetime.datetime.now()
        chn, msg = None, None
        try:
            plugin.execute(filename)
        except RuntimeError as ex:
            logger.error(f"{traceback.format_exc()}")
            chn, msg = err, f"{ex}"
        else:
            chn, msg = out, ""
        finally:
            duration = datetime.datetime.now() - start
            fields = {
                "abspath": filename,
                "timestamp": start,
                "duration": duration,
                "message": msg,
                "version": plugin.version()
            }
            chn.put(Result(**fields))
