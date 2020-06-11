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
import queue
import threading
import time
import traceback
from collections import namedtuple
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from sqlalchemy.sql import exists
from .declaratives import File, attempt_creator, status_creator
from .plugins import NullIngest


__all__ = ["Ingester"]


Result = namedtuple('Result', ['filename', 'timestamp', 'duration', 'message'])


logger = logging.getLogger(__name__)
status = {
    "untried": "UNTRIED",
    "awaits": "AWAITING",
    "success": "SUCCESS",
    "failure": "FAILURE",
    "unknown": "INTERRUPTED",
}


class Ingester(object):
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
        If required setting is missing or any
    """

    def __init__(self, config):
        required = {"plugin", "session"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            raise ValueError(msg)
        self.plugin = config["plugin"]
        self.session = config["session"]

        prefix = config["tables"]["prefix"]
        self.Attempt = attempt_creator(prefix)
        self.Status = status_creator(prefix)

        self.batch_size = config.get("batch_size", 10)
        self.daemon = config.get("daemon", True)
        self.pause = config.get("pause", 1)
        self.pool_size = config.get("pool_size", 1)
        self.status = config.get("file_status", status["untried"])
        if self.status == status["success"]:
            msg = f"invalid status: {self.status}"
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
            if self.status == status["untried"]:
                self._fetch()

            # Grab a bunch of files which need to be (re)ingested.
            try:
                query = self.session.query(self.Status).\
                    filter(self.Status.status == self.status).\
                    limit(self.batch_size)
            except SQLAlchemyError as ex:
                logger.error(f"failed to retrieve files for processing: {ex}")
                time.sleep(self.pause)
                continue
            else:
                records = {rec.url: rec for rec in query}
            if not records:
                msg = f"No files with status '{self.status}' to process, "
                if not self.daemon:
                    logger.debug(msg + "terminating.")
                    break
                logger.debug(msg + f"next check in {self.pause} sec.")
                time.sleep(self.pause)
                continue

            # Update statuses of the files and enqueue them for ingesting.
            for rec in records.values():
                rec.status = status["awaits"]
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"cannot commit updates: {ex}")
            else:
                for url in records:
                    inp.put(url)

            # Create a pool of workers to ingest the files. The pool will be
            # freed once processing is completed.
            pool = []
            pool_size = min(self.pool_size, inp.qsize())
            for _ in range(pool_size):
                t = threading.Thread(target=worker,
                                     args=(inp, out, err),
                                     kwargs={"task": self.plugin})
                t.start()
                pool.append(t)
            for _ in range(len(pool)):
                inp.put(None)
            for t in pool:
                t.join()
            del pool[:]

            # Process the results of the ingest attempts.
            processed = set()

            # Update statuses of the files for which ingest attempt was made
            # and succeeded.
            attempts = self._process(out)
            for url, att in attempts.items():
                records[url].status = status["success"]
                records[url].attempts.append(att)
            processed.update(attempts)

            # Update statuses of the files for which ingest attempt was made
            # but failed.
            attempts = self._process(err)
            for url, att in attempts.items():
                records[url].status = status["failure"]
                records[url].attempts.append(att)
            processed.update(attempts)

            # Update statuses of the files for which the ingest attempt failed
            # for other reasons, e.g. a worker was killed by an external
            # process.
            unfinished = set(records) - processed
            for url in unfinished:
                records[url].status = status["unknown"]

            # Commit all changes to the database.
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
        try:
            query = self.session.query(File.url). \
                filter(~exists().where(File.url == self.Status.url))
        except (ProgrammingError, SQLAlchemyError) as ex:
            logger.error(f"failed to check for new files: {ex}")
        else:
            for url in query:
                rec = self.Status(url=url, status=status["untried"])
                self.session.add(rec)
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"failed to add new files: {ex}")

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
            File names coupled with the results of their ingest attempt.
        """
        attempts = {}
        while not channel.empty():
            path, timestamp, duration, message = channel.get()
            attempt = {
                "task_ver": self.plugin.version(),
                "made_at": timestamp,
                "duration": duration,
                "traceback": message,
            }
            attempts[path] = self.Attempt(**attempt)
        return attempts


def worker(inp, out, err, task=None):
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
    task : optional
        A task to perform on each file name. If None (default), nothing will be
        done, effectively a no-op.
    """
    if task is None:
        task = NullIngest(dict())
    while True:
        filename = inp.get()
        if filename is None:
            break

        start = datetime.datetime.now()
        chn, msg = None, None
        try:
            task.execute(filename)
        except:
            # No idea what kind of exception different LSST task can throw,
            # hence bare exception here.
            chn, msg = err, f"{traceback.format_exc()}"
        else:
            chn, msg = out, ""
        finally:
            dur = datetime.datetime.now() - start
            chn.put(Result(filename,
                           start.isoformat(timespec="milliseconds"),
                           dur / datetime.timedelta(milliseconds=1),
                           msg))
