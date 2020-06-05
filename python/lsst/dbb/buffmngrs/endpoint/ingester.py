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
import logging
import queue
import threading
import time
from collections import namedtuple
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from sqlalchemy.sql import exists
from .declaratives import Attempt, File, Status
from .plugins import NullIngest


__all__ = ["Ingester"]


Result = namedtuple('Result', ['filename', 'timestamp', 'duration', 'message'])


logger = logging.getLogger(__name__)
status = {
    "untried": "UNTRIED",
    "awaits": "AWAITS",
    "success": "SUCCESS",
    "failure": "FAILURE",
    "unknown": "INCOMPLETE",
}


class Ingester(object):

    def __init__(self, config):
        required = {"plugin", "session"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.critical(msg)
            raise ValueError(msg)
        self.plugin = config["plugin"]
        self.session = config["session"]

        self.status = config.get("file_status", status["untried"])
        self.batch_size = config.get("batch_size", 10)
        self.pool_size = config.get("pool_size", 1)
        self.pause = config.get("pause", 1)
        self.daemon = config.get("daemon", True)

    def run(self):
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
            records = {}
            try:
                query = self.session.query(Status).\
                    filter(Status.status == self.status).\
                    limit(self.batch_size)
            except SQLAlchemyError as ex:
                logger.error(f"cannot retrieve files to process: {ex}")
            else:
                records = {rec.url: rec for rec in query}

            # Update their statuses and enqueue them for processing.
            for rec in records.values():
                rec.status = status["awaits"]
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"cannot commit updates: {ex}")
            else:
                for rec in records:
                    inp.put(rec.url)

            if inp.empty():
                logger.debug("no files to process")
                if not self.daemon:
                    break
                time.sleep(self.pause)
                continue

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

            # Update records for files for which the ingest attempt succeeded.
            attempts = self._process(out)
            for url, att in attempts.items():
                records[url].status = status["success"]
                records[url].attempts.append(att)
            processed.update(attempts)

            # Update records for files for which the ingest attempt failed.
            attempts = self._process(err)
            for url, att in attempts.items():
                records[url].status = status["failure"]
                records[url].attempts.append(att)
            processed.update(attempts)

            # Update records for files for which the ingest attempt failed
            # other reasons, e.g. a worker was killed by an external process.
            unfinished = set(records) - processed
            for url in unfinished:
                records[url].status = status["unknown"]

            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"cannot commit updates: {ex}")

            time.sleep(self.pause)

    def _fetch(self):
        """Updates

        Returns
        -------

        """
        try:
            query = self.session.query(File.url). \
                filter(exists().where(File.url != Status.url))
        except (ProgrammingError, SQLAlchemyError) as ex:
            logger.error(f"failed to add new files: {ex}")
        else:
            for url in query:
                rec = Status(url=url, status=status["untried"])
                self.session.add(rec)
            try:
                self.session.commit()
            except SQLAlchemyError as ex:
                logger.error(f"cannot add new files: {ex}")

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
                "version": self.plugin.version(),
                "made_at": timestamp,
                "duration": duration,
                "error": message,
            }
            attempts[path] = Attempt(**attempt)
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

        start = time.time()
        chn, msg = None, None
        try:
            task.execute(filename)
        except RuntimeError as ex:
            chn, msg = err, f"{ex}"
        else:
            chn, msg = out, ""
        finally:
            dur = time.time() - start
            chn.put(Result(filename, start, dur, msg))
