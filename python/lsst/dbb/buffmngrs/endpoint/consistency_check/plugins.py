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
import os.path
import os
from ..abcs import Plugin


__all__ = ["NullCheck", "Filename", "Directory", "Database", "Gen3Butler"]


logger = logging.getLogger(__name__)
Version = "1"


class NullCheck(Plugin):
    """Do nothing.

    A null object, a no-op.
    """

    def __init__(self, config):
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass


class Filename(Plugin):
    """Gets the list of files to compare from a file
    """

    def __init__(self, config_file):
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass


class Directory(Plugin):
    """Gets the list of files to compare from a directory walk
    """

    def __init__(self, config):
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass


class Database(Plugin):
    """Gets the list of files to compare from a database
    """

    def __init__(self, config):
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass


class Gen3Butler(Plugin):
    """Validates Gen3Butler for correct files.
    """

    def __init__(self, config):
        self.config = {
            "config": None,
            "config_file": None,
            "ingest_task": "lsst.obs.base.RawIngestTask",
            "output_run": None,
            "processes": 1,
            "transfer": "symlink",
        }
        self.config.update(config)
        self.session = self.config["session"]

        required = {"root"}
        missing = required - set(self.config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.root = self.config["root"]
        self.File = self.config["file"]
        records = self.session.query(self.File).all()
        self.inButlerNotFS = []
        self.inFSnotButler = []
        try:
            for rec in records:
                if not os.path.exists(os.path.join(self.root, rec.relpath,
                                                   rec.filename)):
                    self.inButlerNotFS.append(os.path.exists(os.path.join(
                        self.root, rec.relpath, rec.filename)))
        except Exception as ex:
            logger.error(f"failed to retrieve files for comparison: {ex}")

        for dirpath, dirnames, filenames in os.walk(config["root"]):
            for file in filenames:
                notfile = self.session.query(self.File).filter(
                    self.File.filename == file).first()
                if notfile is None:
                    self.inFSnotButler.append(file)

    def version(self):
        return Version

    def execute(self, filename):
        return self.inButlerNotFS, self.inFSnotButler

    def get_list(self):
        pass
