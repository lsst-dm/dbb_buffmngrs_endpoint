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
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
import importlib
import sys
import lsst.log
from lsst.daf.butler import Butler
from ..abcs import Plugin
from ..declaratives import file_creator

__all__ = ["NullCompare", "Filename", "Directory", "Database", "Gen3Butler"]


logger = logging.getLogger(__name__)
Version = "1"


class NullCompare(Plugin):
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
        with open(config_file) as fp:
            self.lines = fp.read().splitlines()

    def version(self):
        return Version

    def execute(self, filename):
        pass

    def get_list(self):
        return self.lines


class Directory(Plugin):
    """Gets the list of files to compare from a directory walk
    """

    def __init__(self, config):
        self.files = []
        for dirpath, dirnames, filenames in os.walk(config):
            for file in filenames:
                self.files += [os.path.join(dirpath,file)]

    def version(self):
        return Version

    def execute(self, filename):
        pass

    def get_list(self):
        return self.files


class Database(Plugin):
    """Gets the list of files to compare from a database
    """

    def __init__(self, config):
        logger.info("setting up database connection...")
        module = importlib.import_module("sqlalchemy.pool")
        pool_name = "QueuePool"
        try:
            class_ = getattr(module, pool_name)
        except AttributeError as ex:
            raise RuntimeError(f"unknown connection pool: {pool_name}") from ex
        try:
            engine = sqlalchemy.create_engine(config,
                                   echo=False,
                                   poolclass=class_)
        except (DBAPIError, SQLAlchemyError) as ex:
            raise RuntimeError(ex) from ex

        Session = sessionmaker(bind=engine)
        session = Session()

        file_creator_config = {"file": {"schema": "null", "table": "loc_inst_files"}}
        self.File=file_creator(file_creator_config)
        records = session.query(self.File).all()
        self.files = []
        for record in records:
            self.files.append(os.path.join(record.relpath, record.filename))
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass

    def get_list(self):
        return self.files


class Gen3Butler(Plugin):
    """Gets the list of files to compare from a Gen3 Butler repository
    """

    def __init__(self, config):
        pass

    def version(self):
        return Version

    def execute(self, filename):
        pass

    def get_list(self):
        pass