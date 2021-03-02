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
"""Functions allowing for dynamic creation of object relational mappers.
"""
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Interval,
    String,
    Text)
from sqlalchemy.ext.declarative import declarative_base

from .status import Status


__all__ = ["event_creator", "file_creator"]


Base = declarative_base()


def file_creator(orms):
    """Declarative for database file entries.

    Parameters
    ----------
    orms : `dict`
        A mapping between declarative names and the names of the database
        table it represents.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    schema = orms["file"]["schema"]
    tablename = orms["file"]["table"]
    attributes = {
        "__tablename__": tablename,
        "__table_args__": {"schema": schema},
        "id": Column(BigInteger, primary_key=True),
        "relpath": Column(String, nullable=False),
        "filename": Column(String, nullable=False, unique=True),
        "checksum": Column(String, nullable=False, unique=True),
        "size_bytes": Column(BigInteger, nullable=False),
        "added_on": Column(DateTime, nullable=False, default=datetime.now),
    }
    return type("File", (Base,), attributes)


def event_creator(orms):
    """Create a declarative for database ingest attempts.

    Parameters
    ----------
    orms : `dict`
        A mapping between declarative names and the names of the database
        table it represents.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    schema = orms["event"]["schema"]
    tablename = orms["event"]["table"]
    file_fqn = ".".join([orms["file"]["schema"], orms["file"]["table"]])
    attributes = {
        "__tablename__": tablename,
        "__table_args__": {"schema": schema},
        "ingest_ver": Column(String),
        "start_time": Column(DateTime, primary_key=True),
        "duration": Column(Interval),
        "err_message": Column(Text),
        "status": Column(Enum(Status,
                              values_callable=lambda x: [e.value for e in x])),
        "files_id": Column(Integer, ForeignKey(f"{file_fqn}.id"),
                           primary_key=True),
    }
    return type("Event", (Base,), attributes)
