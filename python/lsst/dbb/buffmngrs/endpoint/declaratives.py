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
from sqlalchemy.orm import relationship
from .status import Status


__all__ = ["event_creator", "file_creator"]


Base = declarative_base()


def file_creator(orms):
    """Declarative for database file entries.
    """
    attributes = {
        "__tablename__": orms["file"],
        "id": Column(BigInteger, primary_key=True),
        "relpath": Column(String, nullable=False),
        "filename": Column(String, nullable=False, unique=True),
        "checksum": Column(String, nullable=False, unique=True),
        "added_on": Column(DateTime, nullable=False, default=datetime.now),
        "events": relationship("Event")
    }
    return type("File", (Base,), attributes)


def event_creator(orms):
    """Create a declarative for database ingest attempts.

    Parameters
    ----------
    orms : `dict`
        Name of the table in the database.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    attributes = {
        "__tablename__": orms["event"],
        "ingest_ver": Column(String),
        "start_time": Column(DateTime, primary_key=True),
        "duration": Column(Interval),
        "err_message": Column(Text),
        "status": Column(Enum(Status)),
        "files_id": Column(Integer, ForeignKey(f"{orms['file']}.id"),
                           primary_key=True),
    }
    return type("Event", (Base,), attributes)
