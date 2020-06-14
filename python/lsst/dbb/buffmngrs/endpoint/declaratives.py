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
from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


__all__ = ["attempt_creator", "file_creator", "status_creator"]


Base = declarative_base()


def file_creator(orms):
    """Declarative for database file entries.
    """
    attributes = {
        "__tablename__": orms["file"],
        "id": Column(Integer, primary_key=True),
        "url": Column(String),
        "checksum": Column(String),
        "added_at": Column(String),
    }
    return type("File", (Base,), attributes)


def status_creator(orms):
    """Create a declarative for database ingest statuses.

    Parameters
    ----------
    orms : `dict`
        A mapping between roles and database table names.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    attributes = {
        "__tablename__": orms["status"],
        "id": Column(Integer, primary_key=True),
        "url": Column(String),
        "status": Column(String),
        "attempts": relationship("Attempt"),
    }
    return type("Status", (Base,), attributes)


def attempt_creator(orms):
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
        "__tablename__": orms["attempt"],
        "id": Column(Integer, primary_key=True),
        "task_ver": Column(String),
        "made_at": Column(String),
        "duration": Column(Integer),
        "traceback": Column(Text),
        "status_id": Column(Integer, ForeignKey(f"{orms['status']}.id")),
    }
    return type("Attempt", (Base,), attributes)
