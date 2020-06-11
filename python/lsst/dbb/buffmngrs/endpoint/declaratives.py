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


__all__ = ["File", "attempt_creator", "status_creator"]


Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    url = Column(String)
    checksum = Column(String)
    added_at = Column(String)

    def __repr__(self):
        return f"<File(url='{self.url}', checksum='{self.checksum}, " \
               f"added_at='{self.added_at}')>"


def status_creator(prefix):
    """Create a declarative for ingest statuses.

    Parameters
    ----------
    prefix: `str`
        A prefix identifying the database table name which will be used to
        store ingest statuses.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    attributes = {
        "__tablename__": f"{prefix}_statuses",
        "id": Column(Integer, primary_key=True),
        "url": Column(String),
        "status": Column(String),
        "attempts": relationship(f"{prefix.capitalize()}Attempt"),
    }
    return type(f"{prefix.capitalize()}Status", (Base,), attributes)


def attempt_creator(prefix):
    """Create a declarative for ingest attempts.

    Parameters
    ----------
    prefix: `str`
        A prefix identifying the database table name which will be used to
        store ingest attempts.

    Returns
    -------
    `sqlalchemy.ext.declarative.api.DeclarativeMeta`
        A declarative (class), a Pythonic representation of database table.
    """
    attributes = {
        "__tablename__": f"{prefix}_attempts",
        "id": Column(Integer, primary_key=True),
        "task_ver": Column(String),
        "made_at": Column(String),
        "duration": Column(Integer),
        "traceback": Column(Text),
        "status_id": Column(Integer, ForeignKey(f"{prefix}_statuses.id")),
    }
    return type(f"{prefix.capitalize()}Attempt", (Base,), attributes)
