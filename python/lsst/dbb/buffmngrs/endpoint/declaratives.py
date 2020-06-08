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


__all__ = ["Attempt", "File", "Status"]


Base = declarative_base()


class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)
    url = Column(String(255))
    checksum = Column(String(128))
    added_at = Column(String(26))

    def __repr__(self):
        return f"<File(url='{self.url}', checksum='{self.checksum}, " \
               f"added_at='{self.added_at}')>"


class Status(Base):
    __tablename__ = "statuses"
    id = Column(Integer, primary_key=True)
    url = Column(String)
    status = Column(String)
    attempts = relationship("Attempt", back_populates="statuses",
                            order_by="Attempt.id")

    def __repr__(self):
        return f"<Status(url='{self.url}', status='{self.status})>"


class Attempt(Base):
    __tablename__ = "attempts"
    id = Column(Integer, primary_key=True)
    version = Column(String)
    made_at = Column(String(26))
    duration = Column(Integer)
    traceback = Column(Text)
    status_id = Column(Integer, ForeignKey("statuses.id"))
    status = relationship("Status", back_populates="attempts")

    def __repr__(self):
        return f"<Attempt(version='{self.version}, " \
               f"made_at='{self.made_at}', duration='{self.duration}', " \
               f"traceback='{self.error}')>"
