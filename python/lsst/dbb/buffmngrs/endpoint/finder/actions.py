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
import os
import shutil
from .abcs import Action


__all__ = ["Null", "Move", "Delete"]


logger = logging.getLogger(__name__)


class Null(Action):
    """Do nothing.

    A null object, a no-op action.

    Parameters
    ----------
    config : `dict`
        A dictionary, maybe empty as no operation will be performed on it.
        This is just to keep the Action interface consistent.
    """

    def __init__(self, config):
        Action.__init__(self)

    def execute(self, path):
        """Execute action for a given file pathname.

        Parameters
        ----------
        path : `str`
            Path to the file.
        """
        self._fp = path

    def undo(self):
        """Roll the action back.
        """
        pass


class Move(Action):
    """Move files from one location (src) to another location (dst).

    Action attempts to move a file between two directories: ``src`` and
    ``dst``, preserving the directory structure.  For example,
    if originally file was located at ``src/foo/bar/baz/file.txt``, its final
    location will be ``dst/foo/bar/baz/file.txt``.

    Parameters
    ----------
    config : `dict`
        Configuration specifying ``src`` and ``dst``.

    Raises
    ------
    ValueError
        If provided configuration is missing any of its required settings or
        when either source or destination locations are not directories.
    """

    def __init__(self, config):
        Action.__init__(self)

        required = {"src", "dst"}
        missing = required - set(config)
        if missing:
            raise ValueError(f"{', '.join(missing)} not provided.")
        self.src = os.path.abspath(config["src"])
        self.dst = os.path.abspath(config["dst"])
        if self.src is None or self.dst is None:
            messages = {
                1: "source location not specified",
                2: "destination location not specified",
                3: "neither source nor destination location are specified"
            }
            score = 0
            if self.src is None:
                score += 1
            if self.dst is None:
                score += 2
            raise ValueError(messages[score])
        for path in (self.src, self.dst):
            if not os.path.isdir(path):
                raise ValueError(f"directory '{path}' not found.")

        self.old = None
        self.new = None

    def execute(self, path):
        """Execute action for a given file pathname.

        Parameters
        ----------
        path : `str`
            Path to the file.

        Returns
        -------
        `str`
            Location of the file after action is executed.

        Raises
        ------
        RuntimeError
            If any problems are encountered during the execution.
        """
        self.old = os.path.abspath(path)

        # Make sure provided file is somewhere in the source location.
        try:
            prefix = os.path.commonpath([self.old, self.src])
        except ValueError as ex:
            raise RuntimeError(f"cannot check if {self.old} is in {self.src}")
        if prefix != self.src:
            raise RuntimeError(f"cannot execute: {self.old} not in {self.src}")

        filename = os.path.basename(self.old)
        subdir = os.path.relpath(os.path.dirname(self.old), start=self.src)
        self.new = os.path.abspath(os.path.join(self.dst, subdir, filename))
        try:
            os.makedirs(os.path.dirname(self.new), exist_ok=True)
            shutil.move(self.old, self.new)
        except OSError as ex:
            self.old, self.new = None, None
            raise RuntimeError(f"cannot execute: {ex}")
        self._fp = self.new

    def undo(self):
        """Move file back to original location.

        If the directory is empty after moving back the file to its original
        location, it will be deleted.

        Returns
        -------
        `str`
            Location of the file after action is rolled back.

        Raises
        ------
        RuntimeError
            If any problems are encountered during the execution.
        """
        if None in (self.old, self.new):
            msg = "cannot undo: action not performed or already reverted."
            raise RuntimeError(msg)

        try:
            shutil.move(self.new, self.old)
        except OSError as ex:
            raise RuntimeError(f"cannot undo: {ex}")
        self._fp = self.old

        # Remove empty subdirectories created while moving the file its
        # original location.
        cwd = os.getcwd()
        try:
            os.chdir(self.dst)
            os.removedirs(os.path.dirname(self.new))
        except OSError as ex:
            raise RuntimeError(f"cannot clean up: {ex}")
        finally:
            os.chdir(cwd)
            self.old, self.new = None, None


class Delete(Action):
    """Delete a file.

    This action in irreversible!  Rolling it back is not implemented.
    """

    def __init__(self, config):
        Action.__init__(self)

    def execute(self, path):
        """Execute action for a given file.

        Parameters
        ----------
        path : `str`
            Path to a file.
        """
        path = os.path.abspath(path)
        try:
            os.remove(path)
        except OSError as ex:
            logger.error(f"Cannot remove '{path}': {ex}.")
        else:
            self._fp = None

    def undo(self):
        """Rolls the action back.

        Raises
        ------
        NotImplementedError
            When called.
        """
        raise NotImplementedError("Action cannot be rolled back.")
