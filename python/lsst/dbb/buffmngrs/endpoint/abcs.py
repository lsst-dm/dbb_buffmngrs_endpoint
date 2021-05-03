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
"""Definitions of abstract base classes.
"""
import abc


__all__ = ["Action", "Macro", "Plugin"]


class Action(abc.ABC):
    """Define interface for an action.
    """

    def __init__(self):
        self._fp = None

    @property
    def path(self):
        """Current path to the file.  None if action was not executed.
        """
        return self._fp

    @abc.abstractmethod
    def execute(self, path):
        """Execute action.

        Parameters
        ----------
        path : `str`
            Path to the file.
        """

    @abc.abstractmethod
    def undo(self):
        """Roll action back.
        """


class Macro(Action):
    """Define a sequence of actions to perform.
    """

    def __init__(self):
        Action.__init__(self)
        self._actions = []

    def add(self, act):
        """Add a step to the sequence.

        Parameters
        ----------
        act : Action
            An action that needs to be added to the sequence.
        """
        if not isinstance(act, Action):
            ValueError(f"Cannot add {act}: not an Action.")
        self._actions.append(act)

    def remove(self, i=None):
        """Remove an action at the given position the sequence.

        If no index is specified, it removes the last action in the sequence.

        Parameters
        ----------
        i : int, optional
            An action that needs to be removed from the sequence.
        """
        if i is None:
            i = len(self._actions) - 1
        self._actions.pop(i)

    def execute(self, path):
        """Execute the sequence on a file.

        Parameters
        ----------
        path : `str`
            Path to the file.
        """
        self._fp = path
        for a in self._actions:
            a.execute(self._fp)
            self._fp = a.path

    def undo(self):
        """Roll back actions performed on the file.

        Returns
        -------
        `str`
            Location of the file after actions are rolled back.
        """
        for a in reversed(self._actions):
            a.undo()
            self._fp = a.filepath


class Plugin(abc.ABC):
    """Define the interface for a plugins.
    """
    @abc.abstractmethod
    def execute(self, data):
        """Run plugin.

        Parameters
        ----------
        data : Any
            Input data for the LSST software.
        """

    @abc.abstractmethod
    def version(self):
        """Return the version of the data management system ingest software.
        """
