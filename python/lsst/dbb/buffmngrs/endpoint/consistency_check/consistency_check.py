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
"""Component responsible for comparing storage area and repository.
"""
import datetime
import logging
import os
from glob import glob
from itertools import chain
from sqlalchemy.exc import SQLAlchemyError
from ..declaratives import event_creator, file_creator
from ..search import scan
from ..status import Status
from ..utils import get_checksum

__all__ = ["Consistency_check"]

logger = logging.getLogger(__name__)


class Consistency_check:
    """A consistency tool to verify the status of repository and storage area.

    It checks database entries for incomplete values, logged but missing files,
    and files not logged.

    Parameters
    ----------
    config : `dict`
        Consistency configuration.

    Raises
    ------
    ValueError
        If a required setting is missing.
    """

    def __init__(self, config):
        # Check if configuration is valid, i.e., all required settings are
        # provided; complain if not.
        required = {"session", "sources", "storage", "tablenames"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        self.session = config["session"]
        section = config["plugin"]
        self.plugin_cls = section["class"]
        self.plugin_cfg = section["config"]

        # Create necessary object-relational mappings. We are doing it
        # dynamically as RDBMS tables to use are determined at runtime.
        required = {"event", "file"}
        missing = required - set(config["tablenames"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.Event = event_creator(config["tablenames"])
        self.File = file_creator(config["tablenames"])

        self.plugin_cfg["tablenames"] = config["tablenames"]
        self.plugin_cfg["session"] = config["session"]
        self.plugin_cfg["file"] = self.File
        self.root = os.path.abspath(config["root"])
        if not os.path.isdir(self.root):
            msg = f"directory '{self.root}' not found"
            logger.error(msg)
            raise ValueError(msg)

    def run(self):
        """Start the framework.
        """

        plugin = self.plugin_cls(self.plugin_cfg)
        results_source, results_dir = plugin.execute()

        if results_source is None:
            print(f"Plugin {self.plugin_cls} Consistency check passed")
            logger.info("plugin %s:Consistency check passed" % self.plugin_cls)
        else:
            print(f"Plugin {self.plugin_cls} reports the following entries "
                  f" are listed but not found: {results_source}")

        if results_dir is None:
            print(f"Plug {self.plugin_cls} directory check passed")
            logger.info("Plugin %s: directory check passed" % self.plugin_cls)
        else:
            print(f"Plugin {self.plugin_cls} found the following files in the "
                  f"directory but not processed: {results_dir}" )

