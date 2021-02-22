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

__all__ = ["Consistency"]

logger = logging.getLogger(__name__)

class Consistency:
    """A consistency tool to verify the status of repository and storage area.

    It compares database entries against the storage area and checks for discrepancies

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

        self.storage = os.path.abspath(config["storage"])
        if not os.path.isdir(self.storage):
            msg = f"directory '{self.storage}' not found"
            logger.error(msg)
            raise ValueError(msg)

        self.sources = config["sources"]
        for src in [path for path in self.sources if path.startswith("/")]:
            logger.warning(f"{src} is absolute, should be relative")
            if os.path.commonpath([self.storage, src]) != self.storage:
                msg = f"{src} is not located in the storage area"
                logger.error(msg)
                raise ValueError(msg)

        search = config["search"]
        self.search_opts = dict(blacklist=search.get("blacklist", None))
