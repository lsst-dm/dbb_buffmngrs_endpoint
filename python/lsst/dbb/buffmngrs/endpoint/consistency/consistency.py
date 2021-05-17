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
        required = {"sourceA", "sourceB", "sourceA_settings", "sourceB_settings"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        Left_source = config["sourceA"](config["sourceA_settings"])
        Right_source = config["sourceB"](config["sourceB_settings"])

        self.Left_list = Left_source.get_list()
        self.Right_list = Right_source.get_list()

    def run(self):
        """Start the framework.
        """

        set_left = set(self.Left_list)
        set_right = set(self.Right_list)

        missing_left = [file for file in self.Right_list if
                        file not in set_left]
        missing_right = [file for file in self.Left_list if
                         file not in set_right]
        same_files = set_left.intersection(set_right)

        print(f"Files in both sources: {same_files}")
        print(f"Files missing from sourceA: {missing_left}")
        print(f"Files missing from sourceB: {missing_right}")

