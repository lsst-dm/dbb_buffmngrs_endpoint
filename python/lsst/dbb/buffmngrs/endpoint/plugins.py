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
import importlib
import logging
import lsst.pipe.tasks.ingest as ingest

__all__ = ["NullIngest", "Gen2Ingest"]


logger = logging.getLogger(__name__)


class NullIngest(object):
    """Do nothing.

    A null object, a no-op.
    """

    def __init__(self, config):
        pass

    def version(self):
        return ""

    def execute(self, filename):
        pass


class Gen2Ingest(object):
    """Ingest a file to Gen2 Butler repository.

    Parameters
    ----------
    config : `dict`


    Raises
    ------
    ValueError
        If root directory of the Bulter repository is not provided.
    """

    def __init__(self, config):
        required = {"root"}
        missing = required - set(config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.critical(msg)
            raise ValueError(msg)
        root = config["root"]
        mode = config.get("mode", "link")
        opts = dict(mode=mode)
        self.task = ingest.IngestTask.prepareTask(root, **opts)

        pkg = importlib.import_module(ingest.__package__)
        self._version = "not available"
        try:
            self._version = getattr(pkg, "__version__")
        except AttributeError:
            logger.warning("failed to identified plugin version")

    def version(self):
        """Return the version of the LSST ingest task.

        Returns
        -------
        `str`
            Version of the LSST ingest task used for the ingest attempt.
        """
        return self._version

    def execute(self, filename):
        """Make an attempt to ingest the file.

        Parameters
        ----------
        filename : `str`
            Path to the file.
        """
        self.task.ingestFiles(filename)
