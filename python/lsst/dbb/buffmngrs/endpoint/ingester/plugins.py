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
"""Definitions of ingest plugins.
"""
import logging
import sys
import lsst.log
from lsst.daf.butler import Butler
from lsst.pipe.base.configOverrides import ConfigOverrides
from lsst.pipe.tasks.ingest import IngestTask
from lsst.utils import doImport
from ..abcs import Plugin


__all__ = ["NullIngest", "Gen2Ingest", "Gen3Ingest"]


logger = logging.getLogger(__name__)


class NullIngest(Plugin):
    """Do nothing.

    A null object, a no-op.
    """

    def __init__(self, config):
        pass

    def version(self):
        return ""

    def execute(self, filename):
        pass


class Gen2Ingest(Plugin):
    """Ingest a file to Gen2 Butler repository.

    Parameters
    ----------
    config : `dict`
        Plugin configuration.

    Raises
    ------
    ValueError
        If root directory of the Butler repository is not provided.

    Notes
    -----
    See ``lsst.pipe.tasks.ingest`` for details.
    """

    def __init__(self, config):
        # Set default values for configuration settings, but override
        # them with the ones provided at runtime, if necessary.
        self.config = {
            "dryrun": False,
            "mode": "link",
            "create": False,
            "ignoreIngested": False,
        }
        self.config.update(config)

        required = {"root"}
        missing = required - set(config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.error(msg)
            raise ValueError(msg)

        # Initialize LSST ingest software.
        with lsst.log.UsePythonLogging():
            self.task = IngestTask.prepareTask(**self.config)

        # Retrieve the version of the LSST ingest software in use.
        pkg_name = sys.modules[IngestTask.__module__].__package__
        pkg = sys.modules[pkg_name]
        self._version = "N/A"
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

    def execute(self, path):
        """Make an attempt to ingest the file.

        Parameters
        ----------
        path : `str`
            Path to the file.
        """
        with lsst.log.UsePythonLogging():
            self.task.ingestFiles(path)


class Gen3Ingest(Plugin):
    """Ingest a file to Gen3 Butler repository.

    The LSST Gen3 Middleware supports parallel ingestion of multiple files.
    However, it does not expose information which files were ingested
    successfully and which failed to ingest.  Hence, the plugin is
    deliberately restricted to ingesting one file at the time.  To enable
    concurrent ingests, use Ingester's ``num_threads`` setting instead.

    Parameters
    ----------
    config : `dict`
        Plugin configuration.

    Raises
    ------
    ValueError
        If root directory of the Butler repository is not provided.

    Notes
    -----
    The code below is essentially a customized version of
    `script/ingestRaws.py`` from ``lsst.obs.base`` package.
    """

    def __init__(self, config):
        # Set default values for configuration settings, but override their
        # them with the ones provided at runtime, if necessary.
        #
        # As parallel ingestion is disabled, ``processes`` setting will be
        # ignored and is included only for sake of completeness.
        self.config = {
            "config": None,
            "config_file": None,
            "ingest_task": "lsst.obs.base.RawIngestTask",
            "output_run": None,
            "processes": 1,
            "transfer": "symlink",
            "failFast": True,
        }
        self.config.update(config)

        required = {"root"}
        missing = required - set(self.config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        # Initialize LSST ingest software.
        butler = Butler(self.config["root"], writeable=True)
        ingest_class = doImport(self.config["ingest_task"])
        ingest_config = ingest_class.ConfigClass()
        ingest_config.transfer = self.config["transfer"]

        # TODO: A temporary hack to make the plugin usable with LSST stack
        #  prior to w_2021_05. Remove the if statement once a later version is
        #  in use.
        if self.config["failFast"] is not None:
            ingest_config.failFast = self.config["failFast"]

        ingest_config_overrides = ConfigOverrides()
        if self.config["config_file"] is not None:
            ingest_config_overrides.addFileOverride(self.config["config_file"])
        if self.config["config"] is not None:
            for key, val in self.config["config"].items():
                ingest_config_overrides.addValueOverride(key, val)
        ingest_config_overrides.applyTo(ingest_config)
        self.task = ingest_class(config=ingest_config, butler=butler)

        # Retrieve the version of the LSST ingest software in use.
        self._version = "N/A"
        pkg_name = ".".join(self.config["ingest_task"].split(".")[:-1])
        pkg = sys.modules[pkg_name]
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
        with lsst.log.UsePythonLogging():
            self.task.run([filename],
                          run=self.config["output_run"], processes=1)
