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

from lsst.log import UsePythonLogging
from lsst.daf.butler import Butler, DatasetRef
from lsst.obs.base.utils import getInstrument
from lsst.pipe.base.configOverrides import ConfigOverrides
from lsst.utils import doImport

from ..abcs import Plugin
from ..utils import get_version


__all__ = ["NullIngest", "Gen2Ingest", "Gen3Ingest"]


logger = logging.getLogger(__name__)


class NullIngest(Plugin):
    """Do nothing.

    A null object, a no-op.
    """

    def __init__(self, config):
        pass

    def execute(self, filename):
        pass

    def version(self):
        return ""


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

    defaults = {
        "create": False,
        "dryrun": False,
        "ignoreIngested": False,
        "mode": "link",
        "task": "lsst.pipe.tasks.ingest.IngestTask"
    }

    def __init__(self, config):
        try:
            butler_config = config["butler"]
        except KeyError:
            msg = "invalid configuration: Butler settings are missing"
            logger.error(msg)
            raise ValueError(msg)

        self.config = {**self.defaults, **butler_config}
        self.config.update(config.get("ingest", {}))

        required = {"root"}
        missing = required - set(self.config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.error(msg)
            raise ValueError(msg)

        # Initialize LSST ingest software.
        task_class = doImport(self.config["task"])
        task_config = {key: val for key, val in self.config.items()
                       if key != "task"}
        with UsePythonLogging():
            self.task = task_class.prepareTask(**task_config)

        self._version = get_version(self.config["task"])

    def execute(self, path):
        """Make an attempt to ingest the file.

        Parameters
        ----------
        path : `str`
            Path to the file.
        """
        with UsePythonLogging():
            self.task.ingestFiles(path)

    def version(self):
        """Return the version of the LSST ingest task.

        Returns
        -------
        ver : `str`
            Version of the LSST ingest task used for the ingest attempt.
        """
        return self._version


class Gen3Ingest(Plugin):
    """Ingest a file to Gen3 Butler repository.

    Depending on its configuration, the plugin will also define a visit for
    the ingested file.

    Parameters
    ----------
    config : `dict`
        Plugin configuration.

    Raises
    ------
    ValueError
        If a required configuration setting is missing.
    """

    def __init__(self, config):
        try:
            butler_config = config["butler"]
        except KeyError:
            msg = "invalid configuration: Butler settings are missing"
            logger.error(msg)
            raise ValueError(msg)

        self._plugins = []

        # Initialize LSST data access interface.
        required = {"root"}
        missing = required - set(butler_config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.error(msg)
            raise ValueError(msg)

        collection = butler_config.get("collection", None)
        butler = Butler(butler_config["root"],
                        collections=collection, writeable=True)

        # Configure and register the LSST task responsible for ingesting raws.
        task_config = config.get("ingest", {})
        if collection is not None:
            task_config["output_run"] = collection
        self.register(Gen3RawIngestPlugin(task_config, butler))

        # Optionally, configure and register the LSST task responsible for
        # defining visits.
        task_config = config.get("visit", {})
        if task_config:
            if "instrument" not in task_config:
                msg = "invalid configuration: instrument not specified"
                logger.error(msg)
                raise ValueError(msg)
            if collection is not None:
                task_config["collections"] = collection
            self.register(Gen3DefineVisitsPlugin(task_config, butler))

        self._version = self._plugins[0].version()
        self._results = None

    def execute(self, data):
        """Execute the plugins in the sequence.

        The plugins are executed in the order they were registered.

        Parameters
        ----------
        data : Any
            The input data required by the first plugin in the sequence.
        """
        for plugin in self._plugins:
            data = plugin.execute(data)

    def register(self, plugin):
        """Add an operation to the sequence.

        Parameters
        ----------
        plugin : Plugin
            A plugin

        Raises
        ------
        ValueError
            If the input argument is not a Plugin class instance.
        """
        if not isinstance(plugin, Plugin):
            msg = f"{plugin} is not a 'Plugin' instance"
            logger.error(msg)
            raise TypeError(msg)
        self._plugins.append(plugin)

    def version(self):
        """Retrieve the version of the LSST software in use.

        Returns
        -------
        `str`
            Version of the LSST software in use.  More specifically,
            the version of the first LSST task in the sequence.
        """
        return self._version


class Gen3RawIngestPlugin(Plugin):
    """Plugin responsible for ingesting images to a Gen3 Butler repository.

    The LSST Gen3 Middleware supports parallel ingestion of multiple files.
    However, it does not expose information which files were ingested
    successfully and which failed to ingest.  Hence, the plugin is
    deliberately restricted to ingesting one file at the time.  To enable
    concurrent ingests, use Ingester's ``num_threads`` setting instead.

    Parameters
    ----------
    config : `dict`
        Configuration of the plugin.
    butler : `lsst.daf.butler.Butler`
        LSST data access interface.

    Notes
    -----
    The code below is essentially a customized version of
    `script/ingestRaws.py`` from ``lsst.obs.base`` package.
    """

    # Default values for configuration settings.
    #
    # Parallel ingestion is disabled, ``pool`` and ``processes`` settings
    # will be ignored and are included only for sake of completeness.
    defaults = {
        "config": None,
        "config_file": None,
        "output_run": None,
        "pool": None,
        "processes": 1,
        "task": "lsst.obs.base.RawIngestTask",
        "transfer": "symlink",
    }

    def __init__(self, config, butler):
        self.config = {**self.defaults, **config}

        task_class = doImport(self.config["task"])

        task_config = task_class.ConfigClass()
        task_config.transfer = self.config["transfer"]

        task_config_overrides = ConfigOverrides()
        if self.config["config_file"] is not None:
            task_config_overrides.addFileOverride(self.config["config_file"])
        if self.config["config"] is not None:
            for key, val in self.config["config"].items():
                task_config_overrides.addValueOverride(key, val)
        task_config_overrides.applyTo(task_config)
        self.task = task_class(config=task_config,
                               butler=butler,
                               on_success=self._handle_success,
                               on_metadata_failure=self._handle_failure,
                               on_ingest_failure=self._handle_failure)

        self._version = get_version(self.config["task"])
        self._result = None

    def execute(self, path):
        """Ingest a file to a Gen3 Butler dataset repository.

        Parameters
        ----------
        path : `str`
            Path to the file.

        Returns
        -------
        result : `list` [`lsst.daf.bulter.DatasetRef`]
            Dataset references for ingested raws.
        """
        data = [path]
        with UsePythonLogging():
            result = self.task.run(data,
                                   run=self.config["output_run"],
                                   pool=None, processes=1)
        return result

    def version(self):
        """Retrieve the version of the LSST task used for ingesting raws.

        Returns
        -------
        version : `str`
            Version of the LSST ingest task responsible for ingesting raws.
        """
        return self._version

    def _handle_success(self, data):
        """Extract data about the file which was successfully ingested.

        Parameters
        ----------
        data : `lsst.daf.butler.FileDataset`
            A data structure representing the ingested dataset.
        """
        self._result = data

    def _handle_failure(self, data, exc):
        """Re-raise the exception encountered during a failed ingest attempt.

        Parameters
        ----------
        data : `lsst.daf.butler.ButlerURI` or `lsst.daf.butler.RawExposureData`
            A Butler data structure received after the failed ingest attempts.
        exc : Exception
            The exception raised during the ingest attempt.

        Raises
        ------
        exc : Exception
            The exception raised during the failed ingestion attempt.
        """
        self._result = data
        raise exc


class Gen3DefineVisitsPlugin(Plugin):
    """Plugin responsible for defining visits in Gen3 Butler repository.

    Parameters
    ----------
    config : `dict`
        Configuration of the plugin.
    butler : `lsst.daf.butler.Butler`
        LSST data access interface.

    Notes
    -----
    The code below is essentially a customized version of
    `script/defineVisits.py`` from ``lsst.obs.base`` package.
    """

    # Default values for configuration settings.
    #
    # The ``pool`` and ``processes`` settings will be ignored and are included
    # only for sake of completeness.
    defaults = {
        "config_file": None,
        "collections": None,
        "pool": None,
        "processes": 1,
        "task": "lsst.obs.base.DefineVisitsTask"
    }

    def __init__(self, config, butler):
        self.config = {**self.defaults, **config}

        task_class = doImport(self.config["task"])
        task_config = task_class.ConfigClass()

        instr = getInstrument(self.config["instrument"], butler.registry)
        instr.applyConfigOverrides(task_class._DefaultName, task_config)
        if self.config["collections"] is None:
            self.config["collections"] = instr.makeDefaultRawIngestRunName()

        if self.config["config_file"] is not None:
            task_config.load(self.config["config_file"])
        self.task = task_class(config=task_config, butler=butler)

        self._version = get_version(self.config["task"])

    def execute(self, refs):
        """Add visit definition to the registry for the given exposure.

        Parameters
        ----------
        refs : `list` [`lsst.daf.butler.DatasetRef`]
            DataIds of the file for which the visit needs to be defined.

        Returns
        -------
        refs : `list` [`lsst.daf.butler.DatasetRef`]
            The references to the datasets it received.
        """
        data = [ref.dataId for ref in refs]
        with UsePythonLogging():
            self.task.run(data,
                          collections=self.config["collections"],
                          pool=None, processes=1)
        return refs

    def version(self):
        """Retrieve the version of the LSST task used for defining visits.

        Returns
        -------
        ver : `str`
            Version of the LSST task responsible for defining visits.
        """
        return self._version
