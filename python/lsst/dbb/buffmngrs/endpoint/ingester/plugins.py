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
from lsst.daf.butler import Butler
from lsst.pipe.base import Instrument
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
    KeyError
        If root directory of the Butler repository is not provided.

    Notes
    -----
    See ``lsst.pipe.tasks.ingest`` for details.
    """

    _defaults = {
        "create": False,
        "dryrun": False,
        "ignoreIngested": False,
        "mode": "link",
        "task": "lsst.pipe.tasks.ingest.IngestTask"
    }

    def __init__(self, config):
        try:
            butler_config = config["butler"]
        except KeyError as exc:
            msg = "invalid configuration: 'butler' section is missing"
            logger.error(msg)
            raise KeyError(msg) from exc

        self._config = {**self._defaults, **butler_config}
        self._config.update(config.get("ingest", {}))

        required = {"root"}
        missing = required - set(self._config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided."
            logger.error(msg)
            raise KeyError(msg)

        # Initialize LSST ingest software.
        task_class = doImport(self._config["task"])
        task_config = {key: val for key, val in self._config.items()
                       if key != "task"}
        with UsePythonLogging():
            self.task = task_class.prepareTask(**task_config)

        # Save the LSST ingest software version for faster lookup as it will
        # be used during every ingest attempt.
        self._version = get_version(self._config["task"])

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
    KeyError
        If a required configuration setting is missing.
    """

    def __init__(self, config):
        try:
            butler_config = config["butler"]
        except KeyError as exc:
            msg = "invalid configuration: 'butler' section is missing"
            logger.error(msg)
            raise KeyError(msg) from exc

        self._plugins = []

        # Initialize LSST data access interface.
        required = {"root"}
        missing = required - set(butler_config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.error(msg)
            raise KeyError(msg)

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
        task_config = config.get("visits", {})
        if task_config:
            if "instrument" not in task_config:
                msg = "invalid configuration: instrument not specified"
                logger.error(msg)
                raise KeyError(msg)
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
        TypeError
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
    _defaults = {
        "config": None,
        "config_file": None,
        "output_run": None,
        "pool": None,
        "processes": 1,
        "file_filter": r"\.fit[s]?\b",
        "group_files": True,
        "skip_existing_exposures": False,
        "update_exposure_records": False,
        "track_file_attrs": True,
        "task": "lsst.obs.base.RawIngestTask",
        "transfer": "direct",
    }

    def __init__(self, config, butler):
        self._config = {**self._defaults, **config}

        task_class = doImport(self._config["task"])

        task_config = task_class.ConfigClass()
        task_config.transfer = self._config["transfer"]

        task_config_overrides = ConfigOverrides()
        if self._config["config_file"] is not None:
            task_config_overrides.addFileOverride(self._config["config_file"])
        if self._config["config"] is not None:
            for key, val in self._config["config"].items():
                task_config_overrides.addValueOverride(key, val)
        task_config_overrides.applyTo(task_config)
        self.task = task_class(config=task_config,
                               butler=butler,
                               on_success=self._handle_success,
                               on_metadata_failure=self._handle_failure,
                               on_ingest_failure=self._handle_failure)

        self._version = get_version(self._config["task"])
        self._result = None

    def execute(self, data):
        """Ingest a file to a Gen3 Butler data repository.

        Parameters
        ----------
        data : `str`
            Path to the file.

        Returns
        -------
        result : `list` [`lsst.daf.butler.DatasetRef`]
            Dataset references for ingested raws.
        """
        with UsePythonLogging():
            result = self.task.run(
                [data],
                run=self._config["output_run"],
                pool=None,
                processes=1,
                file_filter=self._config["file_filter"],
                group_files=self._config["group_files"],
                skip_existing_exposures=self._config["skip_existing_exposures"],
                update_exposure_records=self._config["update_exposure_records"],
                track_file_attrs=self._config["track_file_attrs"]
            )
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

        It should be only be used only as the callback function in the
        LSST ingest software for handling successful ingest attempts.

        Parameters
        ----------
        data : `lsst.daf.butler.FileDataset`
            A data structure representing the ingested dataset.
        """
        self._result = data

    def _handle_failure(self, data, exc):
        """Re-raise the exception encountered during a failed ingest attempt.

        It should be only be used only as the callback function in the
        LSST ingest software for handling failed ingest attempts.

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
    _defaults = {
        "config_file": None,
        "collections": None,
        "update_records": False,
        "task": "lsst.obs.base.DefineVisitsTask",
    }

    def __init__(self, config, butler):
        self._config = {**self._defaults, **config}

        task_class = doImport(self._config["task"])
        task_config = task_class.ConfigClass()

        instr = Instrument.from_string(self._config["instrument"], butler.registry)
        instr.applyConfigOverrides(task_class._DefaultName, task_config)
        if self._config["collections"] is None:
            self._config["collections"] = instr.makeDefaultRawIngestRunName()

        if self._config["config_file"] is not None:
            task_config.load(self._config["config_file"])
        self.task = task_class(config=task_config, butler=butler)

        # Save the LSST ingest software version for faster lookup as it will
        # be used during every ingest attempt.
        self._version = get_version(self._config["task"])

    def execute(self, data):
        """Add visit definition to the registry for the given exposure.

        Parameters
        ----------
        data : `list` [`lsst.daf.butler.DatasetRef`]
            References to the datasets of the file for which the visit needs
            to be defined.

        Returns
        -------
        refs : `list` [`lsst.daf.butler.DatasetRef`]
            The references to the datasets it received.
        """
        ids = [ref.dataId for ref in data]
        with UsePythonLogging():
            self.task.run(ids,
                          collections=self._config["collections"],
                          update_records=self._config["update_records"])
        return data

    def version(self):
        """Retrieve the version of the LSST task used for defining visits.

        Returns
        -------
        ver : `str`
            Version of the LSST task responsible for defining visits.
        """
        return self._version
