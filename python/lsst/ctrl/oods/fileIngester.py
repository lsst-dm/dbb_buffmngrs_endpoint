# This file is part of ctrl_oods
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
import shutil
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from importlib import import_module


logger = logging.getLogger(__name__)


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.
    """

    def __init__(self, config):
        self.config = config
        self.scanner = DirectoryScanner(config)
        self.bad_file_dir = config["badFileDirectory"]

        butlerConfig = config["butler"]
        classConfig = butlerConfig["class"]

        # create the butler
        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.repo = butlerConfig["repoDirectory"]
        self.butler = butlerClass(self.repo)

    def run_task(self):
        """Scan to get the files, and ingest them in batches.
        """
        filenames = self.scanner.getAllFiles()
        for filename in filenames:
            try:
                self.butler.ingest(filename)
                logger.info(f"{filename} ingested successfully.")
            except Exception as e:
                err = f"{filename} could not be ingested. " \
                      f"Moving to {self.bad_file_dir}: {e}"
                logger.warning(err)
                shutil.move(filename, self.bad_file_dir)
