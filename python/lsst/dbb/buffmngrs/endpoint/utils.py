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
import subprocess
import yaml


__all__ = ["dump_config", "dump_env", "setup_logger"]


def setup_logger(logger, options=None):
    """Configure logger.

    Parameters
    ----------
    logger : logging.Logger
        A logger to set up.
    options : dict, optional
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.

    Returns
    -------
    `logging.Logger`
        A root logger to use.
    """
    # Define default settings for the logger. They will be overridden with
    # values found in 'options', if specified.
    settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "INFO",
    }
    if options is not None:
        settings.update(options)

    level_name = settings["level"]
    level = getattr(logging, level_name.upper(), logging.WARNING)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    logfile = settings["file"]
    if logfile is not None:
        handler = logging.FileHandler(logfile)
    logger.addHandler(handler)

    fmt = settings["format"]
    formatter = logging.Formatter(fmt=fmt, datefmt=None)
    handler.setFormatter(formatter)
    return logger


def dump_config(config):
    """Dump the configuration to YAML format.

    Parameters
    ----------
    config : `dict`
        Configuration to dump.

    Returns
    -------
    `str`
        Configuration expressed in YAML format.
    """
    return yaml.dump(config, default_flow_style=False)


def dump_env():
    """Dump runtime LSST environment.

    Returns
    -------
    `str`
        Runtime LSST environment in a text format.
    """
    process = subprocess.run(["eups", "list", "--setup"],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True)
    return process.stdout
