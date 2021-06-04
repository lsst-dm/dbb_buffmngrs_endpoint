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
"""Command line interface for the Backfill component.
"""
import logging
import click
import yaml
import importlib
from .consistency_check import Consistency_check
from ..utils import dump_all, setup_connection, setup_logging, validate_config
from ..validation import BACKFILL


logger = logging.getLogger(__name__)


@click.group()
def consistency_check():
    """Compare the storage area with the repository
    """
    pass


@consistency_check.command()
@click.option("--dump/--no-dump", default=True,
              help="Log runtime environment and configuration "
                   "(ignored if severity is set to WARNING and higher).")
@click.option("--validate/--no-validate", default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, dump, validate):
    """Starts a backfill using a configuration from FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(BACKFILL)
        validate_config(configuration, schema)
        return

    config = configuration.get("logging", None)
    setup_logging(options=config)

    if dump:
        logger.info(dump_all(configuration))

    logger.info("setting up Consistency check...")
    config = configuration["consistency"]
    try:
        session, tablenames = setup_connection(config)
    except RuntimeError as ex:
        logger.error(ex)
        raise RuntimeError(ex)

    logger.info("setting up Consistency Check...")
    config = configuration["consistency"]
    # Detect and validate the consistency sources

    consistency_config = dict(config)
    consistency_config["session"] = session
    consistency_config["tablenames"] = tablenames

    # Configure ingest plugin.
    package_name = "lsst.dbb.buffmngrs.endpoint.consistency_check"
    module = importlib.import_module(".plugins", package=package_name)
    plugin_name = config["plugin"]["name"]
    try:
        class_ = getattr(module, plugin_name)
    except AttributeError as ex:
        msg = f"Unknown ingest plugin '{plugin_name}'."
        logger.error(msg)
        raise RuntimeError(msg)
    consistency_config["plugin"]["class"] = class_

    logger.info("starting Consistency Check...")
    component = Consistency_check(consistency_config)
    component.run()