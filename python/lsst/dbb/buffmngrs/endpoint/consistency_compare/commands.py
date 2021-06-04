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
from .consistency_compare import Consistency_compare
from ..utils import dump_all, setup_connection, setup_logging, validate_config
from ..validation import BACKFILL


logger = logging.getLogger(__name__)


@click.group()
def consistency_compare():
    """Compare the storage area with the repository
    """
    pass


@consistency_compare.command()
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

    # Detect and validate the consistency sources

    sourceA = config["storage_source_one"]
    sourceB = config["storage_source_two"]
    sourceA_settings = config["source_one_data"]
    sourceB_settings = config["source_two_data"]

    consistency_config = dict()
    consistency_config["sourceA"] = sourceA
    consistency_config["sourceB"] = sourceB
    consistency_config["sourceA_settings"] = sourceA_settings
    consistency_config["sourceB_settings"] = sourceB_settings

    logger.info("starting Consistency Check...")
    component = Consistency_compare(consistency_config)
    component.run()