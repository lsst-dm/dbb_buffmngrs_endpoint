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
import click
import yaml
from .ingester import Ingester
from ..utils import dump_all, setup_connection, setup_logging, validate_config
from ..validation import INGESTER


logger = logging.getLogger(__name__)


@click.group()
def ingester():
    """Manage file ingestion to a data management system.
    """
    pass


@ingester.command()
@click.option("--dump/--no-dump", default=True,
              help="Log runtime environment and configuration "
                   "(ignored if severity is set to WARNING and higher).")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, dump):
    """Starts an ingester using a configuration from FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)

    config = configuration.get("logging", None)
    setup_logging(options=config)

    if dump:
        logger.info(dump_all(configuration))

    logger.info("setting up database connection...")
    config = configuration["database"]
    try:
        session, tablenames = setup_connection(config)
    except RuntimeError as ex:
        logger.error(ex)
        raise RuntimeError(ex) from ex

    logger.info("setting up Ingester...")
    config = configuration["ingester"]

    # Create Ingester specific configuration. It is initialized with
    # settings from relevant section of the global configuration, but new
    # settings may be added, already existing ones may be altered.
    ingester_config = dict(config)
    ingester_config["session"] = session
    ingester_config["tablenames"] = tablenames

    # Configure ingest plugin.
    package_name = "lsst.dbb.buffmngrs.endpoint.ingester"
    module = importlib.import_module(".plugins", package=package_name)
    plugin_name = config["plugin"]["name"]
    try:
        class_ = getattr(module, plugin_name)
    except AttributeError as ex:
        msg = f"Unknown ingest plugin '{plugin_name}'."
        logger.error(msg)
        raise RuntimeError(msg)
    ingester_config["plugin"]["class"] = class_

    logger.info("starting Ingester...")
    component = Ingester(ingester_config)
    component.run()


@ingester.command()
@click.argument("filename", type=click.Path(exists=True))
def validate(filename):
    """Validate configuration in the FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    schema = yaml.safe_load(INGESTER)
    validate_config(configuration, schema)
