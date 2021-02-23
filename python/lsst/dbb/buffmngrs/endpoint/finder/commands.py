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
"""Command line interface for the Finder component.
"""
import importlib
import logging
import click
import yaml
from .finder import Finder
from ..utils import dump_all, setup_connection, setup_logging, validate_config
from ..validation import FINDER


logger = logging.getLogger(__name__)


@click.group()
def finder():
    """Manage file discovery at a specified location.
    """


@finder.command()
@click.option("--dump/--no-dump", default=True,
              help="Log runtime environment and configuration "
                   "(ignored if severity is set to WARNING and higher).")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, dump):
    """Starts a finder using a configuration from FILENAME.
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

    logger.info("setting up Finder...")
    config = configuration["finder"]

    # Create Finder specific configuration. It is initialized with
    # settings from relevant section of the global configuration, but new
    # settings may be added, already existing ones may be altered.
    finder_config = dict(config)
    finder_config["session"] = session
    finder_config["tablenames"] = tablenames

    # Set up standard and alternative file actions.
    package_name = "lsst.dbb.buffmngrs.endpoint.finder"
    module = importlib.import_module(".actions", package=package_name)
    for type_, name in config["actions"].items():
        if name is None:
            name = "Noop"
        try:
            class_ = getattr(module, name)
        except AttributeError:
            msg = f"Unknown file action: '{name}'."
            logger.error(msg)
            raise RuntimeError(msg)
        else:
            action_config = {}
            if name == "Move":
                action_config["src"] = config["source"]
                action_config["dst"] = config["storage"]
            try:
                action = class_(action_config)
            except ValueError as ex:
                msg = f"{class_.__name__}: invalid configuration: {ex}."
                raise RuntimeError(msg)
            finder_config[type_] = action

    logger.info("starting Finder...")
    component = Finder(finder_config)
    component.run()


@finder.command()
@click.argument("filename", type=click.Path(exists=True))
def validate(filename):
    """Validate configuration in the FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    schema = yaml.safe_load(FINDER)
    validate_config(configuration, schema)
