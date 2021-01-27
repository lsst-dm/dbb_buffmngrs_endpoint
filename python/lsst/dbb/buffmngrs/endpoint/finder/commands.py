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
import click
import importlib
import inspect
import jsonschema
import logging
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from .finder import Finder
from .. import validation
from ..utils import dump_config, dump_env, setup_logging

MONITOR = 1

if MONITOR:
    from ..DbbMonitor import DbbMonitor
    DbbM = DbbMonitor()
    import datetime

logger = logging.getLogger(__name__)


@click.group()
def finder():
    """A group that all commands controlling Finder are attached to.
    """
    pass


@finder.command()
@click.option("--dump/--no-dump", default=True,
              help="Log runtime environment and configuration "
                   "(ignored if severity is set to WARNING and higher).")
@click.option("--validate/--no-validate", default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, dump, validate):
    """Starts a finder using a configuration from FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(validation.finder)
        try:
            jsonschema.validate(instance=configuration, schema=schema)
        except jsonschema.ValidationError as ex:
            msg = f"configuration error: {ex.message}."
            logger.error(msg)
            raise ValueError(msg)
        except jsonschema.SchemaError as ex:
            msg = f"schema error: {ex.message}."
            logger.error(msg)
            raise ValueError(msg)
        return

    config = configuration.get("logging", None)
    setup_logging(options=config)

    if dump:
        msg = "runtime environment and configuration:\n\n"
        msg += dump_env()
        msg += "\n"
        msg += dump_config(configuration)
        logger.info(msg)

    logger.info("setting up database connection...")
    config = configuration["database"]

    module = importlib.import_module("sqlalchemy.pool")
    pool_name = config.get("pool_class", "QueuePool")
    try:
        class_ = getattr(module, pool_name)
    except AttributeError:
        msg = f"unknown connection pool type: {pool_name}"
        logger.error(msg)
        raise RuntimeError(msg)

    engine = create_engine(config["engine"],
                           echo=config.get("echo", False),
                           poolclass=class_)

    logger.info("checking if required database table exists...")
    required = set(config["tablenames"].values())
    try:
        available = set(inspect(engine).get_table_names())
    except Exception as ex:
        msg = f"{ex}"
        logger.error(msg)
        raise RuntimeError(msg)
    else:
        missing = required - available
        if missing:
            msg = f"table(s) {', '.join(missing)} not found in the database."
            logger.error(msg)
            raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    mapper = config["tablenames"]

    logger.info("setting up Finder...")
    config = configuration["finder"]

    # Create Finder specific configuration. It is initialized with
    # settings from relevant section of the global configuration, but new
    # settings may be added, already existing ones may be altered.
    finder_config = dict(config)

    finder_config["session"] = session
    finder_config["tablenames"] = mapper

    # Set up standard and alternative file actions.
    package_name = "lsst.dbb.buffmngrs.endpoint.finder"
    module = importlib.import_module(".actions", package=package_name)
    for type_, name in config["actions"].items():
        if name is None:
            name = "Null"
        try:
            class_ = getattr(module, name)
        except AttributeError as ex:
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
    if MONITOR:
        mon_start = datetime.datetime.utcnow()

    component.run()

    if MONITOR:
        mon_end = datetime.datetime.utcnow()
        mon_tags = {
            "finderTimeTag": mon_start
        }
        mon_fields = {
            "FinderStart": mon_start,
            "FinderEnd": mon_end,
            "FinderDuration": mon_end-mon_start
        }
        DbbM.report_point("finderElapsed", mon_tags, mon_fields)
