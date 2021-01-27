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
import importlib
import inspect
import jsonschema
import logging
import click
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from .backfill import Backfill
from .. import validation
from ..utils import (
    dump_config,
    dump_env,
    setup_logging,
    find_missing_tables,
    fully_qualify_tables,
)


logger = logging.getLogger(__name__)


@click.group()
def backfill():
    """Populate the database with entries for historical files.
    """
    pass


@backfill.command()
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
        schema = yaml.safe_load(validation.backfill)
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
    insp = inspect(engine)

    # Save tablenames for future reference making sure the schema is always
    # explicitly specified (needed to properly define ORMs later on).
    tablenames = fully_qualify_tables(insp, dict(config["tablenames"]))

    logger.info("checking if required database table exists...")
    try:
        missing = find_missing_tables(insp, list(tablenames.values()),
                                      skip_default=True)
    except Exception as ex:
        msg = f"{ex}"
        logger.error(msg)
        raise RuntimeError(msg)
    else:
        if missing:
            msg = f"table(s) {', '.join(missing)} not found in the database."
            logger.error(msg)
            raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info("setting up Backfill...")
    config = configuration["backfill"]

    # Create Backfill specific configuration. It is initialized with
    # settings from relevant section of the global configuration, but new
    # settings may be added, already existing ones may be altered.
    backfill_config = dict(config)

    backfill_config["session"] = session
    backfill_config["tablenames"] = tablenames

    logger.info("starting Backfill...")
    component = Backfill(backfill_config)
    component.run()
