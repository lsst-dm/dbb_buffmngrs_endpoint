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
import click
import importlib
import inspect
import jsonschema
import logging
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from .ingester import Ingester
from .. import validation
from ..utils import setup_logger


logger = logging.getLogger(__name__)


@click.group()
def ingester():
    pass


@ingester.command()
@click.option("-v", "--validate", is_flag=True, default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, validate):
    """Starts an ingester using a configuration from FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(validation.ingester)
        try:
            jsonschema.validate(instance=configuration, schema=schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"Configuration error: {ex.message}.")
        except jsonschema.SchemaError as ex:
            raise ValueError(f"Schema error: {ex.message}.")

    config = configuration.get("logging", None)
    setup_logger(logging.getLogger(), options=config)

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

    logger.info("checking if required database tables exist...")
    required = {table for table in config["tablenames"].values()}
    available = set(inspect(engine).get_table_names())
    missing = required - available
    if missing:
        msg = f"table(s) {', '.join(missing)} not found in the database"
        logger.error(msg)
        raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    mapper = config["tablenames"]

    logger.info("setting up Ingester...")
    config = configuration["ingester"]
    config["session"] = session
    config["orms"] = mapper

    # Configure ingest plugin.
    package_name = "lsst.dbb.buffmngrs.endpoint.ingester"
    module = importlib.import_module(".plugins", package=package_name)
    plugin_name = config["plugin"]["name"]
    plugin_config = config["plugin"]["config"]
    try:
        class_ = getattr(module, plugin_name)
    except AttributeError as ex:
        msg = f"Unknown ingest plugin '{plugin_name}'."
        logger.error(msg)
        raise RuntimeError(msg)
    else:
        try:
            plugin = class_(plugin_config)
        except ValueError as ex:
            msg = f"{class_.__name__}: invalid configuration: {ex}."
            raise RuntimeError(msg)
        config["plugin"] = plugin

    logger.info("starting Ingester...")
    ingester = Ingester(config)
    ingester.run()
