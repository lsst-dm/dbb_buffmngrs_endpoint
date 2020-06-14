import click
import importlib
import inspect
import jsonschema
import logging
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from .ingester import Ingester
from .. import schemas
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
        schema = yaml.safe_load(schemas.ingester)
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
    engine = create_engine(config["engine"], echo=config.get("echo", False))

    logger.info("checking if required tables exist...")
    required = {table for table in config["orms"].values()}
    available = set(inspect(engine).get_table_names())
    missing = required - available
    if missing:
        msg = f"table(s) {', '.join(missing)} not found in the database"
        logger.error(msg)
        raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    mapper = config["orms"]

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
