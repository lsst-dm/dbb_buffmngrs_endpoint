import click
import importlib
import inspect
import jsonschema
import logging
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from .finder import Finder
from .. import schemas
from ..utils import setup_logger


logger = logging.getLogger(__name__)


@click.group()
def finder():
    pass


@finder.command()
@click.option("-v", "--validate", is_flag=True, default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def start(filename, validate):
    """Starts a finder using a configuration from FILENAME.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(schemas.finder)
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

    config = configuration.get("logging", None)
    setup_logger(logging.getLogger(), options=config)

    logger.info("setting up database connection...")
    config = configuration["database"]
    engine = create_engine(config["engine"], echo=config.get("echo", False))

    logger.info("checking if required table exists...")
    required = {table for table in config["orms"].values()}
    available = set(inspect(engine).get_table_names())
    missing = required - available
    if missing:
        msg = f"table(s) {', '.join(missing)} not found in the database."
        logger.error(msg)
        raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    mapper = config["orms"]

    logger.info("setting up Finder...")
    config = configuration["finder"]
    config["session"] = session
    config["orms"] = mapper

    # Set up standard and alternative file actions.
    package_name = "lsst.dbb.buffmngrs.endpoint.finder"
    module = importlib.import_module(".actions", package=package_name)
    for type_, name in config["actions"].items():
        try:
            class_ = getattr(module, name)
        except AttributeError as ex:
            msg = f"Unknown file action: '{name}'."
            logger.error(msg)
            raise RuntimeError(msg)
        else:
            action_config = {}
            if name == "Move":
                action_config["src"] = config["buffer"]
                action_config["dst"] = config["storage"]
            try:
                action = class_(action_config)
            except ValueError as ex:
                msg = f"{class_.__name__}: invalid configuration: {ex}."
                raise RuntimeError(msg)
            config[type_] = action

    logger.info("starting Finder...")
    finder = Finder(config)
    finder.run()
