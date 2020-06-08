#!/usr/bin/env python
import argparse
import importlib
import jsonschema
import logging
import os
import multiprocessing
import time
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
import python.lsst.dbb.buffmngrs.endpoint as mgr


logger = logging.getLogger("python.lsst.dbb.buffmngrs.endpoint")


def parse_args():
    """Parse command line arguments.

    Returns
    -------
    argparse.Namespace
        A namespace populated with arguments and their values.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default=None,
                        help="configuration file in YAML format")
    parser.add_argument("-v", "--validate", action="store_true", default=False,
                        help="validate configuration file")
    return parser.parse_args()


def set_logger(options=None):
    """Configure logger.

    Parameters
    ----------
    options : dict, optional
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.
    """
    # Define default settings for the logger. They will be overridden with
    # values found in 'options', if specified.
    settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "WARNING",
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


def create_finders(configs):
    """Create services responsible for finding new images.

    Parameters
    ----------
    configs : `list` of `dict`
        List of configurations. For each provided configuration, a separate
        ingest process will be created.

    Returns
    -------
    `list` of `multiprocessing.Process`
        A sequence of processes which will keep discovering new images
        at given locations.
    """
    module_name = "lsst.dbb.buffmngrs.endpoint.actions"
    module = importlib.import_module(module_name)

    processes = []
    for config in configs:

        # Configure standard and alternative file actions.
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

        finder = mgr.Finder(config)
        p = multiprocessing.Process(target=finder.run)
        processes.append(p)

    return processes


def create_ingesters(configs):
    """Create services responsible for ingesting images.

    Parameters
    ----------
    configs : `list` of `dict`
        List of configurations. For each provided configuration, a separate
        ingest process will be created.

    Returns
    -------
    `list` of `multiprocessing.Process`
        A sequence of processes which will keep ingesting images to given
        database systems once started.
    """
    module_name = "lsst.dbb.buffmngrs.endpoint.plugins"
    module = importlib.import_module(module_name)

    processes = []
    for config in configs:

        # Configure ingest plugin.
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

        ingester = mgr.Ingester(config)
        p = multiprocessing.Process(target=ingester.run)
        processes.append(p)

    return processes


def main():
    """Entry point.
    """
    args = parse_args()

    # Read provided configuration or use the default one.
    if args.config is None:
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        filename = os.path.normpath(os.path.join(root, "etc/config.yaml"))
    else:
        filename = args.config
    with open(filename, "r") as f:
        master_config = yaml.safe_load(f)

    # Validate configuration, if requested.
    if args.validate:
        config_schema = yaml.safe_load(mgr.schema)
        try:
            jsonschema.validate(instance=master_config, schema=config_schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"Configuration error: {ex.message}.")

    # Set up a logger.
    logger_options = master_config.get("logging", None)
    set_logger(options=logger_options)
    logger.info(f"Configuration read from '{filename}'.")

    # Establish connection with the database and check if required tables
    # exists.
    config = master_config["database"]
    engine = create_engine(config["engine"], echo=config["echo"])

    required = {"files", "statuses", "attempts"}
    available = set(inspect(engine).get_table_names())
    missing = required - available
    if missing:
        msg = f"Table(s) {', '.join(missing)} not found in the database."
        logger.error(msg)
        raise RuntimeError(msg)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Configure micro services responsible for finding new images and ingesting
    # them to different database systems.
    services = []

    logger.info("Configuring Finders...")
    configs = master_config.get("finders", None)
    if configs is not None:
        for config in configs:
            config["session"] = session
        finders = create_finders(configs)
        services.extend(finders)

    logger.info("Configuring Ingesters...")
    configs = master_config.get("ingesters", None)
    if configs is not None:
        for config in configs:
            config["session"] = session
        ingesters = create_ingesters(configs)
        services.extend(ingesters)
    else:
        msg = "Service requires at least one ingester, none provided."
        logger.error(msg)
        raise ValueError(msg)

    # Start all configured micro services.
    try:
        logger.info("Initializing service...")
        for s in services:
            s.start()
        logger.info("Initialization completed, service is up and running.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested...exiting.")
        for s in services:
            s.terminate()
    finally:
        for s in services:
            s.join()


if __name__ == "__main__":
    main()
