import argparse
import importlib
import jsonschema
import logging
import os
import threading
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import python.lsst.dbb.buffmngrs.endpoint as mgr


logger = logging.getLogger("lsst.dbb.buffmngrs.handoff")


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


def main():
    args = parse_args()

    # Read provided configuration or use the default one.
    if args.config is None:
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        filename = os.path.normpath(os.path.join(root, "etc/config.yaml"))
    else:
        filename = args.config
    with open(filename, "r") as f:
        config = yaml.safe_load(f)

    # Validate configuration, if requested.
    if args.validate:
        config_schema = yaml.safe_load(mgr.schema)
        try:
            jsonschema.validate(instance=config, schema=config_schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"Configuration error: {ex.message}.")

    # Set up a logger.
    logger_options = config.get("logging", None)
    set_logger(options=logger_options)
    logger.info(f"Configuration read from '{filename}'.")

    # Create a connection to the database.
    cfg = config["database"]
    engine = create_engine(cfg["engine"], echo=cfg["echo"])
    Session = sessionmaker(bind=engine)
    session = Session()

    # Start daemons monitoring directories where new files may appear.
    module = importlib.import_module(
        "python.lsst.dbb.buffmngrs.endpoint.actions")
    for cfg in config["finders"]:
        cfg["session"] = session

        # Configure standard and alternative file actions.
        for type_, name in cfg["actions"].items():
            try:
                class_ = getattr(module, name)
            except AttributeError as ex:
                msg = f"Unknown file action: '{name}'."
                logger.error(msg)
                raise RuntimeError(msg)
            else:
                action_config = {}
                if name == "Move":
                    action_config["src"] = cfg["buffer"]
                    action_config["dst"] = cfg["storage"]
                try:
                    action = class_(action_config)
                except ValueError as ex:
                    msg = f"{class_.__name__}: invalid configuration: {ex}."
                    raise RuntimeError(msg)
                cfg[type_] = action

        finder = mgr.Finder(cfg)
        t = threading.Thread(target=finder.run, daemon=True)
        t.start()

    # Start daemons responsible for ingesting images to the database systems.
    module = importlib.import_module(
        "python.lsst.dbb.buffmngrs.endpoint.plugins")
    for cfg in config["ingesters"]:
        cfg["session"] = session

        # Configure ingest plugin.
        plugin_name = cfg["plugin"]["name"]
        plugin_config = cfg["plugin"]["config"]
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
            cfg["plugin"] = plugin

        ingester = mgr.Ingester(cfg)
        t = threading.Thread(target=ingester.run, daemon=True)
        t.start()

    while True:
        pass


if __name__ == "__main__":
    main()
