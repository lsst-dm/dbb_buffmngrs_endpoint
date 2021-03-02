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
"""Definitions of miscellaneous helper functions.
"""
import dataclasses
import hashlib
import importlib
import logging
import subprocess
from pathlib import Path

import jsonschema
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker


__all__ = [
    "FileInfo",
    "dump_all",
    "find_missing_tables",
    "fully_qualify_tables",
    "get_checksum",
    "get_file_attributes",
    "setup_connection",
    "setup_logging",
    "validate_config",
]


@dataclasses.dataclass()
class FileInfo:
    """Data structure encapsulating various file attributes.
    """

    dirname: str = None
    """Directory the file is in.
    """

    filename: str = None
    """Name of the file (both, the stem and the extension, if any).
    """

    checksum: str = None
    """Checksum of the file.
    """

    size: int = None
    """Size of the files expressed in bytes.
    """


def dump_config(config):
    """Dump the configuration to YAML format.

    Parameters
    ----------
    config : `dict`
        Configuration to dump.

    Returns
    -------
    `str`
        Configuration expressed in YAML format.
    """
    return yaml.dump(config, default_flow_style=False)


def dump_env():
    """Dump runtime LSST environment.

    Returns
    -------
    `str`
        Runtime LSST environment in a text format.
    """
    process = subprocess.run(["eups", "list", "--setup"],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True)
    return process.stdout


def dump_all(config):
    """Dump runtime information like environment and configuration.

    Parameters
    ----------
    config : `dict`
        Configuration to dump.

    Returns
    -------
    `str`
        A report capturing runtime information.
    """
    messages = [
        "runtime environment and configuration",
        "",
        dump_env(),
        dump_config(config),
    ]
    return "\n".join(messages)


def find_missing_tables(inspector, specifications, skip_default=False):
    """Determines which required tables are missing from the database.

    Parameters
    ----------
    inspector : `sqlalchemy.engine.reflection.Inspector`
        A proxy object allowing for database inspection.
    specifications : `list` [ `dict` ]
        A list of table specifications. Each specification must be a dictionary
        of the form:

            {
                "schema": "<schema_name>",
                "table": "<table_name>"
            }

        If a table resides in the default database schema, the ``"schema"``
        field can be None or omitted.
    skip_default : `bool`, optional
        If set, the names of tables in the default database schema will not
        be prefixed by it.

    Returns
    -------
    `set`
        A set with fully qualified names of tables which are missing.  If all
        tables were found, an emtpy set is returned instead.
    """
    default_schema = inspector.default_schema_name
    required = set()
    for spec in specifications:
        schema = spec.get("schema", default_schema)
        table = spec["table"]
        required.add((schema, table))
    available = set()
    for schema, _ in required:
        available.update((schema, table)
                         for table in inspector.get_table_names(schema=schema))
    missing = required - available
    return {t if skip_default and s == default_schema else ".".join([s, t])
            for s, t in missing}


def fully_qualify_tables(inspector, specifications):
    """Add schema to each table specification, if missing.

    Parameters
    ----------
    inspector : `sqlalchemy.engine.reflection.Inspector`
        A proxy object allowing for database inspection.
    specifications : `dict` [ `str', `dict` ]
        A mapping between object relational mappers (ORMs) and corresponding
        table specifications. Each specification must be a dictionary of
        the form:

            {
                "schema": "<schema_name>",
                "table": "<table_name>"
            }

        If a table resides in the default database schema, the ``"schema"``
        field can be either None or omitted.

    Returns
    -------
    `dict` [ `str`, `dict` ]
        The mapping of between ORMs and table specifications including
        the schema in which table resides.
    """
    default_schema = inspector.default_schema_name
    for spec in specifications.values():
        if spec.setdefault("schema", default_schema) is None:
            spec["schema"] = default_schema
    return specifications


def get_checksum(path, method='blake2', block_size=4096):
    """Calculate checksum for a file using BLAKE2 cryptographic hash function.

    Parameters
    ----------
    path : `str`
        Path to the file.
    method : `str`
        An algorithm to use for calculating file's hash. Supported algorithms
        include:
        * _blake2_: BLAKE2 cryptographic hash,
        * _md5_: traditional MD5 algorithm,
        * _sha1_: SHA-1 cryptographic hash.
        By default or if unsupported method is provided, BLAKE2 algorithm wil
        be used.
    block_size : `int`, optional
        Size of the block

    Returns
    -------
    `str`
        File's hash calculated using a given method.
    """
    methods = {
        'blake2': hashlib.blake2b,
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
    }
    hasher = methods.get(method, hashlib.blake2b)()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_file_attributes(path, checksum=True, start=None):
    """Gather information about a file.

    Parameters
    ----------
    path : `str`, `os.PathLike`
        Path to the file. It can be either absolute or relative.
    checksum : `bool`, optional
        If set (default), file's checksum will be calculated.  If not set,
        the checksum will be set to None.
    start : `str`, `os.PathLike`, optional
        If note None (default), or from an optional start directory

    Returns
    -------
    `FileInfo`
        Gathered file attributes grouped into a handy bundle.
    """
    path = Path(path).resolve()
    digest = get_checksum(path) if checksum is True else None
    status = path.stat()
    if start is not None:
        path = path.relative_to(start)
    return FileInfo(dirname=str(path.parent),
                    filename=path.name,
                    checksum=digest,
                    size=status.st_size)


def setup_logging(options=None):
    """Configure logger.

    Parameters
    ----------
    options : dict, optional
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.

    Returns
    -------
    `logging.Logger`
        A root logger to use.
    """
    # Define default settings for the logger. They will be overridden with
    # values found in 'options', if specified.
    settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "INFO",
    }
    if options is not None:
        settings.update(options)

    kwargs = {"format": settings["format"]}

    level_name = settings["level"]
    level = getattr(logging, level_name.upper(), logging.WARNING)
    kwargs["level"] = level

    logfile = settings["file"]
    if logfile is not None:
        kwargs["filename"] = logfile

    logging.basicConfig(**kwargs)


def setup_connection(config):
    """Create a database connection.

    Parameters
    ----------
    config : `dict`
        Configuration describing the details of the database connection.

    Returns
    -------
    session : `sqlalchemy.orm.session.Session`
        Object managing persistence operations for ORMs.
    tablenames : `dict`
        Table specifications needed to properly define ORMs later on.
    """
    module = importlib.import_module("sqlalchemy.pool")
    pool_name = config.get("pool_class", "QueuePool")
    try:
        class_ = getattr(module, pool_name)
    except AttributeError as ex:
        raise RuntimeError(f"unknown connection pool: {pool_name}") from ex

    try:
        engine = create_engine(config["engine"],
                               echo=config.get("echo", False),
                               poolclass=class_)
    except (DBAPIError, SQLAlchemyError) as ex:
        raise RuntimeError(ex) from ex

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        insp = inspect(engine)
        tablenames = fully_qualify_tables(insp, dict(config["tablenames"]))
        missing = find_missing_tables(insp, list(tablenames.values()),
                                      skip_default=True)
    except (DBAPIError, SQLAlchemyError) as ex:
        raise RuntimeError(ex) from ex
    else:
        if missing:
            raise RuntimeError(f"table(s) {', '.join(missing)} not found")

    return session, tablenames


def validate_config(filename, schema):
    """Validate configuration.

    Parameters
    ----------
    filename : `str`
        Name of the file with the configuration that needs to validated.
    schema : `str`
        A document in YAML format describing the schema the configuration
        should be validated against.

    Raises
    ------
    ValueError
        If configuration can't be validated against the provided schema.
    OSError
        If the file with the configuration cannot be opened.
    """
    with open(filename) as f:
        config = yaml.safe_load(f)
    schema = yaml.safe_load(schema)
    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.ValidationError as ex:
        raise ValueError(f"configuration error: {ex.message}")
    except jsonschema.SchemaError as ex:
        raise ValueError(f"schema error: {ex.message}")
