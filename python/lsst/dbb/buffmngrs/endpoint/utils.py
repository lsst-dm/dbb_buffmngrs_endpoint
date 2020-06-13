import logging


def setup_logger(logger, options=None):
    """Configure logger.

    Parameters
    ----------
    logger : logging.Logger
        A logger to set up.
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
    return logger
