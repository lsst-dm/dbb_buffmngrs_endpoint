#
#

class Consistency:
    """A consistency tool to verify the status of DB.

    """

    def __init__(self, config):
        # Check if configuration is valid, i.e., all required settings are
        # provided; complain if not.
        required = {"session", "sources", "storage", "tablenames"}
        missing = required - set(config)
        if missing:
            msg = f"invalid configuration: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)

        self.session = config["session"]

        # Create necessary object-relational mappings. We are doing it
        # dynamically as RDBMS tables to use are determined at runtime.
        required = {"event", "file"}
        missing = required - set(config["tablenames"])
        if missing:
            msg = f"invalid ORMs: {', '.join(missing)} not provided"
            logger.error(msg)
            raise ValueError(msg)
        self.Event = event_creator(config["tablenames"])
        self.File = file_creator(config["tablenames"])

        self.storage = os.path.abspath(config["storage"])
        if not os.path.isdir(self.storage):
            msg = f"directory '{self.storage}' not found"
            logger.error(msg)
            raise ValueError(msg)

        self.sources = config["sources"]
        for src in [path for path in self.sources if path.startswith("/")]:
            logger.warning(f"{src} is absolute, should be relative")
            if os.path.commonpath([self.storage, src]) != self.storage:
                msg = f"{src} is not located in the storage area"
                logger.error(msg)
                raise ValueError(msg)

        search = config["search"]
        self.search_opts = dict(blacklist=search.get("blacklist", None))
