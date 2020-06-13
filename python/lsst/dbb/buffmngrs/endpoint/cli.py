import click
import logging
from .finder.commands import finder
from .ingester.commands import ingester


logger = logging.getLogger("lsst.dbb.buffmngrs.endpoint")


@click.group()
def cli():
    pass


cli.add_command(finder)
cli.add_command(ingester)


def main():
    """Start microservices for DBB endpoint manager.
    """
    return cli()
