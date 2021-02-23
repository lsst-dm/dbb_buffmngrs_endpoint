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
"""Command line interface for the DBB endpoint buffer manager.
"""
import click
import logging
from .backfill.commands import backfill
from .finder.commands import finder
from .ingester.commands import ingester


logger = logging.getLogger("lsst.dbb.buffmngrs.endpoint")


@click.group()
def cli():
    pass


cli.add_command(finder)
cli.add_command(ingester)
cli.add_command(backfill)


def main():
    """Start microservices for DBB endpoint manager.
    """
    return cli()
