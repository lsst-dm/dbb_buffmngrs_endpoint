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
"""Methods responsible for file discovery.
"""
import logging
import os
import re
from datetime import date, datetime, timedelta


__all__ = ["parse_rsync_logs", "scan", "search_methods"]


logger = logging.getLogger(__name__)


# A registry of available file discovery methods.
#
# The registry is being populated when module is loaded with function
# decorated with @registry decorator.
search_methods = {}


def register(gen):
    """Register a generator as a file discovery method.

    Parameters
    ----------
    gen : generator
        A generator iterating over a file names in a given directory tree.

    Returns
    -------
    gen : generator
        The original, unchanged generator.
    """
    search_methods[gen.__name__] = gen
    return gen


@register
def scan(directory, exclude_list=None, **kwargs):
    """Generate the file names in a directory tree.

    Generates the file paths in a given directory tree ignoring files
    whose names match select patterns.

    Parameters
    ----------
    directory : `str`
        The root of the directory tree which need to be searched.
    exclude_list : `list` of `str`, optional
        List of regular expressions file names should be match against. If a
        filename matches any of the patterns in the list, file will be ignored.
        By default, no file is ignored.
    **kwargs
        Additional keyword arguments.

    Yields
    ------
    path : `string`
        An file path relative to the ``directory``.
    """
    if exclude_list is None:
        exclude_list = []
    for dirpath, _, filenames in os.walk(directory):
        for fn in filenames:
            path = os.path.relpath(os.path.join(dirpath, fn), start=directory)
            matches = [f"'{patt}'" for patt in exclude_list
                       if re.search(patt, path) is not None]
            if matches:
                logger.debug(f"{path} was excluded by pattern(s): "
                             f"{', '.join(matches)}")
                continue
            yield path


@register
def parse_rsync_logs(directory, exclude_list=None,
                     isodate=None, past_days=1, future_days=1,
                     delay=60, extension="done"):
    """Generate the file names based on the content of the rsync logs.

    This is a specialized search method for finding files which where
    transferred to the storage area by ``rsync``. It identifies these
    files by parsing ``rsync``'s log files. It assumes that:

    1. The logs are stored in a centralized location.
    2. Logs from different days are kept in different subdirectories in that
       location.
    3. The name of each subdirectory represents the date when
       files where transferred and that date is expressed as ``YYYYMMDD``.
    4. Log files contain file paths relative to the root of the storage area.

    After the function completed parsing a given logfile, it creates an
    empty file ``<logfile>.<extension>`` which act as a sentinel preventing it
    from parsing the file again.

    As the files from a single observation night can be placed in different
    directories and these directories can be created in a time zone
    different from the one the endpoint site operates in, be default the
    function monitors also log files in directories corresponding to a day
    before and a day after the current day (if any of them exists).  This
    default settings determining which directories will be monitored can be
    changed by adjusting ``past_days`` and ``future_days`` options (see
    below).

    Parameters
    ----------
    directory : `str`
        The directory where the all log files reside.
    exclude_list : `list` of `str`, optional
        List of regular expressions file names should be match against. If a
        filename matches any of the patterns in the list, file will be ignored.
        By default, no file is ignored.
    isodate : `str`, optional
        String representing ISO date corresponding the directory which the
        function should monitor for new logs. If None (default), it will be set
        to the current date.
    past_days : `int`, optional
        The number of past days to add to the list of monitored directories.
        Defaults to 1.
    future_days : `int`, optional
        The number of future days to add to the list of monitored directories.
        Defaults to 1.
    delay : `int`, optional
        Time (in seconds) that need to pass from log's last modification before
        it will be considered fully transferred. By default, it is 60 s.
    extension : `str`, optional
        An extension to use when creating a sentinel file. Defaults to "done".

    Yields
    ------
    path : `string`
        A file path extracted from the log files.
    """
    if exclude_list is None:
        exclude_list = []
    delay = timedelta(seconds=delay)
    origin = date.today()
    if isodate is not None:
        origin = date.fromisoformat(isodate)
    start = origin - timedelta(days=past_days)
    for offset in range(past_days + future_days + 1):
        day = start + timedelta(days=offset)
        top = os.path.join(directory, day.isoformat().replace("-", ""))
        if not os.path.exists(top):
            continue
        for dirpath, _, filenames in os.walk(top):
            manifests = [os.path.join(dirpath, fn) for fn in filenames
                         if re.match(r"rsync.*log$", fn)]
            for manifest in manifests:
                sentinel = ".".join([manifest, extension])

                # Ignore a log file if it was modified withing a specified
                # time span, i.e., assume that its transfer has not finished
                # yet.
                manifest_mtime = None
                try:
                    status = os.stat(manifest)
                except FileNotFoundError:
                    logger.error(f"{manifest}: file not found")
                else:
                    manifest_mtime = datetime.fromtimestamp(status.st_mtime)
                now = datetime.now()
                if manifest_mtime is None or now - manifest_mtime < delay:
                    continue

                # Check if a log file was already parsed, i.e., a sentinel
                # file exists. If it wasn't (no sentinel), do nothing; file
                # hasn't been parsed yet. If it was parsed, check if by
                # chance the log file hasn't been modified AFTER it was
                # marked as parsed. It that's the case, remove the sentinel.
                sentinel_mtime = None
                try:
                    status = os.stat(sentinel)
                except FileNotFoundError:
                    pass
                else:
                    sentinel_mtime = datetime.fromtimestamp(status.st_mtime)
                if sentinel_mtime is not None:
                    if sentinel_mtime < manifest_mtime:
                        logger.error(f"{manifest} changed since marked as "
                                     f"parsed, removing the sentinel")
                        os.unlink(sentinel)
                    else:
                        continue

                with open(manifest) as f:
                    for line in f:
                        if "<f+++++++++" not in line:
                            continue
                        _, _, path, *_ = line.strip().split()
                        matches = [f"'{patt}'" for patt in exclude_list
                                   if re.search(patt, path) is not None]
                        if matches:
                            logger.debug(f"{path} was excluded by pattern(s) "
                                         f"{', '.join(matches)}")
                            continue
                        yield path
                os.mknod(sentinel)
