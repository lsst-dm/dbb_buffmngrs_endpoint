Configuration files
-------------------

File format
^^^^^^^^^^^

Data Backbone (DBB) endpoint buffer manager configuration files use `YAML`_
format.

Finder configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **finder** component.

.. code-block:: yaml

  database:
    engine: <connection string>
    tablenames:
      file:
        schema: null
        table: <file table>
    echo: false
    pool_class: QueuePool
  finder:
    source: <file source location>
    storage: <storage area>
    actions:
      standard: Move
      alternative: Delete
    search:
      method: scan
      exclude_list: null
      date: null
      past_days: 1
      future_days: 1
    pause: 1
  logging:
    file: null
    format: "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
    level: INFO

``database`` options
""""""""""""""""""""

``database.engine``
    *Type*: string

    Database URL specifying the connection to the database back-end.

    Refer to SQLAlchemy `documentation`__ to find out how to construct a viable
    database URLs.

.. __: https://docs.sqlalchemy.org/en/13/core/engines.html#engine-configuration


``database.tablenames.file.schema``
    *Type*: string

    *Default*: null

    Database schema with the table where the Finder stores information
    about files it discovers.  When ``null``, the default database schema will
    be used (e.g. ``public`` for `PostgreSQL`_).

``database.tablenames.file.table``
    *Type*: string

    Database table where the Finder stores information about files it
    discovers.

``database.echo``
    *Type*: boolean

    *Default*: False

    Activates `SQLAlchemy` login.

    When enabled, SQLAlchemy will log all generated SQL.

``database.pool_class``
    *Type*: string

    *Default*: QueuePool

    Connection pooling method.  Refer to SQLAlchemy `documentation`__ for
    further information.

.. __: https://docs.sqlalchemy.org/en/13/core/pooling.html#module-sqlalchemy.pool


``finder`` options
""""""""""""""""""""

``finder.source``
    *Type*: string

    Location which Finder needs to monitor for new files.
    
    It can be either the DBB buffer or a directory with transfer logs (for
    rsync based file transfers).

``finder.storage``
    *Type*: string

    Absolute path of the storage area, the final destination for the
    incoming files.

``finder.actions.standard``
    *Type*: string

    Action to perform after creating a database entry for a file.

    Set it to ``Move`` for files arriving to the DBB buffer to instruct the
    finder to move files from the buffer to the storage area.

    For rsync based transfer, set it to ``Noop`` as the files are placed
    directly to the storage area.

``finder.actions.alternative``
    *Type*: string

    Action to perform when a file is already tracked, i.e., there is already a
    file with identical checksum and filename in the storage area.

    Set it to ``Delete`` to remove the duplicates or ``Noop`` to leave them
    alone.


``finder.search.method``
    *Type*: string

    Name of the method to use for the file discovery.

    Valid values are ``scan`` and ``parse_rsync_logs``.

    Use ``scan`` to instruct the finder to watch the DBB buffer.  For rsync
    based transfers, use ``parse_rsync_logs``.

``finder.search.exclude_list``
    *Type*: sequence

    *Default*: null

    List of patters (regular expressions) used to exclude unwanted files from
    the search based on their path.

    A file path will be excluded (effectively ignored by the finder) if it
    matches any pattern on that list.

    By default, no file is ignored.

``finder.search.date``
    *Type*: string

    *Default*: null

    Date in ISO format (YYYY-MM-DD).

    It instructs the finder to look for and parse transfer logs from ``[date -
    past_days, date + future_days]`` time range.

    If set to ``null``, the finder will use the current date and it will keep
    updating it with passing time.

    This option is only relevant if ``parse_rsync_logs`` is selected as the
    file discovery method.  Otherwise, it will be ignored.


``finder.search.past_days``
    *Type*: integer

    *Default*: 1

    Number of past days, relative to ``finder.search.date``, to include while
    searching for rsync logs to parse.

    This option is only relevant if ``parse_rsync_logs`` is selected as the
    file discovery method.  Otherwise, it will be ignored.

``finder.search.future_days``
    *Type*: integer

    *Default*: 1

    Number of future days, relative to ``finder.search.date``, to include while
    searching for rsync logs to parse.

    This option is only relevant if ``parse_rsync_logs`` is selected as the
    file discovery method.  Otherwise, it will be ignored.

``finder.search.delay``
    *Type*: integer

    *Default*: 60

    Time (in seconds) that need to pass from log's last modification before it
    will be considered fully transferred.

    This option is only relevant if ``parse_rsync_logs`` is selected as the
    file discovery method.  Otherwise, it will be ignored.

``finder.pause``
    *Type*: integer

    *Default*: 60

    Delay (in seconds) between two consecutive discovery-update cycles.
    
``logging`` options
""""""""""""""""""""

``logging.file``
    *Type*: string

    *Default*: null

    Name of the file where the log messages will be written to.

    By default, the log messages are just redirected to the standard output
    stream.

``logging.format``
    *Type*: string

    *Default*: "%(asctime)s:%(name)s:%(levelname)s:%(message)s"

    Format which is used to display the log messages.

    Refer to official documentation of the Python `logging`__ module to find
    out things which can be put in the format string.

.. __: https://docs.python.org/3/library/logging.html#logrecord-attributes

``logging.level``
    *Type*: string

    *Default*: INFO

    Severity level of log messages.

    The valid verbosity levels, ordered by increasing severity, are DEBUG,
    INFO, WARNING, ERROR, CRITICAL.


.. _PostgreSQL: https://www.postgresql.org
.. _SQLAlchemy: https://www.sqlalchemy.org
.. _YAML: https://yaml.org

