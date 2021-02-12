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

    The generated SQL will be written to the file specified by ``logging.file``
    (stdout, be default).

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

    List of patterns (regular expressions) used to exclude unwanted files from
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

Ingester configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **ingester**
component.

.. code-block:: yaml

   database:
     engine: <connection string>
     tablenames:
     file:
       schema: null
       table: <file table>
     event:
       schema: null
       table: <event table>
     echo: false
     pool_class: QueuePool
   ingester:
     storage: <storage area>
     plugin:
       name: <plugin name>
       config:

         # Gen2Ingest specific options.
         root: <dataset repository>
         dryrun: false
         mode: link
         create: false
         ignoreIngested: false

         # Gen3Ingest specific options.
         root: <dataset repository>
         config: null
         config_file: null
         ingest_task: lsst.obs.base.RawIngestTask
         output_run: null
         processes: 1
         transfer: symlink
         failFast: true

     include_list: null
     exclude_list: null
     file_status: UNTRIED
     batch_size: 10
     daemon: true
     num_threads: 1
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

    Database schema with the table where the finder stores information
    about files it discovers.  When ``null``, the default database schema will
    be used (e.g. ``public`` for `PostgreSQL`_).

``database.tablenames.file.table``
    *Type*: string

    Database table where the Finder stores information about files it
    discovers.

``database.tablenames.events.schema``
    *Type*: string

    *Default*: null

    Database schema with the table where the ingester stores information
    about file events.  When ``null``, the default database schema will
    be used (e.g. ``public`` for `PostgreSQL`_).

``database.tablenames.events.table``
    *Type*: string

    Database table where the ingester stores information about file events.

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

``ingester`` options
""""""""""""""""""""

``ingester.storage``
    *Type*: string

    Absolute path of the storage area.

``ingester.plugin.name``
    *Type*: string

    Name of the plugin responsible for ingesting images to a given data
    management system.

    Currently, there are two plugins: **Gen2Ingest** and **Gen3Ingest** which
    support ingesting images to a Gen2 and Gen3 Butler dataset repository,
    respectively.

``ingester.plugin.config``
    *Type*: object

    This section contains configuration settigs for a specific ingest code used
    by the given plugin.  All settings are passed directly to the LSST function
    responsible for creating the actual ingest task.

    Only **root**, the absolute path of the Butler dataset repository, is
    mandatory, others are optional.

``ingester.include_list``
    *Type*: sequence

    *Default*: null

    List of patterns (regular expressions).

    The ingester will make no ingest attempts for a file path that *doesn't*
    match at least one pattern on that list.

    By default, ingest attempts are made for every file.


``ingester.exclude_list``
    *Type*: sequence

    *Default*: null

    List of patterns (regular expressions)

    The ingester will make no ingest attempts for a file path that *matches*
    any pattern on that list.

    By default, ingest attempts are made for every file.

``ingester.plugin.file_status``
    *Type*: string

    *Default*: UNTRIED

    Status of files for which the ingester will be making ingest attempts.

    The viable values are:

    * UNTRIED,
    * RERUN,
    * FAILURE,
    * IGNORED,
    * BACKFILL.

``ingester.plugin.batch_size``
    *Type*: integer

    *Default*: 10

    Number of files which an ingester attempt to ingest in a single session.

``ingester.plugin.num_threads``
    *Type*: integer

    *Default*: 1

    Number of threads to use for making ingest attempts.

``ingester.plugin.daemon``
    *Type*: boolean

    *Default*: true

    If true, the ingester will run continuously once started, constantly
    checking if new files arrived. Otherwise, it will stop if there are no
    files to process.

``ingester.plugin.pause``
    *Type*: integer

    *Default*: 1

    Delay (in seconds) between two consecutive discovery-ingest sessions.

``logging`` options
"""""""""""""""""""

Logging options for the **ingester** are exactly the same as for the **finder**.

Backfill configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **backfill**
component.

.. code-block:: yaml

   database:
     engine: <connection string>
     tablenames:
       file:
         schema: null
         table: <file table>
       event:
         schema: null
         table: <event table>
     echo: false
     pool_class: QueuePool
   backfill:
     storage: <storage area>
     sources:
       - <subdirectory>
     search:
       exclude_list: null
   logging:
     file: null
     format: "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
     level: INFO

``database`` options
""""""""""""""""""""

Database options for the **backfill** are exactly the same as for the
**ingester**.

``backfill`` options
""""""""""""""""""""

``backfill.storage``
    *Type*: string

    Absolute path of the storage area.

``backfill.sources``
    *Type*: sequence

    List of paths, i.e., files and/or directories.

    All paths should be specified relative to the storage area.

    Unix style pathname pattern expansion when specifying sources. Tilde
    expansion is not supported though.

    To prevent Python YAML parser from accidental conversion of strings like
    ``2021-01-01`` into ``datetime`` objects, enclose them in quotes.

``backfill.search.exclude_list``
    *Type*: sequence

    *Default*: null

    List of patterns (regular expressions).

    A file path will be excluded (effectively ignored by the backfill) if it
    *matches* any pattern on that list.

    By default, no file is ignored.

``logging`` options
"""""""""""""""""""

Logging options for the **backfill** are exactly the same as for the
**ingester**.

.. _PostgreSQL: https://www.postgresql.org
.. _SQLAlchemy: https://www.sqlalchemy.org
.. _YAML: https://yaml.org

