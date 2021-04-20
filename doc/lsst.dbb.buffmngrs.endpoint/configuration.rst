.. _config:

Configuration files
-------------------

.. _config-overview:

Overview
^^^^^^^^

This document describes configuration settings for DBB endpoint buffer manager
which is responsible for ingesting files into different data management
systems.

.. note::

   For more information about DBB buffer managers, see `DMTN-154`_.

It is divided into sections describing configuration settings for each endpoint
manager's component.  Each section contains an example file with all available
configuration settings and their default values followed by descriptions of the
settings.

Many of these configuration values are similar across different components and
are only fully described in the first instance.

.. _config-format:

File format
^^^^^^^^^^^

Data Backbone (DBB) endpoint buffer manager configuration files use `YAML`_
format.

.. note::

   A friendly reminder, in YAML ``null`` represents the lack of a value. This
   is typically bound to a native null-like value (e.g. ``None`` in Python).

.. _finder-config:

Finder configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **finder** component.

.. literalinclude:: ../../etc/finder.yaml
   :language: yaml

.. _finder-config-db:

``database`` settings
"""""""""""""""""""""

``database.engine``
    *Type*: string

    Database URL specifying the connection to the database back-end.

    Refer to SQLAlchemy `documentation`__ to find out how to construct viable
    database URLs.

.. __: https://docs.sqlalchemy.org/en/13/core/engines.html#engine-configuration


``database.tablenames.file.schema``
    *Type*: string

    *Default*: null

    Database schema with the table where the Finder stores information
    about files it discovers.  When ``null``, the finder will use the default
    schema provided by the database connection.

``database.tablenames.file.table``
    *Type*: string

    Database table where the Finder stores information about files it
    discovers.

``database.echo``
    *Type*: boolean

    *Default*: False

    Activates `SQLAlchemy` logging.

    When enabled, SQLAlchemy will log all generated SQL.

    The generated SQL will be written to the file specified by ``logging.file``
    (stdout, be default).

``database.pool_class``
    *Type*: string

    *Default*: QueuePool

    Connection pooling method.  Refer to SQLAlchemy `documentation`__ for
    further information.

.. __: https://docs.sqlalchemy.org/en/13/core/pooling.html#module-sqlalchemy.pool

.. _finder-config-finder:


``finder`` settings
"""""""""""""""""""

``finder.source``
    *Type*: string

    File storage location which Finder needs to monitor for new files.
    
    It can be either the DBB BM buffer or a directory with transfer logs (for
    rsync based file transfers).

``finder.storage``
    *Type*: string

    Absolute path of the storage area, the final destination for the
    incoming files.

``finder.actions.standard``
    *Type*: string

    *Default*: null

    Action to perform after creating a database entry for a file.

    Set it to ``Move`` for files arriving to the DBB buffer to instruct the
    finder to move files from the buffer to the storage area.

    For rsync-based transfer, keep the default value or set it explicitly to
    ``Noop`` as the files are placed directly to the storage area by the
    transfer process.

``finder.actions.alternative``
    *Type*: string

    *Default*: null

    Action to perform when a file is already tracked, i.e., there is already a
    file with identical checksum and filename in the storage area.

    Set it to ``Delete`` to remove the duplicates. To leave the duplicates
    alone, keep the default value or set it explicitly to ``Noop``.

``finder.search.method``
    *Type*: string

    Name of the method to use for the file discovery.

    Valid values are ``scan`` and ``parse_rsync_logs``.

    Use ``scan`` to instruct the finder to watch the DBB buffer.  For
    rsync-based transfers, use ``parse_rsync_logs``.

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

    If set to ``null``, the finder will use the current date at the beginning
    of each discovery cycle.

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

    Time (in seconds) that need to pass from transfer log's last modification
    before it will be considered fully transferred.

    This option is only relevant if ``parse_rsync_logs`` is selected as the
    file discovery method.  Otherwise, it will be ignored.

``finder.pause``
    *Type*: integer

    *Default*: 60

    Delay (in seconds) between two consecutive discovery-update cycles.
    
.. _finder-config-logging:

``logging`` options
"""""""""""""""""""

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

.. _ingester-config:

Ingester configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **ingester**
component using Gen2 plugin.

.. literalinclude:: ../../etc/gen2ingester.yaml
   :language: yaml

.. note::

   The example shows content of the ``etc/gen2ingester.yaml``. For example
   configuration of the ingester using the Gen3 plugin, see
   ``etc/gen3ingester.yaml`` instead.

.. _ingester-config-db:

``database`` settings
"""""""""""""""""""""

Database settings for the **ingester** are *almost* indentical to
:ref:`finder's <finder-config-db>`.

The only difference is that you need to include additional subsection, called
``event``, in the section ``tablenames`` which specifies the table where the
**ingester** should store information about file events.

Event table specification looks exactly like file table specification (see
`this <finder-config-db>` section for details).

.. _ingester-config-ingester:

``ingester`` settings
"""""""""""""""""""""

``ingester.storage``
    *Type*: string

    Absolute path of the storage area.
    
    Must be the same path used by the finder.

``ingester.plugin.name``
    *Type*: string

    Name of the plugin responsible for ingesting images to a given data
    management system.

    Currently, there are two plugins: **Gen2Ingest** and **Gen3Ingest** which
    support ingesting images to a Gen2 and Gen3 Butler dataset repository,
    respectively.

``ingester.plugin.config``
    *Type*: object

    This section contains configuration settings for a specific ingest code
    used by the given plugin.

    For Gen2 and Gen3 Butler ingest plugins, the subsection called **butler**
    must exist and it must contain **root** setting, the absolute path of the
    Butler dataset repository.  An optional subsection called **ingest** allow
    for overriding default ingest settings.

    An additional section, called **visit**, may be specified for Gen3 ingest
    plugin.  If specified, the Gen3 ingest plugin will also invoke the LSST
    task responsible for defining visits after a successful ingestion.  When
    specified, the section must contain **instrument**, a fully-qualified class
    name which handles instrument-specific logic for the Gen3 Butler.

    Example files showing complete list of options recognized by the given
    plugin as well as their default values are located in project's ``etc``
    directory.

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

    Values are case-sensitive.

``ingester.plugin.batch_size``
    *Type*: integer

    *Default*: 10

    Number of files which an ingester attempts to ingest in a single cycle
    before allocating next batch of files to ingest.

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

    Delay (in seconds) between two consecutive discovery-ingest cycles.

.. _ingester-config-logging:

``logging`` options
"""""""""""""""""""

Logging options for the **ingester** are exactly the same as for the
:ref:`finder <finder-config-logging>`.

.. _backfill-config:

Backfill configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example file shows all available configuration settings and their
default values (where applicable) for DBB endpoint manager **backfill**
component.

.. literalinclude:: ../../etc/backfill.yaml
   :language: yaml

.. _backfill-config-db:

``database`` settings
"""""""""""""""""""""

Database options for the **backfill** are exactly the same as for the
:ref:`ingester <ingester-config-db>`.

.. _backfill-config-backfill:


``backfill`` settings
"""""""""""""""""""""

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

.. _backfill-config-logging:

``logging`` options
"""""""""""""""""""

Logging options for the **backfill** are exactly the same as for the
:ref:`finder <finder-config-logging>` and :ref:`ingester
<ingester-config-logging>`.

.. _DMTN-154: https://dmtn-154.lsst.io
.. _PostgreSQL: https://www.postgresql.org
.. _SQLAlchemy: https://www.sqlalchemy.org
.. _YAML: https://yaml.org

