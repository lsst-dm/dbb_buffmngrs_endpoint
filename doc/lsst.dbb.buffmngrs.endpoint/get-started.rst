Quickstart
----------

Overview
^^^^^^^^

Data Backbone (DBB) endpoint manager attempts to ingest files arriving to a
selected location to a data management system of choice, e.g., Gen2/3 Butler.


Prerequisites
^^^^^^^^^^^^^

The endpoint manager consists of two core components: a Finder and an Ingester.
Each component of the endpoint manager uses a relational database management
system (RDBMS) to store various information about files and a complete history
of events for each file.

While the components should be fairly agnostic about the database back-end
(they use internally `SQLAlchemy`_ to interact with the RDMBS), the endpoint
manager is usually tested with `PostgreSQL`_ and `SQLite`_.

To run the manager, you'll also need an access to a LSST slack installation.

Assuming that the environment variable ``LSSTSW`` points to your installation,
load initialize LSST environment and setup required packages:

.. code-block:: bash

   source ${LSSTSW}/loadLSST.bash
   setup lsst_distrib -t current

.. PostgreSQL: https://www.postgresql.org/
.. SQLAlchemy: https://www.sqlalchemy.org/
.. SQLite: https://sqlite.org/index.html

If you need to setup a specific version of the LSST stack, replace ``current``
with a tag of your choice.

Download and install DBB endpoint manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a directory where you want to install DBB endpoint manager. For example

.. code-block:: bash

   mkdir -p lsstsw/addons
   cd lsstsw/addons

Clone the repository from GitHub:

.. code-block:: bash

   git clone https://github.com/lsst-dm/dbb_buffmngrs_endpoint .

Set it up and build:

.. code-block:: bash

   cd dbb_buffmngrs_endpoint
   setup -j -r .
   scons

If you would like to select a specific version of the manager, run ``git
checkout <ver>`` *before* ``setup -j -r .`` where ``<ver>`` is either a
existing git tag or branch.

Configure DBB endpoint buffer manger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each component of the endpoint manager reads its configuration from a file in
the  YAML format.

The following sample configuration file contains several settings for a Finder
component that you may adapt to your local configuration:

.. code-block:: yaml

   database:
     engine: "postgres://tester:password@localhost:5432/test"
     tablenames:
       file: files
       event: gen2_file_events
   finder:
     source: /data/buffer
     storage: /data/storage
     actions:
       standard: Move
       alternative: Delete
     search:
       method: scan


The section ``database`` describes how to access the database and establishes
object relational mappings (ORMs).

It tells the Finder to use `PostgreSQL`_ database ``test`` on ``localhost``
which can be accessed be a user ``tester`` having password ``password``.  It
also instructs the Finder which database tables to use to store the
information, here ``files`` and ``gen2_file_evetns``.

The next section, ``finder``, defines mandatory settings for the Finder itself.

With the settings above, the Finder will scan directory ``/data/buffer`` for
new files. Once it discovers a new file it will perform a standard action that
is it will move the file to the storage area located in ``/data/storage``.  If
the file turns out to be a duplicate already present in the storage area, the
Finder will execute the alternative action, i.e., it will delete the file.

The Ingester configuration looks quite similar:

.. code-block:: yaml

   database:
     engine: "postgres://tester:password@localhost:5432/test"
     tablenames:
       file: files
       event: gen2_file_events
   ingester:
     plugin:
       name: Gen2Ingest
       config:
         root: /data/gen2repo

As you can see, the section ``database`` is identical to the respective
section of Finder's configuration.

Similarly to the Finder, the section ``ingester`` defines mandatory setting for
this component.

The Ingester uses plugins to ingest images to diffrent database systems. Hence
you need to tell which plugin to use and provide settings a given plugin may
need to access the data management system it supports.

In the provided example the Ingester will be ingesting images to a Gen2 data
repository located in ``/data/gen2repo`.

.. note::

   To see other supported configuration options, look at ``etc/finder.yml`` or
   ``etc/ingester.yaml`` in the DBB endpoint buffer manager repository.

Run DBB endpoint manager
^^^^^^^^^^^^^^^^^^^^^^^^

You need to start each component of the endpoint manager separately.

.. note::

   While it may look like an uncessary burden, it gives you a great flexibility
   in creating a DBB endpoint buffer manager and also ensures that potential
   catastrophic failuers of one component won't affect the others.

Assuming that the configuration files for the Finder and the Ingester
(``finder.yaml`` and ``ingester.yaml`` respectively) are in
your current directory, you can start both components with

.. code-block:: bash

   endmgr finder start finder.yaml &
   endmgr ingester start ingester yaml &

.. note::

   Alternatively, you may use a terminal multiplexer like `tmux`_ or `screen`_
   and start each component in a separate window.

.. screen: https://www.gnu.org/software/screen
.. tmux: https://github.com/tmux/tmux/wiki


Stop DBB endpoint manager
^^^^^^^^^^^^^^^^^^^^^^^^^

Once started, the DBB endpoint manager (or more specifically, its components)
will keep monitoring the specified location and ingesting arriving images to
the configured data management system (or systems).

To stop the components a given DBB buffer manager consists of, find the
process ids of its components and terminate them bu sending SIGTERM to each of 
them.

.. code-block:: bash

   killall -15 endmngr

.. warning::

   If you have multiple DBB endpoint managers running on you system, find the
   process ids of the components of the specific endpoint manager with

   .. code-block::

      ps aux | grep endmgr

   and terminate them selectively with

   .. code-block::

      kill -15 FINDER_ID INGESTER_ID

   instead.

Manage file ingestion
^^^^^^^^^^^^^^^^^^^^^

For each file, the Ingester records *all* events associated with it in the
database table usually suffixed with ``_file_events``.

Each event has a status assigned to it.  Possible event statuses are:

#. UNTRIED: the Ingester hasn't tried to ingest the file yet,
#. PENDING: the Ingester added the file to its internal ingest queue and will
   make an attempt to ingest file to a given data management system soon,
#. SUCCESS: the ingest attempt was made and the file was successfully ingested
   to the data management system,
#. FAILURE: the ingest attempt was made, but it failed,
#. UNKNOWN: the Ingester is unable to determine neither if the ingest attempt
   was made nor the result of the attempt (e.g. as a result of an ingest thread
   is killed by an external process during the attempt).

.. warning::

   The database stores a complete history of events for each file.  Remember to
   always look at the most recent event for a given file to determine its
   current status.

There is one more special status, RERUN, which you can use to tell the Ingester
to make another ingest attempt for selected files.

#. Stop the Ingester (if it's running).

#. For each file you want the Ingester to reingest, add a row to the
   ``*_file_events`` table with the filed ``status`` set to ``RERUN``. For
   example, when using PostgreSQL it can be done with:

   .. code-block::

      INSERT INTO gen2_file_events
      (start_time, status, files_id)
      VALUES (CURRENT_TIMESTAMP, 'RERUN', <files_id>);

   where ``<files_id>`` is the id number of the file.

#. Start the Ingester in non-daemonic mode, telling it to look only for files
   with ``RERUN`` status (note two new lines at the end):

   .. code-block::

      database:
        engine: "postgres://tester:password@localhost:5432/test"
        tablenames:
          file: files
          event: gen2_file_events
        ingester:
          plugin:
            name: Gen2Ingest
            config:
              root: /data/gen2repo
          daemon: false
          file_status: RERUN

When run in non-daemonic mode, the Ingester will quit after processing all the
files with ``RERUN`` status.
