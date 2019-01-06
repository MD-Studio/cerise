Cerise Configuration
====================

Introduction
------------

Cerise takes configuration information from various sources, with some
overriding others. This page describes the configuration files and what can be
configured in them.

.. _main_configuration:

Main configuration file
-----------------------

The main configuration file is located at ``conf/config.yml``, and contains
general configuration information for the Cerise service in YAML format. It
looks as follows::

  database:
    file: run/cerise.db

  logging:
    file: /var/log/cerise/cerise_backend.log
    level: INFO

  pidfile: run/cerise_backend.pid

  client-file-exchange:
    store-location-service: file:///tmp/cerise_files
    store-location-client: file:///tmp/cerise_files

  rest-service:
    base-url: http://localhost:29593
    hostname: 127.0.0.1
    port: 29593

Cerise uses SQLite to persistently store the jobs that have been submitted to
it. SQLite databases consist of a single file, the location of which is given by
the ``file`` key under ``database``.

Logging output is configured under the ``logging`` key. Make sure that the user
that Cerise runs under has write access to the given path. If you want to log to
/var/log without giving Cerise root rights, making the specified log file on
beforehand and then giving ownership to the user Cerise runs under works well.
Or you can make a subdirectory and give the user access to that.

The ``pidfile`` key specifies a path to a file into which Cerise's process
identifier (PID) is written. This can be used to shut down a running service,
i.e. ``kill <pid>`` will cleanly shut down Cerise.

Under ``client-file-exchange``, the means of communicating files between Cerise
and its users is configured. Communication is done using a shared folder
accessible to both the users and the Cerise service. If Cerise is running
locally, both parties have access to the same file system, and see the shared
folder in the same location. Thus, ``store-location-service`` and
``store-location-client`` both point to the same on-disk directory.

If the Cerise service does not share a file system with the client, then a
directory on the Cerise server must be made available to the client, e.g. via
WebDAV. In this case, client and service access the same directory using
different URLs, e.g.

::

  client-file-exchange:
    store-location-service: file:///home/webdav/files
    store-location-client: http://localhost:29593/files

The user is expected to submit references to files that start with the URL in
``store-location-client``, Cerise will then fetch the corresponding files from
the directory specified in ``store-location-service``.

``store-location-client`` can be overridden by specifying the environment
variable CERISE_STORE_LOCATION_CLIENT. If you want to run multiple Cerise
instances in containers, simultaneously, then you need to remap the ports on
which they are available to avoid collisions. With this environment variable,
the port can be easily injected into the container, removing the need to have
a different image for each container. Cerise Client uses this functionality.

Finally, key ``rest-service`` has the hostname and port on which the REST
service should listen, as well as the external URL on which it is available.
If you want the service to be available to the outside
world, this should be the IP address of the network adaptor to listen on, or
``0.0.0.0`` to listen on all adaptors. Note that a service running inside a
Docker container needs to have ``0.0.0.0`` for it to be accessible from outside
the container.

Since the service needs to pass URLs to the client sometimes, it needs to know
at which URL it is available to the client. This is specified by ``base-url``,
which should contain the first part of the URL to the REST API, before the
``/jobs`` part. Alternatively, you can set the CERISE_BASE_URL environment
variable to this value.

.. _compute-resource-configuration:

Compute resource configuration
------------------------------

Information on which compute resource to connect to, and how to transfer files
and submit jobs to it, is stored separately from the main service configuration,
to make it easier to create specialisations. Furthermore, to enable different
users to use the same specialised Cerise installation (e.g. Docker image),
credentials can be specified using environment variables. (Cerise Client uses
the latter method.) If you are making a specialisation that is to be shared with
others, do not put your credentials in this file!

Note: this file is somewhat outdated, but well be updated prior to the 1.0 release.

API configuration file
......................

The API configuration file is located in ``api/config.yml``, and has the following
format::

  compute-resource:
    credentials:
      username: None
      password: None
      certfile: None
      passphrase: None

    files:
      credentials:
        username: None
        password: None
        certfile: None
        passphrase: None

      protocol: local
      location: None
      path: /home/$CERISE_USERNAME/.cerise

    jobs:
      credentials:
        username: None
        password: None
        certfile: None
        passphrase: None

      protocol: local
      location: None
      scheduler: none

      queue-name: None      # cluster default
      slots-per-node: None  # cluster default
      cores-per-node: 32
      scheduler-options: None
      cwl-runner: $CERISE_API_FILES/cerise/cwltiny.py

    refresh: 10

This file describes the compute resource and how to connect to it. Under the
``files`` key, file access (staging) is configured, while the ``jobs`` key has
settings on how to submit jobs. ``credentials``, and keys ``username``,
``password``, ``certfile`` and ``passphrase`` occurring throughout, refer to
credentials, and will be discussed below. Keys may be omitted if they are not
needed, e.g. ``location`` may be omitted if ``protocol`` is ``local``, in which
case credentials may also me left out.

For file staging, a protocol, location and path may be specified.  Supported
protocols are ``file``, ``sftp``, ``ftp``, or ``webdav``, where ``file`` refers
to direct access to the local file system.

``location`` provides the host name to connect to; to run locally, this may be
omitted or empty. ``path`` configures the remote directory where Cerise will put
its files. It may contain the string ``$CERISE_USERNAME``, which will be
replaced with the user account name that the service is using. This is useful if
you want to put Cerise's files into the users home directory, e.g.
``/home/$CERISE_USERNAME/.cerise`` (which is the default value). Note that
user's home directories are not always in ``/home`` on compute clusters, so be
sure to check this.

Job management is configured under the ``jobs`` key. Here too a protocol may be
given, as well as a location, and a few other settings can be made.

For job management, the protocol can be ``local`` (default) or ``ssh``. If the
``local`` protocol is selected, ``location`` is ignored, and jobs are run
locally. For the ``ssh`` protocol, ``location`` is the name of the host,
optionally followed by a colon and a port number (e.g. ``example.com:2222``).

Jobs can be run directly or via a scheduler. To run jobs directly, either on the
local machine or on some remote host via SSH, set the scheduler to ``none``.
Other valid values for ``scheduler`` are ``slurm``, ``torque`` and
``gridengine`` to submit jobs to the respective job management system.

If jobs need to be sent to a particular queue, then you can pass the queue name
using the corresponding option; if it is not specified, the default queue is
used. If one or more of your steps start MPI jobs, then you may want to set the
number of MPI slots per node via ``slots-per-node`` for better performance. If
you need to specify additional scheduler options to e.g. select a GPU node, you
can do so using e.g. ``scheduler-options: "-C TitanX --gres=gpu:1"``. Ideally,
it would be possible to specify this in the CWL file for the step, but support
for this in CWL is partial and in-development, and Cerise does not currently
support this. Users can specify the number of cores to run on using a CWL
ResourceRequirement, but Cerise always allocates whole nodes. It therefore needs
to know the number of cores in each node, which you should specify using
``cores-per-node``.

Finally, ``cwl-runner`` specifies the remote path to the CWL runner. It defaults
to ``$CERISE_API_FILES/cerise/cwltiny.py``, which is Cerise's included simple
CWL runner. ``$CERISE_API_FILES`` will be substituted for the appropriate remote
directory by Cerise. See :doc:`Specialising Cerise <specialising>` for more
information.

Cerise will regularly poll the compute resource it is connected to, to check if
any of the running jobs have finished. The ``refresh`` setting can be used to
set the minimum interval in seconds between checks, so as to avoid putting too
much load on the machine.

Credentials may be put into the configuration file as indicated. Valid
combinations are:

- No credentials at all (for running locally)
- Only a username
- A username and a password
- A username and a certificate file
- A username, a certificate file, and a passphrase

If the credentials to use for file access and job management are the same, then
you should list them under ``credentials`` and omit them in the other locations.
If different credentials are needed for files and jobs, then a ``credentials``
block can be specified under ``files`` and ``jobs`` respectively. Credentials
listed here may be overridden by environment variables, as described below.


Environment variables
.....................

Cerise checks a set of environment variables for credentials. If found, they
override the settings in the configuration file. These variables are:

General credentials

- CERISE_USERNAME
- CERISE_PASSWORD
- CERISE_CERTFILE
- CERISE_PASSPHRASE

Credentials for file access

- CERISE_FILES_USERNAME
- CERISE_FILES_PASSWORD
- CERISE_FILES_CERTFILE
- CERISE_FILES_PASSPHRASE

Credentials for job management

- CERISE_JOBS_USERNAME
- CERISE_JOBS_PASSWORD
- CERISE_JOBS_CERTFILE
- CERISE_JOBS_PASSPHRASE

As in the configuration file, specific credentials go before general ones.
Cerise will first try a specific environment variable (e.g.
CERISE_JOBS_USERNAME), then the corresponding specific configuration file entry
(under ``jobs``), then a generic environment variable (e.g. CERISE_USERNAME),
and finally the generic configuration file entry (under ``credentials``).

It does this for each of the four credential components separately, then uses
the first complete combination from the top down to connect:

- username + certfile + passphrase
- username + certfile
- username + password
- username
- <no credentials>

