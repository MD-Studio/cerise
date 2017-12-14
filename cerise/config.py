import jpype
import logging
import os
import xenon
import yaml

class Config:
    def __init__(self, xenon, config, api_config):
        """Create a configuration object.

        Args:
            config (dict): A main configuration dict.
            api_config (dict): An API configuration dict.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._x = xenon
        """xenon.Xenon: The Xenon object to use."""
        self._config = config
        """The main configuration dictionary."""
        self._api_config = api_config
        """The API configuration dictionary."""
        self._cr_config = None
        """The compute-resource part of the main config."""

        if 'compute-resource' not in self._api_config:
            self._api_config['compute-resource'] = {}
        self._cr_config = self._api_config['compute-resource']

    def _get_credential_variable(self, kind, name):
        def have_config(kind, name):
            if kind == '':
                return 'credentials' in self._cr_config and \
                        name in self._cr_config['credentials']
            return kind in self._cr_config and \
                    'credentials' in self._cr_config[kind] and \
                    name in self._cr_config[kind]['credentials']

        def get_config(kind, name):
            if kind == '':
                return self._cr_config['credentials'][name]
            return self._cr_config[kind]['credentials'][name]

        def env_var(kind, name):
            if kind == '':
                return '_'.join(['CERISE', name.upper()])
            else:
                return '_'.join(['CERISE', kind.upper(), name.upper()])

        def have_env(kind, name):
            return env_var(kind, name) in os.environ

        def get_env(kind, name):
            return os.environ[env_var(kind, name)]

        value = None
        if have_config('', name):
            value = get_config('', name)
        if have_env('', name):
            value = get_env('', name)
        if have_config(kind, name):
            value = get_config(kind, name)
        if have_env(kind, name):
            value = get_env(kind, name)
        return value

    def _get_xenon1_scheme(self, kind, protocol, scheduler=None):
        """
        Decides which Xenon 1 scheme to use for a given protocol and
        scheduler.

        Args:
            kind (str): Either 'files' or 'jobs'.
            protocol (str): A protocol name.
            scheduler (str): A scheduler name.

        Returns:
            str: A Xenon 1 scheme name.
        """
        if kind == 'files':
            xenon1_files_map = {
                    'file': 'file',
                    'sftp': 'sftp',
                    'ftp': 'ftp',
                    'webdav': 'http'
                    }
            return xenon1_files_map[protocol]
        elif kind == 'jobs':
            xenon1_jobs_map = {
                    ('local', 'none'): 'local',
                    ('ssh', 'none'): 'ssh',
                    ('ssh', 'slurm'): 'slurm',
                    ('ssh', 'torque'): 'torque',
                    ('ssh', 'gridengine'): 'ge'
                    }
            return xenon1_jobs_map[(protocol, scheduler)]

    def _get_xenon1_password(self, password):
        """
        Manually convert a Python string containing a password to a \
        JPype Java character array.

        JPype does this incorrectly when doing it automatically,
        leading to an incorrect password error. This works around
        that.

        Args:
            password (str): A string containing a password.
        Returns:
            jpype.JArray(jpype.JChar): The equivalent Java char array.
        """
        if password is None:
            return None
        jpassword = jpype.JArray(jpype.JChar)(len(password))
        for i, char in enumerate(password):
            jpassword[i] = char
        return jpassword

    def _get_credential(self, kind, protocol, scheduler):
        """
        Create a Xenon Credential given the configuration, and return
        it together with the username.

        Args:
            kind (str): Either 'files' or 'jobs'.
            protocol (str): The protocol to connect with.
            scheduler (str): The scheduler to use to start jobs.

        Returns:
            (xenon.Credential): The credential to use for connecting
        """
        username = self._get_credential_variable(kind, 'username')
        password = self._get_credential_variable(kind, 'password')
        certfile = self._get_credential_variable(kind, 'certfile')
        passphrase = self._get_credential_variable(kind, 'passphrase')

        # self._logger.debug('Creating credential using {} {}'.format(username, password))

        scheme = self._get_xenon1_scheme(kind, protocol, scheduler)
        jpassword = self._get_xenon1_password(password)

        if username and certfile and passphrase:
            credential = self._x.credentials().newCertificateCredential(
                    scheme, username, jpassword, None)
        elif username and certfile:
            credential = self._x.credentials().newCertificateCredential(
                    scheme, username, jpassword, None)
        elif username and password:
            credential = self._x.credentials().newPasswordCredential(
                    scheme, username, jpassword, None)
        elif username:
            # Wait for Xenon 2
            pass
        else:
            credential = self._x.credentials().getDefaultCredential(
                    scheme)

        return credential

    def get_service_host(self):
        """
        Return the host interface Cerise should listen on.

        Returns:
            str: The IP address of the interface to listen on.
        """
        rs_config = self._config.get('rest-service', {})
        return rs_config.get('hostname', '127.0.0.1')

    def get_service_port(self):
        """
        Return the port on which Cerise should listen.

        Returns:
            int: The port number to listen on.
        """
        rs_config = self._config.get('rest-service', {})
        return int(rs_config.get('port', 29593))

    def get_username(self, kind):
        """
        Return the username used to connect to the specified kind of resource.

        Args:
            kind (str): Either 'files' or 'jobs'

        Returns:
            (str): The configured username
        """
        return self._get_credential_variable(kind, 'username')

    def get_scheduler(self, run_on_head_node=False):
        """
        Returns a scheduler as configured by the user.

        Args:
            run_on_head_node (bool): If True, will create a scheduler using \
            the ssh adaptor instead of the configured one if the configured \
            adaptor is a cluster scheduler (i.e. slurm, torque or gridengine).

        Returns:
            (xenon.Scheduler): A new scheduler
        """
        if 'jobs' not in self._cr_config:
            protocol = 'local'
            location = None
            scheduler = 'none'
        else:
            protocol = self._cr_config['jobs'].get('protocol', 'local')
            location = self._cr_config['jobs'].get('location')
            scheduler = self._cr_config['jobs'].get('scheduler', 'none')

        if run_on_head_node:
            scheduler = 'none'

        scheme = self._get_xenon1_scheme('jobs', protocol, scheduler)
        credential = self._get_credential('jobs', protocol, scheduler)

        properties = jpype.java.util.HashMap()
        if scheduler == 'slurm':
            properties.put('xenon.adaptors.slurm.ignore.version', 'true')

        scheduler = self._x.jobs().newScheduler(
                scheme, location, credential, properties)
        return scheduler

    def get_file_system(self):
        """
        Returns a remote file system as configured by the user.

        Returns:
            (xenon.FileSystem): A new filesystem
        """
        if 'files' not in self._cr_config:
            protocol = 'file'
            location = None
        else:
            protocol = self._cr_config['files'].get('protocol', 'file')
            location = self._cr_config['files'].get('location')

        scheme = self._get_xenon1_scheme('files', protocol)
        credential = self._get_credential('files', protocol, location)
        self._logger.debug('scheme: {}, location: {}, credential: {}'.format(
                scheme, location, credential))
        filesystem = self._x.files().newFileSystem(
                scheme, location, credential, None)

        return filesystem

    def get_remote_cwl_runner(self):
        """
        Returns the configured remote path to the CWL runner to use.

        No macro substitution is done; this gives the configured path as-is.

        Returns:
            (str): The path.
        """
        default = '$CERISE_API_FILES/cerise/cwltiny.py'
        if 'jobs' not in self._cr_config:
            return default
        return self._cr_config['jobs'].get('cwl-runner', default)

    def get_basedir(self):
        """
        Returns the configured remote base directory to use.

        Returns:
            (str): The remote path to the base directory.
        """
        default = '/home/$CERISE_USERNAME/.cerise'
        if 'files' not in self._cr_config:
            return default
        return self._cr_config['files'].get('path', default)

    def get_queue_name(self):
        """
        Returns the name of the queue to submit jobs to, or None if no
        queue name was configured.

        Returns:
            (Union[str,None]): The queue name.
        """
        if 'jobs' not in self._cr_config:
            return None
        return self._cr_config['jobs'].get('queue-name')

    def get_slots_per_node(self):
        """
        Returns the configured number of MPI slots per node.

        Returns:
            (int): The number of slots to use.
        """
        default = 1
        if 'jobs' not in self._cr_config:
            return default
        return self._cr_config['jobs'].get('slots-per-node', default)

    def get_remote_refresh(self):
        """
        Returns the interval in between checks of the remote job \
        status, in seconds.

        Returns:
            (float): How often to check remote job status.
        """
        return self._cr_config.get('refresh', 60.0)

    def get_database_location(self):
        """
        Returns the local path to the database file.

        Returns:
            (str): The path.

        Raises:
            KeyError: No database path was set.
        """
        return self._config['database']['file']

    def get_pid_file(self):
        """
        Returns the location of the PID file, if any.

        Returns:
            (Union[str,None]): The configured path, or None
        """
        return self._config.get('pidfile')

    def has_logging(self):
        """
        Returns if logging is configured.

        Returns:
            (bool): True iff a logging section is available in the configuration.
        """
        return 'logging' in self._config

    def get_log_file(self):
        """
        Returns the configured path for the log file. Use has_logging()
        to see if logging has been configured first.

        Returns:
            (str): The path.
        """
        return self._config['logging'].get('file', '/var/log/cerise/cerise_backend.log')

    def get_log_level(self):
        """
        Returns the configured log level. Use has_logging() to see if
        logging has been configured first.

        Returns:
            (int): The log level, following Python's built-in logging \
                    library.
        """
        import logging
        loglevel_str = self._config['logging'].get('level', 'INFO')
        loglevel = getattr(logging, loglevel_str.upper(), None)
        return loglevel

    def get_store_location_service(self):
        """
        Returns the file exchange location access point for the service.

        Returns:
            (str): A URL.

        Raises:
            KeyError: The location was not set.
        """
        return self._config['client-file-exchange']['store-location-service']

    def get_store_location_client(self):
        """
        Returns the ile exchange location access point for the client.

        Returns:
            (str): A URL.

        Raises:
            KeyError: The location was not set.
        """
        return self._config['client-file-exchange']['store-location-client']

def make_config(xenon=None):
    """Make a configuration object.

    Uses the configuration files and environment variables to determine
    the configuration.

    Args:
        xenon (xenon.Xenon): A Xenon object.
    Returns:
        Config: The Cerise configuration.
    """
    config = None
    api_config = None

    config_file_path = 'conf/config.yml'
    with open(config_file_path) as config_file:
        config = yaml.safe_load(config_file)

    api_config_file_path = 'api/config.yml'
    with open(api_config_file_path) as api_config_file:
        api_config = yaml.safe_load(api_config_file)

    return Config(xenon, config, api_config)
