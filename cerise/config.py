import cerulean
import logging
import os
import yaml


_remote_file_system = None


class Config:
    def __init__(self, config, api_config):
        """Create a configuration object.

        Args:
            config (dict): A main configuration dict.
            api_config (dict): An API configuration dict.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
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

    def _get_credential(self, kind):
        """
        Create a Cerulean Credential given the configuration, and return
        it together with the username.

        Args:
            kind (str): Either 'files' or 'jobs'.

        Returns:
            (cerulean.Credential): The credential to use for connecting
        """
        username = self._get_credential_variable(kind, 'username')
        password = self._get_credential_variable(kind, 'password')
        certfile = self._get_credential_variable(kind, 'certfile')
        passphrase = self._get_credential_variable(kind, 'passphrase')

        if username and certfile:
            credential = cerulean.CertificateCredential(username, certfile, passphrase)
        elif username and password:
            credential = cerulean.PasswordCredential(username, password)
        else:
            credential = None

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
            (cerulean.Scheduler): A new scheduler
        """
        if 'jobs' not in self._cr_config:
            protocol = 'local'
            location = None
            scheduler = 'directgnu'
        else:
            protocol = self._cr_config['jobs'].get('protocol', 'local')
            location = self._cr_config['jobs'].get('location')
            scheduler = self._cr_config['jobs'].get('scheduler', 'directgnu')

        if run_on_head_node:
            scheduler = 'directgnu'

        credential = self._get_credential('jobs')
        terminal = cerulean.make_terminal(protocol, location, credential)
        scheduler = cerulean.make_scheduler(scheduler, terminal)
        return scheduler

    def get_file_system(self):
        """
        Returns a remote file system as configured by the user.

        Returns:
            (cerulean.FileSystem) A new filesystem
        """
        global _remote_file_system
        if _remote_file_system is None:
            if 'files' not in self._cr_config:
                protocol = 'local'
                location = None
            else:
                protocol = self._cr_config['files'].get('protocol', 'local')
                location = self._cr_config['files'].get('location')

            credential = self._get_credential('files')
            self._logger.debug(('protocol: {}, location: {}, credential: {}'
                    ).format(protocol, location, credential))

            _remote_file_system = cerulean.make_file_system(
                    protocol, location, credential)

        return _remote_file_system

    def get_remote_cwl_runner(self):
        """
        Returns the configured remote path to the CWL runner to use.

        No macro substitution is done; this gives the configured path as-is.

        Returns:
            (str): The path.
        """
        default = '$CERISE_API/cerise/files/cwltiny.py'
        if 'jobs' not in self._cr_config:
            return default
        return self._cr_config['jobs'].get('cwl-runner', default)

    def get_basedir(self):
        """
        Returns the configured remote base directory to use.

        Returns:
            (str): The remote path to the base directory.
        """
        basedir = '/home/$CERISE_USERNAME/.cerise'
        if 'files' in self._cr_config:
            basedir = self._cr_config['files'].get('path', basedir)

        username = self.get_username('files')
        if username is not None:
            basedir = basedir.replace('$CERISE_USERNAME', username)
        basedir = basedir.strip('/')
        return self.get_file_system() / basedir

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

    def get_scheduler_options(self):
        """Returns the additional scheduler options to use.

        Returns:
            (str): The options as a single string.
        """
        if 'jobs' not in self._cr_config:
            return None
        return self._cr_config['jobs'].get('scheduler-options', None)

    def get_cores_per_node(self):
        """Returns the number of cores per node.

        This depends on the available compute hardware, and should be
        configured in the specialisation. The incoming workflow
        specifies a number of cores, but we reserve nodes, so we need
        to convert.

        The default is 32, which is probably more than what you have,
        as a result of which we'll allocate fewer nodes than the user
        specified if no value is given. That'll slow things down, but
        at least we won't be burning core hours needlessly.

        Returns:
            (int): The number of cores per node on this machine.
        """
        if 'jobs' not in self._cr_config:
            return 32
        return self._cr_config['jobs'].get('cores-per-node', 32)

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
        Returns the file exchange location access point for the client.

        Returns:
            (str): A URL.

        Raises:
            KeyError: The location was not set.
        """
        if 'CERISE_STORE_LOCATION_CLIENT' in os.environ:
            return os.environ['CERISE_STORE_LOCATION_CLIENT']
        return self._config['client-file-exchange']['store-location-client']


def make_config():
    """Make a configuration object.

    Uses the configuration files and environment variables to determine
    the configuration.

    Returns:
        Config: The Cerise configuration.
    """
    config = None
    api_config = None

    config_file_path = 'conf/config.yml'
    try:
        with open(config_file_path) as config_file:
            config = yaml.safe_load(config_file)
    except:
        print("Could not load main configuration, aborting.")
        print("Does the file exist, and is it valid YAML?")
        print(traceback.format_exc())
        quit(1)

    api_config_file_path = 'api/config.yml'
    try:
        with open(api_config_file_path) as api_config_file:
            api_config = yaml.safe_load(api_config_file)
    except:
        print("Could not load API configuration, aborting.")
        print("Does the file exist, and is it valid YAML?")
        print(traceback.format_exc())
        quit(1)

    return Config(config, api_config)
