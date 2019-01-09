import logging
import os
import traceback
import urllib
from typing import Any, Dict, Optional, cast

import cerulean
import yaml

_remote_file_system = None


class Config:
    def __init__(self, config: Dict[str, Any],
                 api_config: Dict[str, Any]) -> None:
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
        self._cr_config = dict()  # type: Dict[str, Any]
        """The compute-resource part of the main config."""

        if 'compute-resource' in self._api_config:
            self._cr_config = cast(dict, self._api_config['compute-resource'])

    def _get_credential_variable(self, kind: str, name: str) -> Optional[str]:
        def have_config(kind: str, name: str) -> bool:
            if kind == '':
                return ('credentials' in self._cr_config
                        and name in self._cr_config['credentials'])
            return (kind in self._cr_config
                    and 'credentials' in self._cr_config[kind]
                    and name in self._cr_config[kind]['credentials'])

        def get_config(kind: str, name: str) -> str:
            if kind == '':
                return self._cr_config['credentials'][name]
            return self._cr_config[kind]['credentials'][name]

        def env_var(kind: str, name: str) -> str:
            if kind == '':
                return '_'.join(['CERISE', name.upper()])
            else:
                return '_'.join(['CERISE', kind.upper(), name.upper()])

        def have_env(kind: str, name: str) -> bool:
            return env_var(kind, name) in os.environ

        def get_env(kind: str, name: str) -> str:
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

    def _get_credential(self, kind: str) -> Optional[cerulean.Credential]:
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

        credential = None  # type: Optional[cerulean.Credential]
        if username and certfile:
            credential = cerulean.PubKeyCredential(username, certfile,
                                                   passphrase)
        elif username and password:
            credential = cerulean.PasswordCredential(username, password)

        return credential

    def close_file_systems(self) -> None:
        """Close any open connections and free resources.

        This function is to be called on shutdown, to ensure that the
        remote file system managed by Config is shut down properly.
        """
        global _remote_file_system
        if _remote_file_system is not None:
            _remote_file_system.close()

    def get_service_host(self) -> str:
        """
        Return the host interface Cerise should listen on.

        Returns:
            str: The IP address of the interface to listen on.
        """
        rs_config = self._config.get('rest-service', {})
        return rs_config.get('hostname', '127.0.0.1')

    def get_service_port(self) -> int:
        """
        Return the port on which Cerise should listen.

        Returns:
            int: The port number to listen on.
        """
        rs_config = self._config.get('rest-service', {})
        return int(rs_config.get('port', 29593))

    def get_username(self, kind: str) -> Optional[str]:
        """
        Return the username used to connect to the specified kind of resource.

        Args:
            kind (str): Either 'files' or 'jobs'

        Returns:
            (str): The configured username
        """
        return self._get_credential_variable(kind, 'username')

    def get_scheduler(self,
                      run_on_head_node: bool = False) -> cerulean.Scheduler:
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
            scheduler_type = 'directgnu'
        else:
            protocol = self._cr_config['jobs'].get('protocol', 'local')
            location = self._cr_config['jobs'].get('location')
            scheduler_type = self._cr_config['jobs'].get(
                'scheduler', 'directgnu')

        if run_on_head_node:
            scheduler_type = 'directgnu'

        credential = self._get_credential('jobs')
        terminal = cerulean.make_terminal(protocol, location, credential)
        scheduler = cerulean.make_scheduler(scheduler_type, terminal)
        return scheduler

    def get_file_system(self) -> cerulean.FileSystem:
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
            self._logger.debug(
                ('protocol: {}, location: {}, credential: {}').format(
                    protocol, location, credential))

            _remote_file_system = cerulean.make_file_system(
                protocol, location, credential)

        return _remote_file_system

    def get_remote_cwl_runner(self) -> str:
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

    def get_basedir(self) -> cerulean.Path:
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

    def get_queue_name(self) -> Optional[str]:
        """
        Returns the name of the queue to submit jobs to, or None if no
        queue name was configured.

        Returns:
            (Union[str,None]): The queue name.
        """
        if 'jobs' not in self._cr_config:
            return None
        return self._cr_config['jobs'].get('queue-name')

    def get_slots_per_node(self) -> int:
        """
        Returns the configured number of MPI slots per node.

        Returns:
            (int): The number of slots to use.
        """
        default = 1
        if 'jobs' not in self._cr_config:
            return default
        return self._cr_config['jobs'].get('slots-per-node', default)

    def get_scheduler_options(self) -> Optional[str]:
        """Returns the additional scheduler options to use.

        Returns:
            (str): The options as a single string.
        """
        if 'jobs' not in self._cr_config:
            return None
        return self._cr_config['jobs'].get('scheduler-options', None)

    def get_cores_per_node(self) -> int:
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

    def get_remote_refresh(self) -> float:
        """
        Returns the interval in between checks of the remote job \
        status, in seconds.

        Returns:
            (float): How often to check remote job status.
        """
        return self._cr_config.get('refresh', 60.0)

    def get_database_location(self) -> str:
        """
        Returns the local path to the database file.

        Returns:
            (str): The path.

        Raises:
            KeyError: No database path was set.
        """
        return self._config['database']['file']

    def get_pid_file(self) -> Optional[str]:
        """
        Returns the location of the PID file, if any.

        Returns:
            (Union[str,None]): The configured path, or None
        """
        return self._config.get('pidfile')

    def has_logging(self) -> bool:
        """
        Returns if logging is configured.

        Returns:
            True iff a logging section is available in the configuration.
        """
        return 'logging' in self._config

    def get_log_file(self) -> str:
        """
        Returns the configured path for the log file. Use has_logging()
        to see if logging has been configured first.

        Returns:
            (str): The path.
        """
        return self._config['logging'].get(
            'file', '/var/log/cerise/cerise_backend.log')

    def get_log_level(self) -> int:
        """
        Returns the configured log level. Use has_logging() to see if
        logging has been configured first.

        Returns:
            (int): The log level, following Python's built-in logging \
                    library.
        """
        import logging
        loglevel_str = 'INFO'
        if 'logging' in self._config:
            loglevel_str = self._config['logging'].get('level', 'INFO')
        if 'CERISE_LOG_LEVEL' in os.environ:
            loglevel_str = os.environ['CERISE_LOG_LEVEL'].strip()
        loglevel = getattr(logging, loglevel_str.upper(), None)
        return loglevel

    def get_base_url(self) -> str:
        """Returns the service's base url.

        This is the URL of the REST API, before the /jobs part, e.g. if
        listing jobs is done by a GET to http://localhost/jobs, then
        this should be set to http://localhost. Obtained from the
        configuration file or the CERISE_BASE_URL environment variable.
        """
        if 'CERISE_BASE_URL' in os.environ:
            return os.environ['CERISE_BASE_URL']
        rs_config = self._config.get('rest-service', {})
        return rs_config.get('base-url', '')

    def get_store_location_service(self) -> cerulean.Path:
        """
        Returns the file exchange location access point for the service.

        Returns:
            The local base directory for file exchange with the client.

        Raises:
            KeyError: The location was not set.
        """
        url = self._config['client-file-exchange']['store-location-service']
        urlparts = urllib.parse.urlparse(url)
        if urlparts.scheme == 'file':
            return cerulean.LocalFileSystem() / urlparts.path
        elif urlparts.scheme == 'http':
            return cerulean.WebdavFileSystem(url) / ''
        else:
            raise RuntimeError('Config store-location-service contains an'
                               ' invalid scheme. Use file:// or http://.')

    def get_store_location_client(self) -> str:
        """
        Returns the file exchange location access point for the client.

        Returns:
            (str): A URL.

        Raises:
            KeyError: The location was not set.
        """
        if 'CERISE_STORE_LOCATION_CLIENT' in os.environ:
            return os.environ['CERISE_STORE_LOCATION_CLIENT']
        return self._config['client-file-exchange'].get(
            'store-location-client')


def make_config() -> Config:
    """Make a configuration object.

    Uses the configuration files and environment variables to determine
    the configuration.

    Returns:
        Config: The Cerise configuration.
    """
    config_file_path = 'conf/config.yml'
    try:
        with open(config_file_path) as config_file:
            config = yaml.safe_load(config_file)
    except Exception:
        print("Could not load main configuration, aborting.")
        print("Does the file exist, and is it valid YAML?")
        print(traceback.format_exc())
        quit(1)

    api_config_file_path = 'api/config.yml'
    try:
        with open(api_config_file_path) as api_config_file:
            api_config = yaml.safe_load(api_config_file)
    except Exception:
        print("Could not load API configuration, aborting.")
        print("Does the file exist, and is it valid YAML?")
        print(traceback.format_exc())
        quit(1)

    return Config(config, api_config)
