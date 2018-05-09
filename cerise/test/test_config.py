import cerise.config as config
from cerise.test.xenon import xenon_init

import logging
import pytest
import xenon


@pytest.fixture
def config_0(xenon_init):
    return config.Config({}, {})

@pytest.fixture
def config_1(xenon_init):
    test_config = {
            'database': {
                'file': 'test/database.db'
                },
            'logging': {
                'file': 'test/logfile.txt',
                'level': 'INFO'
                },
            'pidfile': 'test/cerise.pid',
            'client-file-exchange': {
                'store-location-service': 'file:///tmp/cerise_files',
                'store-location-client': 'file:///tmp/cerise_files2'
                },
            'rest-service': {
                'hostname': '192.168.0.1',
                'port': 29594
                }
            }

    test_api_config = {
            'compute-resource': {
                'credentials': {
                    'username': 'test_user',
                    'password': 'test_password',
                    'certfile': 'test_certificate',
                    'passphrase': 'test_passphrase'
                },
                'refresh': 1.0,
                'files': {
                    'protocol': 'sftp',
                    'location': 'example.com',
                    'path': '/scratch/$CERISE_USERNAME/.cerise'
                },
                'jobs': {
                    'protocol': 'ssh',
                    'location': 'example.com',
                    'scheduler': 'slurm',
                    'cwl-runner': '$CERISE_API_FILES/myfiles/cwltool.sh',
                    'queue-name': 'test_queue',
                    'slots-per-node': 4
                }
            }
        }

    return config.Config(test_config, test_api_config)

def test_create_config(config_0, config_1):
    pass

def test_get_service_host(config_0, config_1):
    assert config_0.get_service_host() == '127.0.0.1'
    assert config_1.get_service_host() == '192.168.0.1'

def test_get_service_port(config_0, config_1):
    assert config_0.get_service_port() == 29593
    assert config_1.get_service_port() == 29594

def test_get_username(config_0, config_1):
    assert config_0.get_username('files') is None
    assert config_0.get_username('jobs') is None
    assert config_1.get_username('files') == 'test_user'
    assert config_1.get_username('jobs') == 'test_user'

def test_get_remote_cwl_runner(config_0, config_1):
    assert config_0.get_remote_cwl_runner() == '$CERISE_API_FILES/cerise/cwltiny.py'
    assert config_1.get_remote_cwl_runner() == '$CERISE_API_FILES/myfiles/cwltool.sh'

def test_get_basedir(config_0, config_1):
    assert config_0.get_basedir() == '/home/$CERISE_USERNAME/.cerise'
    assert config_1.get_basedir() == '/scratch/$CERISE_USERNAME/.cerise'

def test_get_queue_name(config_0, config_1):
    assert config_0.get_queue_name() is None
    assert config_1.get_queue_name() == 'test_queue'

def test_get_slots_per_node(config_0, config_1):
    assert config_0.get_slots_per_node() is 1
    assert config_1.get_slots_per_node() == 4

def test_get_remote_refresh(config_0, config_1):
    assert config_0.get_remote_refresh() == 60.0
    assert config_1.get_remote_refresh() == 1.0

def test_get_database_location(config_0, config_1):
    with pytest.raises(KeyError):
        config_0.get_database_location()
    assert config_1.get_database_location() == 'test/database.db'

def test_get_pid_file(config_0, config_1):
    assert config_0.get_pid_file() is None
    assert config_1.get_pid_file() == 'test/cerise.pid'

def test_has_logging(config_0, config_1):
    assert not config_0.has_logging()
    assert config_1.has_logging()

def test_get_log_file(config_0, config_1):
    assert config_1.get_log_file() == 'test/logfile.txt'

def test_get_log_level(config_0, config_1):
    assert config_1.get_log_level() == logging.INFO

def test_get_store_location_service(config_0, config_1):
    with pytest.raises(KeyError):
        config_0.get_store_location_service()
    assert config_1.get_store_location_service() == 'file:///tmp/cerise_files'

def test_get_store_location_client(config_0, config_1):
    with pytest.raises(KeyError):
        config_0.get_store_location_client()
    assert config_1.get_store_location_client() == 'file:///tmp/cerise_files2'
