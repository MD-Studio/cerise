from cerise.test.fixture_jobs import WcJob
from cerise.back_end.remote_api import RemoteApi

import cerulean

import os
from pathlib import Path
import pytest


@pytest.fixture
def installed_api_dir(mock_config, local_api_dir):
    remote_api = RemoteApi(mock_config, str(local_api_dir))
    remote_api.install()
    return mock_config.get_basedir() / 'api'


def test_install(installed_api_dir):
    # check that steps were staged
    test_steps_dir = installed_api_dir / 'test' / 'steps'
    assert (test_steps_dir / 'test' / 'wc.cwl').is_file()

    # check that install script was run
    test_files_dir = installed_api_dir / 'test' / 'files'
    assert (test_files_dir / 'test_file.txt').is_file()

    # check that it was run once
    assert (installed_api_dir / 'count.txt').read_text().strip() == '1'


def test_update(installed_api_dir, mock_config):
    local_api_dir = Path(__file__).parent / 'api_new'
    remote_api_files = RemoteApi(mock_config, str(local_api_dir))

    assert remote_api_files.update_available()
    remote_api_files.install()
    assert not remote_api_files.update_available()

    test_files_dir = installed_api_dir / 'test' / 'files'
    assert not (test_files_dir / 'test' / 'test_file.txt').exists()
    assert (test_files_dir / 'test' / 'test_file2.txt').exists()


def test_dev_update(installed_api_dir, mock_config, local_api_dir):
    remote_api_files = RemoteApi(mock_config, str(local_api_dir))

    assert remote_api_files.update_available()
    remote_api_files.install()

    # check that it was run a second time
    assert (installed_api_dir / 'count.txt').read_text().strip() == '2'

    # check that it will keep reinstalling
    assert remote_api_files.update_available()
