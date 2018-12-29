from .mock_store import MockStore
from cerise.back_end.test.fixture_jobs import WcJob
from cerise.back_end.remote_api import RemoteApi

import cerulean

import os
import pytest


@pytest.fixture
def installation(tmpdir, mock_config):
    local_api_dir = os.path.join(os.path.dirname(__file__), 'api')
    remote_api_files = RemoteApi(mock_config, local_api_dir)
    remote_api_files.install()
    return mock_config.get_basedir()


def test_install(installation):
    # check that steps were staged
    assert (installation / 'api' / 'test' / 'steps' / 'test' /
            'wc.cwl').is_file()

    # check that install script was run
    assert (installation / 'api' / 'test' / 'files' / 'test' /
            'test_file.txt').is_file()

    # check that it was run once
    assert ((installation / 'api' / 'count.txt'
            ).read_text('utf-8').strip() == '1')


def test_update(installation, mock_config):
    local_api_dir = os.path.join(os.path.dirname(__file__), 'api_new')
    remote_api_files = RemoteApi(mock_config, local_api_dir)

    assert remote_api_files.update_available()
    remote_api_files.install()
    assert not remote_api_files.update_available()

    assert not (installation / 'api' / 'test' / 'files' / 'test' /
            'test_file.txt').exists()
    assert (installation / 'api' / 'test' / 'files' / 'test' /
            'test_file2.txt').exists()


def test_dev_update(installation, mock_config):
    local_api_dir = os.path.join(os.path.dirname(__file__), 'api')
    remote_api_files = RemoteApi(mock_config, local_api_dir)

    assert remote_api_files.update_available()
    remote_api_files.install()

    # check that it was run a second time
    assert ((installation / 'api' / 'count.txt'
            ).read_text('utf-8').strip() == '2')

    # check that it will keep reinstalling
    assert remote_api_files.update_available()
