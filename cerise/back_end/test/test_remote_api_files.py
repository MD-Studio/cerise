from .mock_store import MockStore
from cerise.test.fixture_jobs import WcJob
from cerise.back_end.remote_api_files import RemoteApiFiles

from cerise.back_end.test.conftest import MockConfig

import cerulean

import os
import pytest


def test_stage_api(tmpdir):
    config = MockConfig(str(tmpdir))
    remote_api_files = RemoteApiFiles(config)

    local_api_dir = os.path.join(os.path.dirname(__file__), 'api')
    remote_api_files.stage_api(local_api_dir)

    # check that steps were staged
    assert os.path.isfile(os.path.join(
        str(tmpdir), 'api', 'steps', 'test', 'wc.cwl'))

    # check that install script was run
    assert os.path.isfile(os.path.join(
        str(tmpdir), 'api', 'files', 'test', 'test_file.txt'))
