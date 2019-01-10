import cerulean
import pytest

from cerise.back_end.remote_api import RemoteApi
from cerise.test.fixture_jobs import BrokenJob, MissingInputJob, PassJob

lfs = cerulean.LocalFileSystem()


@pytest.fixture
def installed_api_dir(mock_config, local_api_dir):
    remote_api = RemoteApi(mock_config, lfs / str(local_api_dir))
    remote_api.install()
    return mock_config.get_basedir() / 'api'


@pytest.fixture
def remote_api(mock_config, local_api_dir):
    return RemoteApi(mock_config, lfs / str(local_api_dir))


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
    local_api_dir = (lfs / __file__).parent / 'api_new'
    remote_api_files = RemoteApi(mock_config, local_api_dir)

    assert remote_api_files.update_available()
    remote_api_files.install()
    assert not remote_api_files.update_available()

    test_files_dir = installed_api_dir / 'test' / 'files'
    assert not (test_files_dir / 'test' / 'test_file.txt').exists()
    assert (test_files_dir / 'test' / 'test_file2.txt').exists()


def test_dev_update(installed_api_dir, mock_config, local_api_dir):
    remote_api = RemoteApi(mock_config, lfs / str(local_api_dir))

    assert remote_api.update_available()
    remote_api.install()

    # check that it was run a second time
    assert (installed_api_dir / 'count.txt').read_text().strip() == '2'

    # check that it will keep reinstalling
    assert remote_api.update_available()


def test_get_projects(remote_api):
    print(remote_api.get_projects())
    assert 'test 0.0.0.dev' in remote_api.get_projects()
    assert 'cerise 0.0.0.dev' in remote_api.get_projects()


def test_translate_runner_location(installed_api_dir, mock_config, remote_api):
    remote_api_dir = mock_config.get_basedir() / 'api'
    location = remote_api.translate_runner_location(
        '$CERISE_API/project/files/cwltool.sh')
    assert location == '{}/project/files/cwltool.sh'.format(remote_api_dir)


def test_translate_workflow(mock_store_submitted, remote_api, mock_config):
    _, job_fixture = mock_store_submitted

    if job_fixture in [PassJob, MissingInputJob, BrokenJob]:
        with pytest.raises(RuntimeError):
            remote_api.translate_workflow(job_fixture.workflow)
    else:
        translated_json = remote_api.translate_workflow(job_fixture.workflow)
        assert str(mock_config.get_basedir()) in translated_json.decode()
