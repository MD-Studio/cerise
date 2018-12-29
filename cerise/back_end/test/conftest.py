import cerulean
import pytest


class MockConfig:
    def __init__(self, tmpdir):
        print(type(tmpdir))
        self._file_system = cerulean.LocalFileSystem()
        base_dir_path = str(tmpdir / 'remote_basedir')
        self._base_dir = self._file_system / base_dir_path
        self._exchange_path = str(tmpdir / 'local_exchange')

    def get_scheduler(self, run_on_head_node=False):
        term = cerulean.LocalTerminal()
        return cerulean.DirectGnuScheduler(term)

    def get_queue_name(self):
        return None

    def get_slots_per_node(self):
        return 1

    def get_cores_per_node(self):
        return 16

    def get_scheduler_options(self):
        return None

    def get_remote_cwl_runner(self):
        return '$CERISE_API/cerise/files/cwltiny.py'

    def get_file_system(self):
        return self._file_system

    def get_basedir(self):
        return self._base_dir

    def get_username(self, kind):
        return None

    def get_store_location_service(self):
        return 'file://{}'.format(self._exchange_path)

    def get_store_location_client(self):
        return 'http://example.com'


@pytest.fixture
def mock_config(tmpdir):
    return MockConfig(tmpdir)
