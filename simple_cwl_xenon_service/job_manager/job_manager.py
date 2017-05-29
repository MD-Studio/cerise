from .in_memory_job_store import InMemoryJobStore
from .sqlite_job_store import SQLiteJobStore
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

class JobManager:
    def __init__(self, config, xenon):
        # _job_store = InMemoryJobStore()
        self._job_store = SQLiteJobStore(config['database']['file'])
        self._local_files = LocalFiles(self._job_store, config['client-file-exchange'])
        self._remote_files = XenonRemoteFiles(self._job_store, xenon, config['compute-resource'])
        self._job_runner = XenonJobRunner(self._job_store, xenon, config['compute-resource'])

    def job_store(self):
        return self._job_store

    def local_files(self):
        return self._local_files

    def remote_files(self):
        return self._remote_files

    def job_runner(self):
        return self._job_runner
