import cerulean


class MockConfig:
    def __init__(self, remote_dir):
        self._remote_dir = remote_dir

    def get_scheduler(self, run_on_head_node=False):
        term = cerulean.LocalTerminal()
        return cerulean.DirectGnuScheduler(term)

    def get_file_system(self):
        return cerulean.LocalFileSystem()

    def get_basedir(self):
        return self.get_file_system() / self._remote_dir.strip('/')

    def get_username(self, kind):
        return None
