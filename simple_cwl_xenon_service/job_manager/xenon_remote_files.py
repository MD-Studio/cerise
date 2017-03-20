import jpype
import xenon
from xenon.files import OpenOption

from .job_state import JobState

class XenonRemoteFiles:
    def __init__(self, xenon, xenon_config={}):
        """Create a XenonRemoteFiles object.
        Sets up remote directory structure as well, but refuses to
        create the top-level directory.

        Args:
            xenon: The Xenon object to use.
            xenon_config: A dict containing key-value pairs with Xenon
                configuration.
        """
        self._x = xenon
        """The Xenon instance to use."""
        self._fs = self._x.files().newFileSystem(
                xenon_config['files'].get('scheme', 'local'),
                xenon_config['files'].get('location'),
                xenon_config['files'].get('credential'),
                xenon_config['files'].get('properties')
                )
        """The Xenon remote file system to stage to."""
        self._basedir = xenon_config['files']['path']
        """The remote path to the base directory where we store our stuff."""

        basedirpath = self._make_xenon_path('')
        if not self._x.files().exists(basedirpath):
            raise RuntimeError(('Configuration error: Base directory {} ' +
                'not found on remote file system').format(basedirpath))

        self._make_remote_dir('jobs', True)

    def create_work_dir(self, job_id):
        """Create a work directory for a job.

        Args:
            job_id: The id of the job to make a work directory for.
        """
        self._make_remote_dir('jobs/' + job_id + '/work')

    def get_work_dir_path(self, job_id):
        """Return the job's work directory as an absolute path on the compute resource.

        Args:
            job_id: The id of the job whose work directory to return
        """
        return self._to_remote_path('jobs/' + job_id + '/work')

    def remove_work_dir(self, job_id):
        """Remove the work directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id: The id of the job whose work directory to delete.
        """
        self._rm_remote_dir('jobs/' + job_id, True)

    def get_remote_file_path(self, job_id, rel_path):
        """Get a corresponding absolute path on the compute resource.

        Args:
            job_id: The of the job that is the context for this path
            rel_path: A string with a path relative to the job's directory
        """
        return self._to_remote_path('jobs/' + job_id + '/' + rel_path)

    def write_to_file(self, job_id, rel_path, data):
        """Write the data to a remote file.

        Args:
            job_id: The id of the job to write data for
            rel_path: A string with a path relative to the job's directory
            data: A bytes-object containing the data to write
        """
        self._write_remote_file('jobs/' + job_id + '/' + rel_path, data)

    def read_from_file(self, job_id, rel_path):
        """Read data from a remote file.

        Args:
            job_id: A job from whose work dir a file is read
            rel_path: A string with a path relative to the job's directory
        """
        return self._read_remote_file('jobs/' + job_id + '/' + rel_path)

    def _make_remote_dir(self, rel_path, existing_ok=False):
        try:
            xenonpath = self._make_xenon_path(rel_path)
            self._x.files().createDirectories(xenonpath)
        except jpype.JException(xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException):
            if not existing_ok:
                raise
            else:
                pass

    def _rm_remote_dir(self, rel_path, recursive):
        x_remote_path = self._make_xenon_path(rel_path)
        if recursive:
            self._x_recursive_delete(x_remote_path)
        else:
            self._x.files().delete(x_remote_path)

    def _x_recursive_delete(self, x_remote_path):
        x_dir = self._x.files().newAttributesDirectoryStream(x_remote_path)
        x_dir_it = x_dir.iterator()
        while x_dir_it.hasNext():
            x_path_attr = x_dir_it.next()
            if x_path_attr.attributes().isDirectory():
                self._x_recursive_delete(x_path_attr.path())
            else:
                self._x.files().delete(x_path_attr.path())
        self._x.files().delete(x_remote_path)

    def _write_remote_file(self, rel_path, data):
        """Write a file on the remote resource containing the given raw data.
        Args:
            rel_path A string containing a relative remote path
            data A bytes-type object containing the data to write
        """
        x_remote_path = self._make_xenon_path(rel_path)
        stream = self._x.files().newOutputStream(x_remote_path, [OpenOption.CREATE, OpenOption.TRUNCATE])
        stream.write(data)
        stream.close()

    def _read_remote_file(self, rel_path):
        result = bytearray()

        x_remote_path = self._make_xenon_path(rel_path)
        if self._x.files().exists(x_remote_path):
            stream = self._x.files().newInputStream(x_remote_path)
            buf = jpype.JArray(jpype.JByte)(1024)
            bytes_read = stream.read(buf)
            while bytes_read != -1:
                result = bytearray().join([result, bytearray(buf[0:bytes_read])])
                bytes_read = stream.read(buf)

        return result

    def _to_remote_path(self, rel_path):
        return self._basedir + '/' + rel_path

    def _make_xenon_path(self, rel_path):
        remote_path = self._to_remote_path(rel_path)
        xenon_path = xenon.files.RelativePath(remote_path)
        return self._x.files().newPath(self._fs, xenon_path)
