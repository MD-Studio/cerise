import jpype
import json
import logging
import os
import re
import time
import xenon
import yaml

from xenon.files import OpenOption
from xenon.files import RelativePath

from simple_cwl_xenon_service.job_store.job_state import JobState
from .cwl import get_files_from_binding

Utils = xenon.nl.esciencecenter.xenon.util.Utils
PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException
NoSuchPathException = xenon.nl.esciencecenter.xenon.files.NoSuchPathException
CopyOption = xenon.nl.esciencecenter.xenon.files.CopyOption

class XenonRemoteFiles:
    """Manages a remote directory structure.
    Expects to be given a remote dir to work within. Inside this
    directory, it makes a jobs/ directory, and inside that there
    is a directory for every job.

    Within each job directory are the following files:

    - jobs/<job_id>/name.txt contains the user-given name of the job
    - jobs/<job_id>/workflow.cwl contains the workflow to run
    - jobs/<job_id>/work/ contains input and output files, and is the
      working directory for the job.
    - jobs/<job_id>/stdout.txt is the standard output of the CWL runner
    - jobs/<job_id>/stderr.txt is the standard error of the CWL runner
    """

    def __init__(self, job_store, x, xenon_config):
        """Create a XenonRemoteFiles object.
        Sets up remote directory structure as well, but refuses to
        create the top-level directory.

        Args:
            x (Xenon): The Xenon object to use.
            xenon_config (Dict): A dict containing key-value pairs with
                Xenon configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """JobStore: The job store to use."""
        self._x = x
        """Xenon: The Xenon instance to use."""
        self._fs = None
        """FileSystem: The Xenon remote file system to stage to."""
        self._basedir = xenon_config['files']['path']
        """str: The remote path to the base directory where we store our stuff."""
        self._stepsdir = None
        """str: The remote path to the directory where the API steps are."""
        self._local_fs = None
        """FileSystem: Xenon object for the local file system."""

        self._create_fss(xenon_config)

        # Create basedir if it doesn't exist
        self._basedir = self._basedir.rstrip('/')
        basedir_rel_path = RelativePath(self._basedir)
        basedir_full_path = self._x.files().newPath(self._fs, basedir_rel_path)
        try:
            self._x.files().createDirectories(basedir_full_path)
        except jpype.JException(PathAlreadyExistsException):
            pass

        # Create a subdirectory for jobs
        jobsdir_rel_path = RelativePath(self._basedir + '/jobs')
        jobsdir_full_path = self._x.files().newPath(self._fs, jobsdir_rel_path)
        try:
            self._x.files().createDirectories(jobsdir_full_path)
        except jpype.JException(PathAlreadyExistsException):
            pass

    def stage_api(self, local_api_dir):
        """Stage the API to the compute resource. Copies subdirectory
        steps/ of the given local api dir to the compute resource.

        Args:
            local_api_dir (str): The absolute local path of the api/
                directory to copy from
        """
        self._stepsdir = self._basedir + '/steps'
        x_stepsdir = self._x.files().newPath(self._fs, RelativePath(self._stepsdir))
        local_stepsdir = RelativePath(os.path.join(local_api_dir, 'steps'))
        local_stepsdir_path = self._x.files().newPath(self._local_fs, local_stepsdir)
        self._logger.debug('Staging API to ' + self._stepsdir + ' from ' + local_api_dir + '/steps')
        try:
            # self._x.files().createDirectories(x_stepsdir)
            Utils.recursiveCopy(
                    self._x.files(), local_stepsdir_path,
                    x_stepsdir, None)
            self._logger.debug('Staged API')

        except jpype.JException(PathAlreadyExistsException):
            pass

    def stage_job(self, job_id, input_files):
        """Stage a job. Copies any necessary files to
        the remote resource.

        Args:
            job_id (str): The id of the job to stage
        """
        self._logger.debug('Staging job ' + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)

            # create work dir
            self._make_remote_dir(job_id, '')
            self._make_remote_dir(job_id, 'work')
            job.remote_workdir_path = self._abs_path(job_id, 'work')

            # stage name of the job
            self._write_remote_file(job_id, 'name.txt', job.name.encode('utf-8'))

            # stage workflow
            remote_workflow_content = self._translate_steps(job.workflow_content)
            self._write_remote_file(job_id, 'workflow.cwl', remote_workflow_content)
            job.remote_workflow_path = self._abs_path(job_id, 'workflow.cwl')

            # stage input files
            inputs = json.loads(job.local_input)
            count = 1
            for name, location, content in input_files:
                staged_name = _create_input_filename(str(count).zfill(2), location)
                count += 1
                self._write_remote_file(job_id, 'work/' + staged_name, content)
                inputs[name]['location'] = self._abs_path(job_id, 'work/' + staged_name)

            # stage input description
            inputs_json = json.dumps(inputs).encode('utf-8')
            self._write_remote_file(job_id, 'input.json', inputs_json)
            job.remote_input_path = self._abs_path(job_id, 'input.json')

            # configure output
            job.remote_stdout_path = self._abs_path(job_id, 'stdout.txt')
            job.remote_stderr_path = self._abs_path(job_id, 'stderr.txt')

    def destage_job_output(self, job_id):
        """Download results of the given job from the compute resource.

        Args:
            job_id (str): The id of the job to download results of.

        Returns:
            List[str, str, bytes]: A list of (name, path, content) tuples.
        """
        self._logger.debug('Destaging job ' + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)
            self._logger.debug("Remote output" + job.remote_output)
            outputs = json.loads(job.remote_output)
            output_files = []
            for output_name, path in get_files_from_binding(outputs):
                self._logger.debug('Destage path = ' + path + ' for output ' + output_name)
                prefix = 'file://' + self._basedir + '/jobs/' + job_id + '/work/'
                if not path.startswith(prefix):
                    raise Exception("Unexpected output location in cwl-runner output: " + path
                            + ", expected it to start with: " + prefix)
                rel_path = path[len(prefix):]
                content = self._read_remote_file(job_id, 'work/' + rel_path)
                output_files.append((output_name, rel_path, content))

        # output_name and rel_path are (immutable) str's, while content
        # does not come from the store, so we're not leaking here
        return output_files

    def delete_job(self, job_id):
        """Remove the work directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id (str): The id of the job whose work directory to delete.
        """
        self._rm_remote_dir(job_id, '')

    def update_job(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id (str): ID of the job to get the status of.
        """
        self._logger.debug("Updating " + job_id + " from remote files")
        output_files = None
        with self._job_store:
            job = self._job_store.get_job(job_id)

            # get output
            output = self._read_remote_file(job_id, 'stdout.txt')
            if len(output) > 0:
                self._logger.debug("Output:")
                self._logger.debug(output)
                job.remote_output = output.decode()

            # get log
            log = self._read_remote_file(job_id, 'stderr.txt')
            if len(log) > 0:
                job.log = log.decode()
                self._logger.debug("Log:")
                self._logger.debug(job.log)

    def _translate_steps(self, workflow_content):
        workflow = yaml.safe_load(str(workflow_content, 'utf-8'))
        for _, step in workflow['steps'].items():
            if not isinstance(step['run'], str):
                raise RuntimeError('Invalid step in workflow')
            # check against known steps?
            step['run'] = self._stepsdir + '/' + step['run']
        return bytes(yaml.safe_dump(workflow), 'utf-8')

    def _make_remote_dir(self, job_id, rel_path):
        xenonpath = self._x_abs_path(job_id, rel_path)
        self._x.files().createDirectories(xenonpath)

    def _rm_remote_dir(self, job_id, rel_path):
        try:
            x_remote_path = self._x_abs_path(job_id, rel_path)
            self._x_recursive_delete(x_remote_path)
        except jpype.JException(NoSuchPathException):
            pass

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

    def _write_remote_file(self, job_id, rel_path, data):
        """Write a file on the remote resource containing the given raw data.

        Args:
            job_id (str): The id of the job to write data for
            rel_path (str): A path relative to the job's directory
            data (bytes): The data to write
        """
        x_remote_path = self._x_abs_path(job_id, rel_path)
        stream = self._x.files().newOutputStream(x_remote_path, [OpenOption.CREATE, OpenOption.TRUNCATE])
        stream.write(data)
        stream.close()

    def _read_remote_file(self, job_id, rel_path):
        """Read data from a remote file.

        Args:
            job_id (str): A job from whose work dir a file is read
            rel_path (str): A path relative to the job's directory
        """
        result = bytearray()

        x_remote_path = self._x_abs_path(job_id, rel_path)
        if self._x.files().exists(x_remote_path):
            stream = self._x.files().newInputStream(x_remote_path)
            buf = jpype.JArray(jpype.JByte)(1024)
            bytes_read = stream.read(buf)
            while bytes_read != -1:
                result = bytearray().join([result, bytearray(buf[0:bytes_read])])
                bytes_read = stream.read(buf)

        return result

    def _abs_path(self, job_id, rel_path):
        """Return an absolute remote path given a job-relative path.

        Args:
            job_id (str): A job from whose dir a file is read
            rel_path (str): A a path relative to the job's directory
        """
        ret = self._basedir + '/jobs/' + job_id
        if rel_path != '':
            ret += '/' + rel_path
        return ret

    def _x_abs_path(self, job_id, rel_path):
        """Return a Xenon Path object containing an absolute path
        corresponding to the given relative path.

        Args:
            job_id (str): A job from whose dir a file is read
            rel_path (str): A path relative to the job's directory

        Returns:
            Path: A Xenon Path object corresponding to the input
        """
        abs_path = self._abs_path(job_id, rel_path)
        xenon_path = xenon.files.RelativePath(abs_path)
        return self._x.files().newPath(self._fs, xenon_path)

    def _create_fss(self, xenon_config):
        """Create local and remote file systems.
        """
        self._local_fs = self._x.files().newFileSystem(
                'local', None, None, None)

        scheme = xenon_config['files'].get('scheme', 'local')
        location = xenon_config['files'].get('location', '')
        if 'username' in xenon_config['files']:
            username = xenon_config['files'].get('username')
            password = xenon_config['files'].get('password')
            jpassword = jpype.JArray(jpype.JChar)(len(password))
            for i in range(len(password)):
                jpassword[i] = password[i]
            credential = self._x.credentials().newPasswordCredential(
                    scheme, username, jpassword, None)
            self._fs = self._x.files().newFileSystem(
                    scheme, location, credential, None)
        else:
            self._fs = self._x.files().newFileSystem(
                    scheme, location, None, None)

def _create_input_filename(unique_prefix, orig_path):
    """Return a string containing a remote filename that
    resembles the original path this file was submitted with.

    Args:
        unique_prefix (str): A unique prefix, used to avoid collisions.
        orig_path (str): A string we will try to resemble to aid
            debugging.
    """
    result = orig_path

    result.replace('/', '_')
    result.replace('?', '_')
    result.replace('&', '_')
    result.replace('=', '_')

    regex = re.compile('[^a-zA-Z0-9_.-]+')
    result = regex.sub('_', result)

    if len(result) > 39:
        result = result[:18] + '___' + result[-18:]

    return unique_prefix + '_' + result


