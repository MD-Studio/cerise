import jpype
import json
import logging
import os
import re
import xenon
import yaml

from .cwl import get_files_from_binding

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
        from xenon.files import RelativePath
        PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException

        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """JobStore: The job store to use."""
        self._x = x
        """Xenon: The Xenon instance to use."""
        self._fs = None
        """FileSystem: The Xenon remote file system to stage to."""
        self._username = None
        """str: The remote user name to use, if any."""
        self._basedir = xenon_config['files']['path']
        """str: The remote path to the base directory where we store our stuff."""
        self._api_files_dir = None
        """str: The remote path to the directory where the API files are."""
        self._api_steps_dir = None
        """str: The remote path to the directory where the API steps are."""
        self._local_fs = None
        """FileSystem: Xenon object for the local file system."""

        self._create_fss(xenon_config)

        # Create basedir if it doesn't exist
        if self._username is not None:
            self._basedir = self._basedir.replace('$CERISE_USERNAME', self._username)
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

        Returns:
            (str, str): The remote path to the api install script, and
                the remote path to the api files/ directory.
        """
        from xenon.files import RelativePath
        PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException

        remote_api_dir = self._basedir + '/api'
        x_remote_api_dir = self._x.files().newPath(self._fs, RelativePath(remote_api_dir))
        try:
            self._x.files().createDirectories(x_remote_api_dir)
        except jpype.JException(PathAlreadyExistsException):
            pass

        self._stage_api_files(local_api_dir, remote_api_dir)
        self._stage_api_steps(local_api_dir, remote_api_dir)
        remote_api_script_path = self._stage_install_script(local_api_dir, remote_api_dir)

        remote_api_files_dir = remote_api_dir + '/files'
        return remote_api_script_path, remote_api_files_dir

    def stage_job(self, job_id, input_files):
        """Stage a job. Copies any necessary files to
        the remote resource.

        Args:
            job_id (str): The id of the job to stage
            input_files ([InputFile]): A list of input files to stage.
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
            for input_file in input_files:
                staged_name = _create_input_filename(str(count).zfill(2), input_file.location)
                count += 1
                self._write_remote_file(job_id, 'work/' + staged_name, input_file.content)
                inputs[input_file.name]['location'] = \
                        self._abs_path(job_id, 'work/' + staged_name)

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
        output_files = []
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if job.remote_output != '':
                self._logger.debug("Remote output" + job.remote_output)
                outputs = json.loads(job.remote_output)
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
        """Parse workflow content, check that it calls steps, and
        insert the location of the steps on the remote resource so that
        the remote runner can find them.

        Args:
            workflow_content (bytes): The raw workflow data

        Returns:
            bytes: The modified workflow data, serialised as JSON

        """
        workflow = yaml.safe_load(str(workflow_content, 'utf-8'))
        for _, step in workflow['steps'].items():
            if not isinstance(step['run'], str):
                raise RuntimeError('Invalid step in workflow')
            # check against known steps?
            step['run'] = self._api_steps_dir + '/' + step['run']
        return bytes(json.dumps(workflow), 'utf-8')

    def _stage_api_steps(self, local_api_dir, remote_api_dir):
        """Copy the CWL steps forming the API to the remote compute
        resource, replacing $CERISE_API_FILES at the start of a
        baseCommand and in arguments with the remote path to the files,
        and saving the result as JSON.
        """
        from xenon.files import OpenOption
        from xenon.files import RelativePath
        PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException

        try:
            self._api_steps_dir = remote_api_dir + '/steps'
            x_remote_steps_dir = self._x.files().newPath(self._fs, RelativePath(self._api_steps_dir))
            self._x.files().createDirectories(x_remote_steps_dir)

            local_steps_dir = os.path.join(local_api_dir, 'steps')
            for this_dir, _, files in os.walk(local_steps_dir):
                self._logger.debug('Scanning file for staging: ' + this_dir + '/' + str(files))
                for filename in files:
                    if filename.endswith('.cwl'):
                        cwlfile = yaml.safe_load(open(os.path.join(this_dir, filename), 'r'))
                        # do CERISE_API_FILES macro substitution
                        if cwlfile.get('class') == 'CommandLineTool':
                            if 'baseCommand' in cwlfile:
                                if cwlfile['baseCommand'].lstrip().startswith('$CERISE_API_FILES'):
                                    cwlfile['baseCommand'] = cwlfile['baseCommand'].replace(
                                            '$CERISE_API_FILES', self._api_files_dir, 1)

                            if 'arguments' in cwlfile:
                                if not isinstance(cwlfile['arguments'], list):
                                    raise RuntimeError('Invalid step ' + filename + ': arguments must be an array')
                                newargs = []
                                for i, argument in enumerate(cwlfile['arguments']):
                                    self._logger.debug("Processing argument " + argument)
                                    newargs.append(argument.replace(
                                            '$CERISE_API_FILES', self._api_files_dir))
                                    self._logger.debug("Done processing argument " + cwlfile['arguments'][i])
                                cwlfile['arguments'] = newargs

                        # make parent directory
                        rel_this_dir = os.path.relpath(this_dir, start=local_steps_dir)
                        remote_this_dir = remote_api_dir + '/steps/' + rel_this_dir
                        try:
                            x_this_dir = self._x.files().newPath(self._fs, RelativePath(remote_this_dir))
                            self._x.files().createDirectories(x_this_dir)
                        except jpype.JException(PathAlreadyExistsException):
                            pass

                        # write it to remote
                        rem_file = remote_this_dir + '/' + filename
                        self._logger.debug('Staging step to ' + rem_file + ' from ' + filename)
                        x_rel_file = self._x.files().newPath(self._fs, RelativePath(rem_file))
                        data = bytes(json.dumps(cwlfile), 'utf-8')
                        stream = self._x.files().newOutputStream(x_rel_file,
                                [OpenOption.CREATE, OpenOption.TRUNCATE])
                        stream.write(data)
                        stream.close()

        except jpype.JException(PathAlreadyExistsException):
            pass

    def _stage_api_files(self, local_api_dir, remote_api_dir):
        PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException

        self._api_files_dir = remote_api_dir + '/files'
        local_dir = os.path.join(local_api_dir, 'files')
        if not os.path.isdir(local_dir):
            self._logger.debug('API files not found, not staging')
            return
        self._logger.debug('Staging API part to ' + self._api_files_dir + ' from ' + local_dir)
        try:
            self._copy_dir(local_dir, self._api_files_dir)
        except jpype.JException(PathAlreadyExistsException):
            pass

    def _stage_install_script(self, local_api_dir, remote_api_dir):
        from xenon.files import CopyOption
        from xenon.files import RelativePath
        PathAlreadyExistsException = xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException

        local_path = os.path.join(local_api_dir, 'install.sh')
        if not os.path.isfile(local_path):
            self._logger.debug('API install script not found, not staging')
            return None

        remote_path = remote_api_dir + '/install.sh'
        self._logger.debug('Staging API install script to ' + remote_path + ' from ' + local_path)
        x_local_path = self._x.files().newPath(self._local_fs, RelativePath(local_path))
        x_remote_path = self._x.files().newPath(self._fs, RelativePath(remote_path))
        if self._x.files().exists(x_remote_path):
            self._x.files().delete(x_remote_path)
        # CopyOption.REMOVE doesn't overwrite but fails?
        self._x.files().copy(x_local_path, x_remote_path, [])

        self._make_remote_file_executable(remote_path)

        return remote_path

    def _make_remote_dir(self, job_id, rel_path):
        xenonpath = self._x_abs_path(job_id, rel_path)
        self._x.files().createDirectories(xenonpath)

    def _rm_remote_dir(self, job_id, rel_path):
        NoSuchPathException = xenon.nl.esciencecenter.xenon.files.NoSuchPathException

        try:
            x_remote_path = self._x_abs_path(job_id, rel_path)
            self._x_recursive_delete(x_remote_path)
        except jpype.JException(NoSuchPathException):
            pass

    def _copy_dir(self, local_dir, remote_dir):
        """Copy a directory and all its contents from the local filesystem
        to the remote filesystem.

        Args:
            local_dir (str): The absolute local path of the directory
            to copy.
            remote_dir (str): The absolute remote path to copy it to
        """
        from xenon.files import RelativePath
        Utils = xenon.nl.esciencecenter.xenon.util.Utils

        x_remote_dir = self._x.files().newPath(self._fs, RelativePath(remote_dir))
        rel_local_dir = RelativePath(local_dir)
        x_local_dir = self._x.files().newPath(self._local_fs, rel_local_dir)
        Utils.recursiveCopy(
                self._x.files(), x_local_dir,
                x_remote_dir, None)

        # patch up execute permissions
        for this_dir, _, files in os.walk(local_dir):
            for filename in files:
                if os.access(os.path.join(this_dir, filename), os.X_OK):
                    rel_this_dir = os.path.relpath(this_dir, start=local_dir)
                    remote_this_dir = remote_dir + '/' + rel_this_dir
                    rem_file = remote_this_dir + '/' + filename
                    self._make_remote_file_executable(rem_file)

    def _make_remote_file_executable(self, remote_path):
        from xenon.files import RelativePath
        PosixFilePermission = xenon.nl.esciencecenter.xenon.files.PosixFilePermission

        x_rel_file = self._x.files().newPath(self._fs, RelativePath(remote_path))
        owner_read_execute = jpype.java.util.HashSet()
        owner_read_execute.add(PosixFilePermission.OWNER_READ)
        owner_read_execute.add(PosixFilePermission.OWNER_EXECUTE)
        self._x.files().setPosixFilePermissions(x_rel_file,
                owner_read_execute)

    def _x_recursive_delete(self, x_remote_path):
        Utils = xenon.nl.esciencecenter.xenon.util.Utils
        # Xenon throws an exception if the path does not exist
        if self._x.files().exists(x_remote_path):
            Utils.recursiveDelete(self._x.files(), x_remote_path)
        return

    def _write_remote_file(self, job_id, rel_path, data):
        """Write a file on the remote resource containing the given raw data.

        Args:
            job_id (str): The id of the job to write data for
            rel_path (str): A path relative to the job's directory
            data (bytes): The data to write
        """
        from xenon.files import OpenOption

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

        def sbyte_to_ubyte(buf, size):
            ret = bytearray(size)
            for i, val in enumerate(buf[0:size]):
                if val >= 0:
                    ret[i] = val
                else:
                    ret[i] = val + 256
            return ret

        x_remote_path = self._x_abs_path(job_id, rel_path)
        if self._x.files().exists(x_remote_path):
            stream = self._x.files().newInputStream(x_remote_path)
            buf = jpype.JArray(jpype.JByte)(1024)
            bytes_read = stream.read(buf)
            while bytes_read != -1:
                result = bytearray().join([result, sbyte_to_ubyte(buf, bytes_read)])
                bytes_read = stream.read(buf)
            stream.close()

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
        from xenon.files import RelativePath

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

        username = None
        password = None
        if 'username' in xenon_config['files']:
            username = xenon_config['files'].get('username')
            password = xenon_config['files'].get('password')
        if 'CERISE_USERNAME' in os.environ:
            username = os.environ['CERISE_USERNAME']
            password = os.environ.get('CERISE_PASSWORD', '')
        if 'CERISE_JOBS_USERNAME' in os.environ:
            username = os.environ['CERISE_JOBS_USERNAME']
            password = os.environ.get('CERISE_JOBS_PASSWORD', '')

        if username is not None:
            self._username = username
            jpassword = jpype.JArray(jpype.JChar)(len(password))
            for i, char in enumerate(password):
                jpassword[i] = char
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


