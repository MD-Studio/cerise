import cerulean
import json
import logging
import os
import re
import yaml

from pathlib import Path

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

    def __init__(self, job_store, config):
        """Create a XenonRemoteFiles object.
        Sets up remote directory structure as well, but refuses to
        create the top-level directory.

        Args:
            job_store (JobStore): The job store to use.
            config (Config): The configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """JobStore: The job store to use."""
        self._fs = config.get_file_system()
        """cerulean.FileSystem: The Cerulean remote file system to stage to."""
        self._username = config.get_username('files')
        """str: The remote user name to use, if any."""
        self._basedir = None
        """cerulean.Path: The remote path to the base directory where we store our stuff."""
        self._api_files_dir = None
        """cerulean.Path: The remote path to the directory where the API files are."""
        self._api_steps_dir = None
        """cerulean.Path: The remote path to the directory where the API steps are."""
        self._local_fs = cerulean.LocalFileSystem()
        """Cerulean.FileSystem: Cerulean object for the local file system."""

        # Create directories if they don't exist
        self._logger.debug('username = {}'.format(self._username))
        if self._username is not None:
            self._basedir = self._fs / (config.get_basedir()
                        .replace('$CERISE_USERNAME', self._username)
                        .strip('/'))
        else:
            self._basedir = self._fs / config.get_basedir().strip('/')

        print('basedir: {}'.format(self._basedir))
        self._basedir.mkdir(0o750, parents=True, exists_ok=True)
        (self._basedir / 'jobs').mkdir(parents=True, exists_ok=True)

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
        remote_api_dir = self._basedir / 'api'
        self._logger.info('Staging API from {} to {}'.format(local_api_dir, remote_api_dir))
        remote_api_dir.mkdir(0o750, exists_ok=True)

        local_api_dir_path = self._local_fs / local_api_dir
        self._stage_api_files(local_api_dir_path, remote_api_dir)
        self._stage_api_steps(local_api_dir_path, remote_api_dir)
        remote_api_script_path = self._stage_install_script(local_api_dir_path, remote_api_dir)

        remote_api_files_dir = remote_api_dir / 'files'
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
            self._abs_path(job_id, '').mkdir(0o700, parents=True, exists_ok=True)
            self._abs_path(job_id, 'work').mkdir(0o700, parents=True, exists_ok=True)
            job.remote_workdir_path = str(self._abs_path(job_id, 'work'))

            # stage name of the job
            self._add_file_to_job(job_id, 'name.txt', job.name.encode('utf-8'))

            # stage workflow
            remote_workflow_content = self._translate_workflow(job.workflow_content)
            self._add_file_to_job(job_id, 'workflow.cwl', remote_workflow_content)
            job.remote_workflow_path = str(self._abs_path(job_id, 'workflow.cwl'))

            # stage input files
            inputs = json.loads(job.local_input)
            count = 1
            for input_file in input_files:
                if input_file.index is not None:
                    input_desc = inputs[input_file.name][input_file.index]
                else:
                    input_desc = inputs[input_file.name]
                count = self._stage_input_file(count, job_id, input_file, input_desc)

            # stage input description
            inputs_json = json.dumps(inputs).encode('utf-8')
            self._add_file_to_job(job_id, 'input.json', inputs_json)
            job.remote_input_path = str(self._abs_path(job_id, 'input.json'))

            # configure output
            job.remote_stdout_path = str(self._abs_path(job_id, 'stdout.txt'))
            job.remote_stderr_path = str(self._abs_path(job_id, 'stderr.txt'))

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
                for output_file in get_files_from_binding(outputs):
                    self._logger.debug('Destage path = {} for output {}'.format(
                        output_file.location, output_file.name))
                    prefix = 'file://' + str(self._basedir / 'jobs' / job_id / 'work') + '/'
                    if not output_file.location.startswith(prefix):
                        raise Exception("Unexpected output location in cwl-runner output: {}, expected it to start with: {}, {}".format(output_file.location, prefix, str(self._basedir._Path__path)))
                    rel_path = output_file.location[len(prefix):]
                    content = self._read_remote_file(job_id, 'work/' + rel_path)
                    output_files.append((output_file.name, rel_path, content))

        # output_name and rel_path are (immutable) str's, while content
        # does not come from the store, so we're not leaking here
        return output_files

    def delete_job(self, job_id):
        """Remove the work directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id (str): The id of the job whose work directory to delete.
        """
        self._abs_path(job_id, '').rmdir(recursive=True)

    def update_job(self, job_id):
        """Get status from remote resource and update store.

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

    def _translate_workflow(self, workflow_content):
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
            step['run'] = str(self._api_steps_dir / step['run'])
        return bytes(json.dumps(workflow), 'utf-8')

    def _stage_api_steps(self, local_api_dir, remote_api_dir):
        """Copy the CWL steps forming the API to the remote compute
        resource, replacing $CERISE_API_FILES at the start of a
        baseCommand and in arguments with the remote path to the files,
        and saving the result as JSON.
        """
        self._api_steps_dir = remote_api_dir / 'steps'
        self._api_steps_dir.mkdir(0o750, parents=True, exists_ok=True)

        local_steps_dir = local_api_dir / 'steps'

        for this_dir, _, files in local_steps_dir.walk():
            self._logger.debug('Scanning file for staging: ' + str(this_dir) + '/' + str(files))
            for filename in files:
                if filename.endswith('.cwl'):
                    cwlfile = self._translate_api_step(this_dir / filename)
                    # make parent directory
                    rel_this_dir = this_dir.relative_to(str(local_steps_dir))
                    remote_this_dir = remote_api_dir / 'steps' / str(rel_this_dir)
                    remote_this_dir.mkdir(0o700, parents=True, exists_ok=True)

                    # write it to remote
                    rem_file = remote_this_dir / filename
                    self._logger.debug('Staging step to {} from {}'.format(
                        rem_file, filename))
                    data = bytes(json.dumps(cwlfile), 'utf-8')
                    rem_file.write_bytes(data)

    def _translate_api_step(self, workflow_path):
        """Do CERISE_API_FILES macro substitution on an API step file.
        """
        cwlfile = yaml.safe_load(workflow_path.read_text())
        if cwlfile.get('class') == 'CommandLineTool':
            if 'baseCommand' in cwlfile:
                if cwlfile['baseCommand'].lstrip().startswith('$CERISE_API_FILES'):
                    cwlfile['baseCommand'] = cwlfile['baseCommand'].replace(
                            '$CERISE_API_FILES', str(self._api_files_dir), 1)

            if 'arguments' in cwlfile:
                if not isinstance(cwlfile['arguments'], list):
                    raise RuntimeError('Invalid step {}: arguments must be an array'.format(
                        filename))
                newargs = []
                for i, argument in enumerate(cwlfile['arguments']):
                    self._logger.debug("Processing argument {}".format(argument))
                    newargs.append(argument.replace(
                        '$CERISE_API_FILES', str(self._api_files_dir)))
                    self._logger.debug("Done processing argument {}".format(cwlfile['arguments'][i]))
                cwlfile['arguments'] = newargs
        return cwlfile

    def _stage_api_files(self, local_api_dir, remote_api_dir):
        self._api_files_dir = remote_api_dir / 'files'
        local_dir = local_api_dir / 'files'
        if not local_dir.exists():
            self._logger.debug('API files not found, not staging')
            return
        self._logger.debug('Staging API part to {} from {}'.format(
                self._api_files_dir, local_dir))
        cerulean.copy(local_dir, self._api_files_dir, overwrite='always',
                      copy_into=False, copy_permissions=True)

    def _stage_install_script(self, local_api_dir, remote_api_dir):
        local_path = local_api_dir / 'install.sh'
        if not local_path.exists():
            self._logger.debug('API install script not found, not staging')
            return None

        remote_path = remote_api_dir / 'install.sh'
        self._logger.debug('Staging API install script to {} from {}'.format(
            remote_path, local_path))
        cerulean.copy(local_path, remote_path, overwrite='always', copy_into=False)

        while not remote_path.exists():
            pass

        remote_path.chmod(0o700)
        return remote_path

    def _stage_input_file(self, count, job_id, input_file, input_desc):
        """Stage an input file. Copies the file to the remote resource.

        Uses count to create unique file names, returns the new count \
        (i.e. the next available number).

        Args:
            count (int): The next available unique count
            job_id (str): The job id to stage for
            input_file (InputFile): The input file to stage
            input_desc (dict): The input description whose location \
                    (and secondaryFiles) to update.

        Returns:
            (int) The updated count
        """
        self._logger.debug(type(input_file))
        staged_name = _create_input_filename(str(count).zfill(2), input_file.location)
        self._logger.debug('Staging input file {} to remote file {}'.format(
            input_file.location, staged_name))
        count += 1
        self._add_file_to_job(job_id, 'work/' + staged_name, input_file.content)
        input_desc['location'] = str(self._abs_path(job_id, 'work/' + staged_name))

        for i, secondary_file in enumerate(input_file.secondary_files):
            sec_input_desc = input_desc['secondaryFiles'][i]
            count = self._stage_input_file(count, job_id, secondary_file, sec_input_desc)

        return count

    def _add_file_to_job(self, job_id, rel_path, data):
        """Write a file on the remote resource containing the given raw data.

        Args:
            job_id (str): The id of the job to write data for
            rel_path (str): A path relative to the job's directory
            data (bytes): The data to write
        """
        remote_path = self._abs_path(job_id, rel_path)
        remote_path.write_bytes(data)

    def _read_remote_file(self, job_id, rel_path):
        """Read data from a remote file.

        Silently returns an empty result if the file does not exist.

        Args:
            job_id (str): A job from whose work dir a file is read
            rel_path (str): A path relative to the job's directory
        """
        try:
            return self._abs_path(job_id, rel_path).read_bytes()
        except FileNotFoundError:
            return bytes()

    def _abs_path(self, job_id, rel_path):
        """Return an absolute remote path given a job-relative path.

        Args:
            job_id (str): A job from whose dir a file is read
            rel_path (str): A a path relative to the job's directory
        """
        ret = self._basedir / 'jobs' / job_id
        if rel_path != '':
            ret /= rel_path
        return ret

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


