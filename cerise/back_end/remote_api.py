import cerulean
import json
import logging
import os
import re
import yaml

from .cwl import get_files_from_binding, get_required_num_cores


class RemoteApi:
    """Manages the remote API installation.

    This class manages the remote directories in which the CWL API is
    installed:

    <project>/<api_major_version>/steps/
    <project>/<api_major_version>/files/
    <project>/<api_major_version>/install.sh
    """

    def __init__(self, config):
        """Create a RemoteApiFiles object.
        Sets up remote directory structure as well, but refuses to
        create the top-level directory.

        Args:
            config (Config): The configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._local_fs = cerulean.LocalFileSystem()
        """Cerulean.FileSystem: Cerulean object for the local file system."""
        self._fs = config.get_file_system()
        """cerulean.FileSystem: The Cerulean remote file system to stage to."""
        self._sched = config.get_scheduler(run_on_head_node=True)
        """cerulean.Scheduler: Scheduler for running install script."""
        self._username = config.get_username('files')
        """str: The remote user name to use, if any."""
        self._basedir = config.get_basedir()
        """cerulean.Path: The remote path to the base directory where we store our stuff."""
        self._api_dir = self._basedir / 'api'
        """cerulean.Path: The remote path to the base directory where we store our stuff."""
        self._files_dir = self._basedir / 'api' / 'files'
        """cerulean.Path: The remote path to the directory where the API files are."""
        self._steps_dir = self._basedir / 'api' / 'steps'
        """cerulean.Path: The remote path to the directory where the API steps are."""
        self._steps_requirements = dict()  # type: Dict[Dict[str, int]]
        """Resource requirements for each loaded step."""

        print('basedir: {}'.format(self._basedir))
        self._api_dir.mkdir(0o750, parents=True, exists_ok=True)
        self._files_dir.mkdir(0o750, exists_ok=True)
        self._steps_dir.mkdir(0o750, exists_ok=True)

    def install(self, local_api_dir):
        """Install the API onto the compute resource.

        Copies subdirectories steps/ and files/ of the given local api
        dir to the compute resource, copies files/ to the compute
        resource, and runs the install script.

        Args:
            local_api_dir (str): The absolute local path of the api/
                directory to copy from

        Returns:
            The remote path of the api files and steps directories
        """
        self._logger.info('Staging API from {} to {}'.format(local_api_dir, self._api_dir))

        local_api_dir_path = self._local_fs / local_api_dir
        self._stage_api_files(local_api_dir_path)
        self._stage_api_steps(local_api_dir_path)
        remote_api_script_path = self._stage_install_script(local_api_dir_path)
        self._run_install_script(remote_api_script_path)
        return self._files_dir, self._steps_dir

    def translate_workflow(self, workflow_content):
        """Parse workflow content, check that it calls steps, and
        insert the location of the steps on the remote resource so that
        the remote runner can find them.

        Also converts YAML to JSON, for cwltiny compatibility.

        Args:
            workflow_content (bytes): The raw workflow data

        Returns:
            bytes: The modified workflow data, serialised as JSON

        """
        workflow = yaml.safe_load(str(workflow_content, 'utf-8'))
        if not 'steps' in workflow:
            raise RuntimeError('Workflow contains no steps')
        for _, step in workflow['steps'].items():
            if not isinstance(step['run'], str):
                raise RuntimeError('Invalid step in workflow')
            step['run'] = str(self._steps_dir / step['run'])
        return bytes(json.dumps(workflow), 'utf-8')

    def _stage_api_steps(self, local_api_dir):
        """Copy the CWL steps forming the API to the remote compute
        resource, replacing $CERISE_API_FILES at the start of a
        baseCommand and in arguments with the remote path to the files,
        and saving the result as JSON.
        """
        local_steps_dir = local_api_dir / 'steps'

        for this_dir, _, files in local_steps_dir.walk():
            self._logger.debug('Scanning file for staging: ' + str(this_dir) + '/' + str(files))
            for filename in files:
                if filename.endswith('.cwl'):
                    cwlfile = self._translate_api_step(this_dir / filename)
                    # make parent directory
                    rel_this_dir = this_dir.relative_to(str(local_steps_dir))
                    remote_this_dir = self._steps_dir / str(rel_this_dir)
                    remote_this_dir.mkdir(0o700, parents=True, exists_ok=True)

                    # write it to remote
                    rem_file = remote_this_dir / filename
                    self._logger.debug('Staging step to {} from {}'.format(
                        rem_file, filename))
                    rem_file.write_text(json.dumps(cwlfile))

    def _translate_api_step(self, workflow_path):
        """Do CERISE_API_FILES macro substitution on an API step file.
        """
        cwlfile = yaml.safe_load(workflow_path.read_text())
        if cwlfile.get('class') == 'CommandLineTool':
            if 'baseCommand' in cwlfile:
                if cwlfile['baseCommand'].lstrip().startswith('$CERISE_API_FILES'):
                    cwlfile['baseCommand'] = cwlfile['baseCommand'].replace(
                            '$CERISE_API_FILES', str(self._files_dir), 1)

            if 'arguments' in cwlfile:
                if not isinstance(cwlfile['arguments'], list):
                    raise RuntimeError('Invalid step {}: arguments must be an array'.format(
                        filename))
                newargs = []
                for i, argument in enumerate(cwlfile['arguments']):
                    self._logger.debug("Processing argument {}".format(argument))
                    newargs.append(argument.replace(
                        '$CERISE_API_FILES', str(self._files_dir)))
                    self._logger.debug("Done processing argument {}".format(cwlfile['arguments'][i]))
                cwlfile['arguments'] = newargs
        return cwlfile

    def _stage_api_files(self, local_api_dir):
        local_dir = local_api_dir / 'files'
        if not local_dir.exists():
            self._logger.debug('API files not found, not staging')
            return
        self._logger.debug('Staging API part to {} from {}'.format(
                self._files_dir, local_dir))
        cerulean.copy(local_dir, self._files_dir, overwrite='always',
                      copy_into=False, copy_permissions=True)

    def _stage_install_script(self, local_api_dir):
        local_path = local_api_dir / 'install.sh'
        if not local_path.exists():
            self._logger.debug('API install script not found, not staging')
            return None

        remote_path = self._api_dir / 'install.sh'
        self._logger.debug('Staging API install script to {} from {}'.format(
            remote_path, local_path))
        cerulean.copy(local_path, remote_path, overwrite='always', copy_into=False)

        while not remote_path.exists():
            pass

        remote_path.chmod(0o700)
        return remote_path

    def _run_install_script(self, api_install_script_path):
        jobdesc = cerulean.JobDescription()
        jobdesc.working_directory = self._files_dir
        jobdesc.command = str(api_install_script_path)
        jobdesc.arguments=[str(self._files_dir)]
        jobdesc.environment={'CERISE_API_FILES': str(self._files_dir)}

        self._logger.debug("Starting api install script {}".format(api_install_script_path))
        job_id = self._sched.submit(jobdesc)
        while self._sched.get_status(job_id) != cerulean.JobStatus.DONE:
            time.sleep(1.0)
        self._logger.debug("API install script done")
