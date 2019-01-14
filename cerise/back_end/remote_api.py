import json
import logging
import time
from typing import Any, Dict, List, Optional

import cerulean
import yaml
from paramiko.ssh_exception import SSHException  # type: ignore
from retrying import retry

from cerise.config import Config


class RemoteApi:
    """Manages the remote API installation.

    This class manages the remote directories in which the CWL API is
    installed, which is <basedir>/api/

    Within this, there is a directory per project, with entries

    <project>/version
    <project>/steps/...
    <project>/files/...
    <project>/install.sh
    """

    def __init__(self, config: Config, local_api_dir: cerulean.Path) -> None:
        """Create a RemoteApiFiles object.
        Sets up remote directory structure as well, but refuses to
        create the top-level directory.

        Args:
            config: The configuration.
            local_api_dir: The path to the local API dir to install from.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._sched = config.get_scheduler(run_on_head_node=True)
        """cerulean.Scheduler: Scheduler for running install script."""
        self._username = config.get_username('files')
        """str: The remote user name to use, if any."""
        self._local_api_dir = local_api_dir
        """cerulean.Path: The path to the local API dir."""
        self._remote_api_dir = config.get_basedir() / 'api'
        """cerulean.Path: The remote path to the base directory."""
        self._steps_requirements = dict()  # type: Dict[str, Dict[str, int]]
        """Resource requirements for each loaded step."""

        self._remote_api_dir.mkdir(0o750, parents=True, exists_ok=True)

    def update_available(self) -> bool:
        """Returns whether the remote API is older than the local one.

        Returns:
            True iff an update is available/required.
        """
        return self._updatable_projects() != []

    def install(self) -> None:
        """Install the API onto the compute resource.

        Copies subdirectories steps/ and files/ of the given local api
        dir to the compute resource, copies files/ to the compute
        resource, and runs the install script.
        """
        self._logger.info('Staging API from {} to {}'.format(
            self._local_api_dir, self._remote_api_dir))

        try:
            for project_name in self._updatable_projects():
                local_project_dir = self._local_api_dir / project_name
                remote_project_dir = self._make_remote_project(project_name)
                self._stage_api_files(local_project_dir, remote_project_dir)
                self._stage_api_steps(local_project_dir, remote_project_dir)
                self._stage_install_script(local_project_dir,
                                           remote_project_dir)
                self._run_install_script(remote_project_dir)
        except IOError as e:
            self._logger.critical('An IO error occurred while uploading the'
                                  ' API: {}. Please check that your network'
                                  ' connection works, and that you have enough'
                                  ' disk space or quota on the remote machine.'
                                  ''.format(e))

    def get_projects(self) -> List[str]:
        """Return names and versions of the installed projects.

        Returns:
            A list of strings, one for each project, with name and
                    version.
        """
        projects_versions = list()
        for project_dir in self._local_api_dir.iterdir():
            if project_dir.is_dir():
                project_name = str(
                    project_dir.relative_to(self._local_api_dir))
                version_file = project_dir / 'version'
                version = version_file.read_text().strip()
                projects_versions.append('{} {}'.format(project_name, version))

        return projects_versions

    def translate_runner_location(self, runner_location: str) -> str:
        """Perform macro substitution on CWL runner location.

        This replaces $CERISE_API with the API base dir.

        Args:
            runner_location (str): Location of the runner as configured
                    by the user.

        Returns:
            (str) A remote path with variables substituted.
        """
        actual_location = runner_location.replace('$CERISE_API',
                                                  str(self._remote_api_dir))
        if self._username:
            actual_location = actual_location.replace('$CERISE_USERNAME',
                                                      self._username)
        return actual_location

    def translate_workflow(self, workflow_content: bytes) -> bytes:
        """Parse workflow content, check that it calls steps, and
        insert the location of the steps on the remote resource so that
        the remote runner can find them.

        Also converts YAML to JSON, for cwltiny compatibility.

        Args:
            workflow_content: The raw workflow data

        Returns:
            The modified workflow data, serialised as JSON

        """
        workflow = yaml.safe_load(str(workflow_content, 'utf-8'))
        if 'steps' not in workflow:
            raise RuntimeError('Workflow contains no steps')
        for _, step in workflow['steps'].items():
            if not isinstance(step['run'], str):
                raise RuntimeError('Invalid step in workflow')
            step_parts = step['run'].split('/')
            project = step_parts[0]
            steps_dir = self._remote_api_dir / project / 'steps'
            step['run'] = str(steps_dir) + '/' + '/'.join(step_parts)
        return bytes(json.dumps(workflow), 'utf-8')

    def _updatable_projects(self) -> List[str]:
        """Returns a list of names of projects that can be updated.

        A project is updatable if its local version is larger than its
        remote version, or if the local version ends with ``.dev``. The
        latter is handy for development.

        Returns:
            A list of updatable project names.
        """
        updatable_projects = list()
        for local_project_dir in self._local_api_dir.iterdir():
            if local_project_dir.is_dir():
                project_name = str(
                    local_project_dir.relative_to(self._local_api_dir))

                local_version_file = local_project_dir / 'version'
                if not local_version_file.exists():
                    raise RuntimeError(
                        'Project "{}" in local API definition'
                        ' is missing a "version" file.'.format(project_name))
                local_version = local_version_file.read_text().strip().split(
                    '.')

                remote_version_file = (self._remote_api_dir / project_name
                                       / 'version')
                if not remote_version_file.exists():
                    updatable_projects.append(project_name)
                    continue
                remote_version = remote_version_file.read_text().strip().split(
                    '.')

                if local_version[
                        -1] == 'dev' or local_version[:3] > remote_version[:3]:
                    updatable_projects.append(project_name)

        return updatable_projects

    @retry(
        retry_on_exception=lambda e: isinstance(e, SSHException),
        stop_max_attempt_number=10)
    def _make_remote_project(self, name: str) -> cerulean.Path:
        """Creates a remote directory for a given project.

        If a directory already exists, removes it first.

        Args:
            name: Name of the project.

        Returns:
            Path of the remote project directory.
        """
        remote_project_dir = self._remote_api_dir / name
        # A recursive rmdir via SFTP is slow if you have many files, since
        # the directory hierarchy has to be walked client-side. Since we
        # already run the installation script anyway, we use a server-side
        # rm -rf to delete an existing project dir, for performance reasons.
        if remote_project_dir.exists():
            rmdir = cerulean.JobDescription()
            rmdir.command = 'rm'
            rmdir.arguments = ['-r', '-f', str(remote_project_dir)]
            job_id = self._sched.submit(rmdir)
            exit_code = self._sched.wait(job_id)
            if exit_code != 0:
                msg = 'Failed to delete existing project dir: {}'.format(
                        exit_code)
                self._logger.debug(msg)
                raise RuntimeError(msg)
        remote_project_dir.mkdir(0o700)
        return remote_project_dir

    @retry(
        retry_on_exception=lambda e: isinstance(e, SSHException),
        stop_max_attempt_number=10)
    def _stage_api_steps(self, local_project_dir: cerulean.Path,
                         remote_project_dir: cerulean.Path) -> None:
        """Copy the CWL steps forming the API to the remote compute
        resource, replacing $CERISE_PROJECT_FILES at the start of a
        baseCommand and in arguments with the remote path to the files,
        and saving the result as JSON.

        Args:
            local_project_dir: The local directory to copy from.
            project_name: Name of the project to stage steps for.
        """
        local_steps_dir = local_project_dir / 'steps'
        remote_steps_dir = remote_project_dir / 'steps'

        for this_dir, _, files in local_steps_dir.walk():
            self._logger.debug('Scanning file for staging: ' + str(this_dir) +
                               '/' + str(files))
            for filename in files:
                if filename.endswith('.cwl'):
                    cwlfile = self._translate_api_step(this_dir / filename,
                                                       remote_project_dir)
                    # make parent directory
                    rel_this_dir = this_dir.relative_to(str(local_steps_dir))
                    remote_this_dir = remote_steps_dir / str(rel_this_dir)
                    remote_this_dir.mkdir(0o700, parents=True, exists_ok=True)

                    # write it to remote
                    rem_file = remote_this_dir / filename
                    self._logger.debug('Staging step to {} from {}'.format(
                        rem_file, filename))
                    rem_file.write_text(json.dumps(cwlfile))

    def _translate_api_step(self, step_path: cerulean.Path,
                            remote_project_dir: cerulean.Path) -> Any:
        """Do CERISE_PROJECT_FILES macro substitution on an API step file.

        Args:
            step_path: Path to the step file to translate
            remote_project_dir: Remote path of the project directory,
                    for substitution.

        Returns:
            The modified contents of the CWL file, as objects.
        """
        files_dir = remote_project_dir / 'files'
        cwlfile = yaml.safe_load(step_path.read_text())
        if cwlfile.get('class') == 'CommandLineTool':
            if 'baseCommand' in cwlfile:
                if cwlfile['baseCommand'].lstrip().startswith(
                        '$CERISE_PROJECT_FILES'):
                    cwlfile['baseCommand'] = cwlfile['baseCommand'].replace(
                        '$CERISE_PROJECT_FILES', str(files_dir), 1)

            if 'arguments' in cwlfile:
                if not isinstance(cwlfile['arguments'], list):
                    raise RuntimeError(
                        'Invalid step {}: arguments must be an array'.format(
                            step_path))
                newargs = []
                for i, argument in enumerate(cwlfile['arguments']):
                    self._logger.debug(
                        "Processing argument {}".format(argument))
                    newargs.append(
                        argument.replace('$CERISE_PROJECT_FILES',
                                         str(files_dir)))
                    self._logger.debug("Done processing argument {}".format(
                        cwlfile['arguments'][i]))
                cwlfile['arguments'] = newargs
        return cwlfile

    @retry(
        retry_on_exception=lambda e: isinstance(e, SSHException),
        stop_max_attempt_number=10)
    def _stage_api_files(self, local_project_dir: cerulean.Path,
                         remote_project_dir: cerulean.Path) -> None:
        cerulean.copy(
            local_project_dir / 'version',
            remote_project_dir / 'version',
            overwrite='always')

        local_dir = local_project_dir / 'files'
        if not local_dir.exists():
            self._logger.debug('API files at {} not found, not'
                               ' staging'.format(local_dir))
            return

        remote_dir = remote_project_dir / 'files'
        self._logger.debug('Staging API part to {} from {}'.format(
            remote_dir, local_dir))

        try_count = 0
        succeeded = False
        while not succeeded and try_count < 10:
            try:
                cerulean.copy(
                    local_dir,
                    remote_dir,
                    overwrite='always',
                    copy_into=False,
                    copy_permissions=True)
                succeeded = True
            except SSHException as e:
                self._logger.info('Connection error: {}'.format(e.args[0]))
                try_count += 1
                self._logger.info('Try {} of 10 failed'.format(try_count))

    @retry(
        retry_on_exception=lambda e: isinstance(e, SSHException),
        stop_max_attempt_number=10)
    def _stage_install_script(
            self, local_project_dir: cerulean.Path,
            remote_project_dir: cerulean.Path) -> Optional[cerulean.Path]:
        local_path = local_project_dir / 'install.sh'
        if not local_path.exists():
            self._logger.debug('API install script not found at {}, not'
                               ' staging'.format(local_path))
            return None

        remote_path = remote_project_dir / 'install.sh'
        self._logger.debug('Staging API install script to {} from {}'.format(
            remote_path, local_path))
        cerulean.copy(
            local_path, remote_path, overwrite='always', copy_into=False)

        while not remote_path.exists():
            time.sleep(0.05)

        remote_path.chmod(0o700)
        return remote_path

    @retry(
        retry_on_exception=lambda e: isinstance(e, SSHException),
        stop_max_attempt_number=10)
    def _run_install_script(self, remote_project_dir: cerulean.Path) -> None:
        files_dir = remote_project_dir / 'files'
        install_script = remote_project_dir / 'install.sh'
        remote_stdout = remote_project_dir / '.cerise_install.out'
        remote_stderr = remote_project_dir / '.cerise_install.err'
        if install_script.exists():
            jobdesc = cerulean.JobDescription()
            jobdesc.command = str(install_script)
            jobdesc.arguments = [str(files_dir)]
            jobdesc.environment['CERISE_PROJECT_FILES'] = str(files_dir)
            jobdesc.stdout_file = str(remote_stdout)
            jobdesc.stderr_file = str(remote_stderr)

            self._logger.debug("Starting api install script {}".format(
                jobdesc.command))
            job_id = self._sched.submit(jobdesc)
            exit_code = self._sched.wait(job_id)
            if exit_code != 0:
                self._logger.debug(
                    'API install script error code: {}'.format(exit_code))
                self._logger.debug('API install script stdout: {}'.format(
                    remote_stdout.read_text()))
                self._logger.debug('API install script stderr: {}'.format(
                    remote_stderr.read_text()))
                raise RuntimeError('API install script returned error code'
                                   ' {}'.format(exit_code))
            else:
                remote_stdout.unlink()
                remote_stderr.unlink()
            self._logger.debug("API install script done")
