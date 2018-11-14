import logging
from cerulean import LocalFileSystem

from .cwl import get_workflow_step_names, get_required_num_cores
from cerise.job_store.job_state import JobState


class InvalidJobError(RuntimeError):
    pass


class JobPlanner:
    """Handles workflow execution requirements.

    This class keeps track of which hardware is needed for each
    available step, then analyses a workflow and decides which
    resources it needs based on this.
    """

    def __init__(self, job_store, local_api_dir):
        """Create a JobPlanner.

        Args:
            job_store (JobStore): The job store to act on.
            local_api_dir (str): Path of local api directory.
        """
        self._logger = logging.getLogger(__name__)
        """A logger for this object."""
        self._local_fs = LocalFileSystem()
        """The local file system."""
        self._job_store = job_store
        """The job store to act on."""
        self._steps_requirements = dict()  # type: Dict[str, Dict[str, int]]
        """Requirements per step, keyed by step name and requirement
                name.
        """

        self._get_steps_resource_requirements(local_api_dir)

    def plan_job(self, job_id):
        """Figures out which resources a job needs.

        Resources are identified by strings. Currently, there is only
        ``num_cores``, the number of cores to run on.

        Args:
            job_id: Id of the job to plan.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)

            steps = get_workflow_step_names(job.workflow_content)
            for step in steps:
                if step not in self._steps_requirements:
                    self._logger.info('Found invalid step {} in workflow'.format(step))
                    raise InvalidJobError()

            num_cores = [self._steps_requirements[step]['num_cores']
                         for step in steps]
            required_cores = max(num_cores)
            # If required_cores is zero, then none of the steps set
            # a value. In that case, we don't set the value on the
            # job, but leave the current value, which may come from
            # the workflow, or it may be unset (0) to begin with,
            # which would just run with the cluster default.
            if required_cores > 0:
                job.required_num_cores = required_cores

    def _get_steps_resource_requirements(self, local_api_dir):
        """Scan CWL steps and extract resource requirements.

        Args:
            local_api_dir: The local directory with the API
        """
        local_steps_dir = self._local_fs / local_api_dir / 'steps'

        for this_dir, _, files in local_steps_dir.walk():
            for filename in files:
                if filename.endswith('.cwl'):
                    self._logger.debug('Scanning file for requirements: {}'.format(this_dir / filename))
                    rel_this_dir = this_dir.relative_to(str(local_steps_dir))
                    step_name = str(rel_this_dir / filename)
                    step_contents = (this_dir / filename).read_bytes()
                    step_num_cores = get_required_num_cores(step_contents)
                    if not step_name in self._steps_requirements:
                        self._steps_requirements[step_name] = dict()
                    self._steps_requirements[step_name]['num_cores'] = step_num_cores
                    self._logger.debug('Step {} requires {} cores'.format(step_name, step_num_cores))
