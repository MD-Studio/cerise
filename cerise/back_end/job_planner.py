import logging
from typing import Dict, cast

import cerulean

from cerise.back_end.cwl import (get_required_num_cores, get_time_limit,
                                 get_workflow_step_names)
from cerise.job_store.sqlite_job_store import SQLiteJobStore


class InvalidJobError(RuntimeError):
    pass


class JobPlanner:
    """Handles workflow execution requirements.

    This class keeps track of which hardware is needed for each
    available step, then analyses a workflow and decides which
    resources it needs based on this.
    """

    def __init__(self, job_store: SQLiteJobStore,
                 local_api_dir: cerulean.Path):
        """Create a JobPlanner.

        Args:
            job_store: The job store to act on.
            local_api_dir: Path of local api directory.
        """
        self._logger = logging.getLogger(__name__)
        """A logger for this object."""
        self._job_store = job_store
        """The job store to act on."""
        self._steps_requirements = dict()  # type: Dict[str, Dict[str, int]]
        """Requirements per step, keyed by step name and requirement
                name.
        """
        self._get_steps_resource_requirements(local_api_dir)

    def plan_job(self, job_id: str) -> None:
        """Figures out which resources a job needs.

        Resources are identified by strings. Currently, there is
        ``num_cores``, the number of cores to run on, and
        ``time_limit``, the amount of time to reserve in seconds.

        Args:
            job_id: Id of the job to plan.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)

            steps = get_workflow_step_names(cast(bytes, job.workflow_content))
            for step in steps:
                if step not in self._steps_requirements:
                    job.error('Found invalid step {} in workflow'.format(step))
                    raise InvalidJobError('Invalid step in workflow')

            job.required_num_cores = get_required_num_cores(
                cast(bytes, job.workflow_content))
            num_cores_steps = [
                self._steps_requirements[step]['num_cores'] for step in steps
            ]
            if max(num_cores_steps) > 0:
                job.required_num_cores = max(num_cores_steps)

            job.time_limit = get_time_limit(cast(bytes, job.workflow_content))
            time_limit_steps = [
                self._steps_requirements[step]['time_limit'] for step in steps
            ]
            job.time_limit = max(job.time_limit, sum(time_limit_steps))

    def _get_steps_resource_requirements(self,
                                         local_api_dir: cerulean.Path) -> None:
        """Scan CWL steps and extract resource requirements.

        Args:
            local_api_dir: The local directory with the API
        """
        for project_dir in local_api_dir.iterdir():
            local_steps_dir = project_dir / 'steps'

            for this_dir, _, files in local_steps_dir.walk():
                for filename in files:
                    if filename.endswith('.cwl'):
                        self._logger.debug(
                            'Scanning file for requirements: {}'.format(
                                this_dir / filename))
                        rel_this_dir = this_dir.relative_to(
                            str(local_steps_dir))
                        step_name = str(rel_this_dir / filename)
                        step_contents = (this_dir / filename).read_bytes()
                        step_num_cores = get_required_num_cores(step_contents)
                        step_time_limit = get_time_limit(step_contents)
                        if step_name not in self._steps_requirements:
                            self._steps_requirements[step_name] = dict()
                        self._steps_requirements[step_name][
                            'num_cores'] = step_num_cores
                        self._steps_requirements[step_name][
                            'time_limit'] = step_time_limit
                        self._logger.debug('Step {} requires {} cores'.format(
                            step_name, step_num_cores))
