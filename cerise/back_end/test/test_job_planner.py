from cerise.back_end.job_planner import JobPlanner
from cerise.test.fixture_jobs import (PassJob, SlowJob, BrokenJob,
                                      NoSuchStepJob, MissingInputJob)

import cerulean
import pytest


lfs = cerulean.LocalFileSystem()


def test_job_planner_init(mock_config, mock_store_resolved, local_api_dir):
    store, job_fixture = mock_store_resolved

    planner = JobPlanner(store, lfs / str(local_api_dir))

    requirements = planner._steps_requirements
    assert requirements['test/wc.cwl']['num_cores'] == 0
    assert requirements['test/hostname.cwl']['num_cores'] == 2
    assert requirements['test/echo.cwl']['num_cores'] == 0
    assert requirements['test/sleep.cwl']['num_cores'] == 0

    assert requirements['test/wc.cwl']['time_limit'] == 60
    assert requirements['test/hostname.cwl']['time_limit'] == 0
    assert requirements['test/echo.cwl']['time_limit'] == 0
    assert requirements['test/sleep.cwl']['time_limit'] == 0


def test_plan_job(mock_config, mock_store_resolved, local_api_dir):
    store, job_fixture = mock_store_resolved

    planner = JobPlanner(store, lfs / str(local_api_dir))

    job = store.get_job('test_job')
    assert job.required_num_cores == 0
    assert job.time_limit == 0

    try:
        planner.plan_job('test_job')
    except RuntimeError as e:
        if 'Invalid workflow file' in e.args[0]:
            if job_fixture in [PassJob, SlowJob, BrokenJob, MissingInputJob]:
                return
        elif 'Invalid step in workflow' in e.args[0]:
            if job_fixture is NoSuchStepJob:
                return
        raise
    assert job.required_num_cores == job_fixture.required_num_cores
    assert job.time_limit == job_fixture.time_limit
