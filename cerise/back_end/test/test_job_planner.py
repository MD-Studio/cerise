from cerise.back_end.job_planner import JobPlanner
from cerise.back_end.test.fixture_jobs import PassJob, SlowJob, BrokenJob

import pytest


def test_job_planner_init(mock_config, mock_store_resolved, local_api_dir):
    store, job_fixture = mock_store_resolved

    planner = JobPlanner(store, str(local_api_dir))

    requirements = planner._steps_requirements
    assert requirements['cerise/test/wc.cwl']['num_cores'] == 0
    assert requirements['cerise/test/hostname.cwl']['num_cores'] == 2
    assert requirements['cerise/test/echo.cwl']['num_cores'] == 0
    assert requirements['cerise/test/sleep.cwl']['num_cores'] == 0

    assert requirements['cerise/test/wc.cwl']['time_limit'] == 60
    assert requirements['cerise/test/hostname.cwl']['time_limit'] == 0
    assert requirements['cerise/test/echo.cwl']['time_limit'] == 0
    assert requirements['cerise/test/sleep.cwl']['time_limit'] == 0


def test_plan_job_new(mock_config, mock_store_resolved, local_api_dir):
    store, job_fixture = mock_store_resolved

    planner = JobPlanner(store, str(local_api_dir))

    job = store.get_job('test_job')
    assert job.required_num_cores == 0
    assert job.time_limit == 0

    try:
        planner.plan_job('test_job')
    except RuntimeError as e:
        if 'Invalid workflow file' in e.args[0]:
            if job_fixture in [PassJob, SlowJob, BrokenJob]:
                return
        raise
    assert job.required_num_cores == job_fixture.required_num_cores
    assert job.time_limit == job_fixture.time_limit
