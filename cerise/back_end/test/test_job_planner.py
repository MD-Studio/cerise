from cerise.back_end.job_planner import JobPlanner

from .mock_store import MockStore

import cerulean
import os
import pytest


@pytest.fixture
def fixture(request, tmpdir):
    result = {}

    result['remote-dir'] = str(tmpdir)
    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })

    base_api_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'api')
    result['planner'] = JobPlanner(result['store'], base_api_dir)
    return result


def test_job_planner_init(fixture):
    requirements = fixture['planner']._steps_requirements
    assert requirements['cerise/test/wc.cwl']['num_cores'] == 0
    assert requirements['cerise/test/hostname.cwl']['num_cores'] == 2
    assert requirements['cerise/test/echo.cwl']['num_cores'] == 0
    assert requirements['cerise/test/sleep.cwl']['num_cores'] == 0

    assert requirements['cerise/test/wc.cwl']['time_limit'] == 60
    assert requirements['cerise/test/hostname.cwl']['time_limit'] == 0
    assert requirements['cerise/test/echo.cwl']['time_limit'] == 0
    assert requirements['cerise/test/sleep.cwl']['time_limit'] == 0


def test_plan_job(fixture):
    fixture['store'].add_test_job('test_plan_job', 'hostname', 'resolved')
    job = fixture['store'].get_job('test_plan_job')
    assert job.required_num_cores == 0
    assert job.time_limit == 0

    fixture['planner'].plan_job('test_plan_job')
    assert job.required_num_cores == 2
    assert job.time_limit == 101


def test_plan_job2(fixture):
    fixture['store'].add_test_job('test_plan_job2', 'wc', 'resolved')
    job = fixture['store'].get_job('test_plan_job2')
    assert job.required_num_cores == 0
    assert job.time_limit == 0

    fixture['planner'].plan_job('test_plan_job2')
    assert job.required_num_cores == 3
    assert job.time_limit == 60
