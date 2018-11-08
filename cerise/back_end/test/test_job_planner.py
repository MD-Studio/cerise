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


def test_plan_job(fixture):
    fixture['store'].add_test_job('test_stage_job', 'hostname', 'resolved')
    job = fixture['store'].get_job('test_stage_job')
    assert job.required_num_cores == 0

    fixture['planner'].plan_job('test_stage_job')
    assert job.required_num_cores == 2
