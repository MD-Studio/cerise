from simple_cwl_xenon_service.job_manager.sqlite_job_store import SQLiteJobStore

from simple_cwl_xenon_service.job_manager.job_description import JobDescription
from simple_cwl_xenon_service.job_manager.job_state import JobState

from .fixture_jobs import PassJob
from .fixture_jobs import WcJob

import os
import pytest
import sqlite3

@pytest.fixture
def db_name(request, tmpdir):
    return os.path.join(str(tmpdir), 'test_sqlite_job_store.db')

@pytest.fixture
def empty_db(request, db_name):
    return {'file': db_name, 'conn': sqlite3.connect(db_name)}

@pytest.fixture
def inited_db(request, empty_db):
    empty_db['conn'].execute("""
        CREATE TABLE jobs(
            job_id CHARACTER(32),
            name VARCHAR(255),
            workflow VARCHAR(255),
            local_input TEXT,
            state VARCHAR(17) DEFAULT 'Submitted',
            log TEXT,
            output TEXT,
            workflow_content BLOB,
            remote_workdir_path VARCHAR(255),
            remote_workflow_path VARCHAR(255),
            remote_input_path VARCHAR(255),
            remote_stdout_path VARCHAR(255),
            remote_stderr_path VARCHAR(255),
            remote_job_id VARCHAR(255)
            )
            """)
    empty_db['conn'].commit()
    return empty_db

@pytest.fixture
def onejob_db(request, inited_db):
    inited_db['conn'].execute("""
        INSERT INTO jobs (job_id, name, workflow, local_input, state) VALUES (
            '258685677b034756b55bbad161b2b89b',
            'test_sqlite_job_store',
            ?,
            ?,
            ?);
        """, (PassJob.workflow, PassJob.local_input, JobState.SUBMITTED.name))
    inited_db['conn'].commit()
    return inited_db

@pytest.fixture
def onejob_store(request, onejob_db):
    return {'conn': onejob_db['conn'], 'store': SQLiteJobStore(onejob_db['file'])}

@pytest.fixture
def job(request, onejob_store):
    with onejob_store['store']:
        yield onejob_store['store'].get_job('258685677b034756b55bbad161b2b89b')



def test_create_store(db_name):
    store = SQLiteJobStore(db_name)

def test_open_existing_store(empty_db):
    store = SQLiteJobStore(empty_db['file'])

def test_open_existing_store_data(onejob_db):
    store = SQLiteJobStore(onejob_db['file'])
    with store:
        assert store.get_job('258685677b034756b55bbad161b2b89b').name == 'test_sqlite_job_store'

def test_create_job(onejob_store):
    desc = JobDescription('test_create_job', 'file:///', '{}')
    with onejob_store['store']:
        onejob_store['store'].create_job(desc)
    res = onejob_store['conn'].execute("""SELECT * FROM jobs WHERE name = 'test_create_job';""")
    assert len(res.fetchall()) == 1

def test_list_jobs(onejob_store):
    with onejob_store['store']:
        joblist = onejob_store['store'].list_jobs()
        assert len(joblist) == 1
        assert joblist[0].name == 'test_sqlite_job_store'

def test_get_job(onejob_store):
    with onejob_store['store']:
        job = onejob_store['store'].get_job('258685677b034756b55bbad161b2b89b')
        assert job.name == 'test_sqlite_job_store'

def test_delete_job(onejob_store):
    with onejob_store['store']:
        onejob_store['store'].delete_job('258685677b034756b55bbad161b2b89b')
    res = onejob_store['conn'].execute("""
        SELECT * FROM jobs
        WHERE job_id == '258685677b034756b55bbad161b2b89b';
        """)
    assert len(res.fetchall()) == 0

def test_reading_name(job):
    assert job.name == 'test_sqlite_job_store'

def test_reading_workflow(job):
    assert job.workflow == PassJob.workflow

def test_reading_local_input(job):
    assert job.local_input == PassJob.local_input

def test_reading_state(job):
    assert job.state == JobState.SUBMITTED

def test_setting_state(job):
    job.state = JobState.CANCELLED
    assert job.state == JobState.CANCELLED

def test_state_transitions(job):
    assert not job.try_transition(JobState.STAGING, JobState.STAGING_CR)
    assert job.try_transition(JobState.SUBMITTED, JobState.STAGING)
    assert job.state == JobState.STAGING
    assert job.try_transition(JobState.STAGING, JobState.STAGING_CR)
    assert job.state == JobState.STAGING_CR

def test_set_get_log(job):
    test_log = """This is a test log
        With newlines.
        """
    job.log = test_log
    assert job.log == test_log

def test_set_get_output(job):
    test_output = WcJob.remote_output('')
    job.output = test_output
    assert job.output == test_output

def test_set_get_workflow_content(job):
    job.workflow_content = WcJob.workflow
    assert job.workflow_content == WcJob.workflow

def test_set_get_remote_workdir_path(job):
    job.remote_workdir_path = '/test_set_get'
    assert job.remote_workdir_path == '/test_set_get'

def test_set_get_remote_workflow_path(job):
    job.remote_workflow_path = '/test_set_get'
    assert job.remote_workflow_path == '/test_set_get'

def test_set_get_remote_input_path(job):
    job.remote_input_path = '/test_set_get'
    assert job.remote_input_path == '/test_set_get'

def test_set_get_remote_stdout_path(job):
    job.remote_stdout_path = '/test_set_get'
    assert job.remote_stdout_path == '/test_set_get'

def test_set_get_remote_stderr_path(job):
    job.remote_stderr_path = '/test_set_get'
    assert job.remote_stderr_path == '/test_set_get'

def test_set_get_remote_job_id(job):
    job.remote_job_id = 'slurm.00042'
    assert job.remote_job_id == 'slurm.00042'

