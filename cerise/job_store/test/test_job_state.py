from cerise.job_store.job_state import JobState


def test_is_final():
    assert not JobState.is_final(JobState.SUBMITTED)
    assert JobState.is_final(JobState.PERMANENT_FAILURE)


def test_cancellation_active():
    for state in JobState:
        assert state.name.endswith('_CR') == JobState.cancellation_active(
            state)


def test_is_remote():
    for state in JobState:
        assert ('RUNNING' in state.name
                or 'WAITING' in state.name) == JobState.is_remote(state)


def test_to_cwl_state_string():
    assert JobState.to_cwl_state_string(JobState.SUBMITTED) == 'Waiting'
    assert JobState.to_cwl_state_string(
        JobState.TEMPORARY_FAILURE) == 'TemporaryFailure'
