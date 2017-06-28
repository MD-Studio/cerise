import pytest
import xenon

_xenon_inited = False

@pytest.fixture(scope="session")
def xenon_init(request):
    # As far as I can tell, this should be run only once with a
    # session scope, but apparently it isn't, because xenon
    # complains about multiple inits. So have a guard.
    global _xenon_inited
    if not _xenon_inited:
        xenon.init()
        _xenon_inited = True
    return None
