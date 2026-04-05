import pytest


@pytest.fixture
def repo_root(pytestconfig):
    return pytestconfig.rootpath
