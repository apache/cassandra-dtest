import pytest

from bootstrap_test import TestBootstrap

since = pytest.mark.since


@pytest.mark.upgrade_test
class TestBootstrapUpgrade(TestBootstrap):

    """
    @jira_ticket CASSANDRA-11841
    Test that bootstrap works with a mixed version cluster
    In particular, we want to test that keep-alive is not sent
    to a node with version < 3.10
    """
    @pytest.mark.no_vnodes
    @since('3.10', max_version='3.99')
    def test_simple_bootstrap_mixed_versions(self):
        self._base_bootstrap_test(bootstrap_from_version="3.5")
