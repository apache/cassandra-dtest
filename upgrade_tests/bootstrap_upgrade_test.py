import pytest

from bootstrap_test import BootstrapTester

since = pytest.mark.since


@pytest.mark.upgrade_test
class TestBootstrapUpgrade(BootstrapTester):

    """
    @jira_ticket CASSANDRA-11841
    Test that bootstrap works with a mixed version cluster
    In particular, we want to test that keep-alive is not sent
    to a node with version < 3.10
    """
    @pytest.mark.no_vnodes
    @since('3.10', max_version='3.99')
    def test_simple_bootstrap_mixed_versions(self):
        # Compatibility flag ensures that bootstrapping gets schema information during
        # upgrades from 3.0.14+ to anything upwards for 3.0.x or 3.x clusters.
        # @jira_ticket CASSANDRA-13004 for detailed context on `force_3_0_protocol_version` flag
        self._test_bootstrap_with_compatibility_flag_on(bootstrap_from_version="3.5")
