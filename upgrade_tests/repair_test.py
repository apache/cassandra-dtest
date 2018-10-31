import time
import pytest
import logging

from repair_tests.repair_test import BaseRepairTest

since = pytest.mark.since
logger = logging.getLogger(__name__)

LEGACY_SSTABLES_JVM_ARGS = ["-Dcassandra.streamdes.initial_mem_buffer_size=1",
                            "-Dcassandra.streamdes.max_mem_buffer_size=5",
                            "-Dcassandra.streamdes.max_spill_file_size=16"]


# We don't support directly upgrading from 2.2 to 4.0 so disabling this on 4.0.
# TODO: we should probably not hardcode versions?
@pytest.mark.upgrade_test
@since('3.0', max_version='3.99')
class TestUpgradeRepair(BaseRepairTest):

    @since('3.0', max_version='3.99')
    def test_repair_after_upgrade(self):
        """
        @jira_ticket CASSANDRA-10990
        """
        default_install_dir = self.cluster.get_install_dir()
        cluster = self.cluster
        logger.debug("Setting version to 2.2.5")
        cluster.set_install_dir(version="2.2.5")
        self._populate_cluster()

        self._do_upgrade(default_install_dir)
        self._repair_and_verify(True)

    def _do_upgrade(self, default_install_dir):
        cluster = self.cluster

        for node in cluster.nodelist():
            logger.debug("Upgrading %s to current version" % node.name)
            if node.is_running():
                node.flush()
                time.sleep(1)
                node.stop(wait_other_notice=True)
            node.set_install_dir(install_dir=default_install_dir)
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            cursor = self.patient_cql_connection(node)
        cluster.set_install_dir(default_install_dir)
