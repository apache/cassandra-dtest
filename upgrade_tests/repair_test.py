import time

from dtest import debug
from repair_tests.repair_test import BaseRepairTest
from tools import since

LEGACY_SSTABLES_JVM_ARGS = ["-Dcassandra.streamdes.initial_mem_buffer_size=1",
                            "-Dcassandra.streamdes.max_mem_buffer_size=5",
                            "-Dcassandra.streamdes.max_spill_file_size=16"]


@since('3.0')
class TestUpgradeRepair(BaseRepairTest):
    __test__ = True

    @since('3.0')
    def repair_after_upgrade_test(self):
        """
        @jira_ticket CASSANDRA-10990
        """
        default_install_dir = self.cluster.get_install_dir()
        cluster = self.cluster
        debug("Setting version to 2.2.5")
        cluster.set_install_dir(version="2.2.5")
        self._populate_cluster()

        self._do_upgrade(default_install_dir)
        self._repair_and_verify(True)

    def _do_upgrade(self, default_install_dir):
        cluster = self.cluster

        for node in cluster.nodelist():
            debug("Upgrading %s to current version" % node.name)
            if node.is_running():
                node.flush()
                time.sleep(1)
                node.stop(wait_other_notice=True)
            node.set_install_dir(install_dir=default_install_dir)
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            cursor = self.patient_cql_connection(node)
        cluster.set_install_dir(default_install_dir)
