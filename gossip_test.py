import logging

import pytest

from dtest import Tester
from tools.misc import new_node
from tools.assertions import assert_stderr_clean

since = pytest.mark.since
logger = logging.getLogger(__name__)

class TestGossip(Tester):
    """
    This test suite looks at how gossip information is distributed within
    a cluster under various scenarios that alter gossip state.
    """
    @pytest.mark.no_vnodes
    def test_assassination_of_unknown_node(self):
        """
        @jira_ticket CASSANDRA-16588
        Test that a non-seed node can come back online after assassinating an
        unknown node.
        """
        cluster = self.cluster

        # Create a 5-node cluster
        cluster.populate(5)
        node1 = cluster.nodelist()[0]
        node3 = cluster.nodelist()[2]

        self.cluster.set_configuration_options({
            'seed_provider': [{'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                               'parameters': [{'seeds': node1.address()}]
                              }]
            })

        cluster.start()

        logger.debug("Shutting down node {}".format(node3.address()))
        node3.stop()

        logger.debug("Assassinating unknown node 11.1.1.1")
        out, err, _ = node1.nodetool("assassinate 11.1.1.1")
        assert_stderr_clean(err)

        logger.debug("Starting node {}".format(node3.address()))
        node3.start()

    def test_assassinate_valid_node(self):
        """
        @jira_ticket CASSANDRA-16588
        Test that after taking two non-seed nodes down and assassinating
        one of them, the other can come back up.
        """
        cluster = self.cluster

        cluster.populate(5).start()
        node1 = cluster.nodelist()[0]
        node3 = cluster.nodelist()[2]

        self.cluster.set_configuration_options({
            'seed_provider': [{'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                               'parameters': [{'seeds': node1.address()}]
                              }]
            })

        non_seed_nodes = cluster.nodelist()[-2:]
        for node in non_seed_nodes:
            node.stop()

        assassination_target = non_seed_nodes[0]
        logger.debug("Assassinating non-seed node {}".format(assassination_target.address()))
        out, err, _ = node1.nodetool("assassinate {}".format(assassination_target.address()))
        assert_stderr_clean(err)

        logger.debug("Starting non-seed nodes")
        for node in non_seed_nodes:
            node.start()

    def _cluster_create_node(self, num):
        address = '127.0.0.%d' % num
        return self.cluster.create_node("node%s" % num, False, None,
                                        (address, 7000 + num),
                                        str(7100 + num), str(2000 + num), None,
                                        binary_interface=(address, 9042))

    def _start_nodes_in_parallel(self, nodelist):
        for node in nodelist:
            node.start(no_wait=True)

        # Wait 30 seconds for nodes to come up
        for _ in range(30):
            if [n for n in nodelist if n.is_running()] == nodelist:
                break
            time.sleep(1)

        for node in nodelist:
            assert node.is_running()

    def _create_2dc_cluster(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})

        # Add dc1 nodes
        node1 = self._cluster_create_node(1)
        cluster.add(node1, True, data_center='dc1')
        node1.start(wait_for_binary_proto=True)
        node2 = self._cluster_create_node(2)
        cluster.add(node2, False, data_center='dc1')
        node2.start(wait_for_binary_proto=True)

        # Add dc2 nodes
        node3 = self._cluster_create_node(3)
        cluster.add(node3, True, data_center='dc2')
        node3.start(wait_for_binary_proto=True)
        node4 = self._cluster_create_node(4)
        cluster.add(node4, False, data_center='dc2')
        node4.start(wait_for_binary_proto=True)

        # Stop cluster
        for node in cluster.nodelist():
            node.stop()

        return node1, node2, node3, node4

    @since('4.0')
    def test_2dc_parallel_startup(self):
        """
        @jira_ticket CASSANDRA-16588
        Given a 2 DC cluster, start all seeds node in parallel followed by
        all non-seed nodes in parallel.
        """
        node1, node2, node3, node4 = self._create_2dc_cluster()

        # Start seeds in parallel
        self._start_nodes_in_parallel([node1, node3])

        # Start non-seeds in parallel
        self._start_nodes_in_parallel([node2, node4])

    @since('4.0')
    def test_2dc_parallel_startup_one_seed(self):
        """
        @jira_ticket CASSANDRA-16588
        Given a 2 DC cluster, start one seed node followed by all non-seed
        nodes in parallel.
        """
        node1, node2, node3, node4 = self._create_2dc_cluster()

        # Start one seed (of two)
        self._start_nodes_in_parallel([node1])

        # Start non-seeds in parallel
        self._start_nodes_in_parallel([node2, node4])
