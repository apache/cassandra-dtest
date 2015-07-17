import os
import time

from dtest import Tester, DEBUG


QUERY_UPGRADED = os.environ.get('QUERY_UPGRADED', 'true').lower() in ('yes', 'true')
QUERY_OLD = os.environ.get('QUERY_OLD', 'true').lower() in ('yes', 'true')
OLD_CASSANDRA_DIR = os.environ.get('OLD_CASSANDRA_DIR', None)

# This controls how many of the nodes are upgraded.  Accepted values are
# "normal", "none", and "all".
# The "normal" setting results in one of the two nodes being upgraded before
# queries are run.
# The "none" setting doesn't upgrade any nodes.  When combined with debug
# logging, this is useful for seeing exactly what commands and responses
# a 2.1 cluster will use.
# The "all" setting upgrades all nodes before querying.  When combined with debug
# logging, this is useful for seeing exactly what commands and responses
# a 3.0 cluster will use.
UPGRADE_MODE = os.environ.get('UPGRADE_MODE', 'normal').lower()


class UpgradeTester(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=2, rf=1, protocol_version=None, **kwargs):
        assert nodes >= 2, "backwards compatibility tests require at least two nodes"
        assert not self._preserve_cluster, "preserve_cluster cannot be True for upgrade tests"

        self.protocol_version = protocol_version

        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        cluster.set_configuration_options(values={'internode_compression': 'none'})
        if not cluster.nodelist():
            cluster.populate(nodes)
            self.original_install_dir = cluster.nodelist()[0].get_install_dir()
            if OLD_CASSANDRA_DIR:
                cluster.set_install_dir(install_dir=OLD_CASSANDRA_DIR)
            else:
                cluster.set_install_dir(version='git:cassandra-2.1')
            cluster.start()

        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            self.create_ks(session, 'ks', rf)

        return session

    def do_upgrade(self, session):
        """
        Upgrades the first node in the cluster and returns a list of
        (is_upgraded, Session) tuples.  If `is_upgraded` is true, the
        Session is connected to the upgraded node.
        """
        session.cluster.shutdown()
        node1 = self.cluster.nodelist()[0]
        node2 = self.cluster.nodelist()[1]

        if UPGRADE_MODE not in ('normal', 'all', 'none'):
            raise Exception("UPGRADE_MODE should be one of 'normal', 'all', or 'none'")

        # stop the nodes
        if UPGRADE_MODE != "none":
            node1.drain()
            node1.stop(gently=True)

        if UPGRADE_MODE == "all":
            node2.drain()
            node2.stop(gently=True)

        # start them again
        if UPGRADE_MODE != "none":
            node1.set_install_dir(version=self.original_install_dir)
            node1.set_log_level("DEBUG" if DEBUG else "INFO")
            node1.set_configuration_options(values={'internode_compression': 'none'})
            node1.start(wait_for_binary_proto=True)

        if UPGRADE_MODE == "all":
            node2.set_install_dir(version=self.original_install_dir)
            node2.set_log_level("DEBUG" if DEBUG else "INFO")
            node2.set_configuration_options(values={'internode_compression': 'none'})
            node2.start(wait_for_binary_proto=True)

        sessions = []
        if QUERY_UPGRADED:
            session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((True, session))
        if QUERY_OLD:
            # open a second session with the node on the old version
            session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((False, session))

        return sessions

    def get_version(self):
        node1 = self.cluster.nodelist()[0]
        return node1.version()
