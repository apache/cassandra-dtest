import os
import time

from dtest import Tester, DEBUG

cql_version = "3.0.0"

QUERY_UPGRADED = os.environ.get('QUERY_UPGRADED', 'true').lower() in ('yes', 'true')
QUERY_OLD = os.environ.get('QUERY_OLD', 'true').lower() in ('yes', 'true')
OLD_CASSANDRA_DIR = os.environ.get('OLD_CASSANDRA_DIR', None)
SKIP_UPGRADE = os.environ.get('SKIP_UPGRADE', 'false').lower() in ('yes', 'true')


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

        session = self.patient_cql_connection(node1, version=cql_version, protocol_version=protocol_version)
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

        if not SKIP_UPGRADE:
            node1.drain()
            node1.stop(gently=True)

            node1.set_install_dir(version=self.original_install_dir)
            node1.set_log_level("DEBUG" if DEBUG else "INFO")
            node1.set_configuration_options(values={'internode_compression': 'none'})
            node1.start(wait_for_binary_proto=True)

        sessions = []
        if QUERY_UPGRADED:
            session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((True, session))
        if QUERY_OLD:
            # open a second session with the node on the old version
            node2 = self.cluster.nodelist()[1]
            session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((False, session))

        return sessions

    def get_version(self):
        node1 = self.cluster.nodelist()[0]
        return node1.version()
