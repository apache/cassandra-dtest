import os
from dtest import Tester
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from tools import generate_ssl_stores, putget


class NativeTransportSSL(Tester):
    """
    Native transport integration tests, specifically for ssl and port configurations.
    """

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def connect_to_ssl_test(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client
        """
        cluster = self._populateCluster(enableSSL=True)
        node1 = cluster.nodelist()[0]

        # try to connect without ssl options
        try:
            cluster.start()
            session = self.patient_cql_connection(node1)
            assert False, "Should not be able to connect to SSL socket without SSL enabled client"
        except NoHostAvailable:
            pass

        assert len(node1.grep_log("^io.netty.handler.ssl.NotSslRecordException.*")) > 0, \
            "Missing SSL handshake exception while connecting with non-SSL enabled client"

        # enabled ssl on the client and try again (this should work)
        session = self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
        self._putget(cluster, session)

    def use_custom_port_test(self):
        """
        Connect to non-default native transport port
        """

        cluster = self._populateCluster(nativePort=9567)
        node1 = cluster.nodelist()[0]

        try:
            cluster.start()
            session = self.patient_cql_connection(node1)
            assert False, "Should not be able to connect to non-default port"
        except NoHostAvailable:
            pass

        session = self.patient_cql_connection(node1, port=9567)
        self._putget(cluster, session)

    def use_custom_ssl_port_test(self):
        """
        Connect to additional ssl enabled native transport port
        @jira_ticket CASSANDRA-9590
        """

        cluster = self._populateCluster(enableSSL=True, nativePortSSL=9666)
        node1 = cluster.nodelist()[0]
        cluster.start()

        # we should be able to connect to default non-ssl port
        session = self.patient_cql_connection(node1)
        self._putget(cluster, session)

        # connect to additional dedicated ssl port
        session = self.patient_cql_connection(node1, port=9666, ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
        self._putget(cluster, session, ks='ks2')

    def _populateCluster(self, enableSSL=False, nativePort=None, nativePortSSL=None):
        cluster = self.cluster

        if enableSSL:
            generate_ssl_stores(self.test_path)
            cluster.set_configuration_options({
                'client_encryption_options': {
                    'enabled': True,
                    'keystore': os.path.join(self.test_path, 'keystore.jks'),
                    'keystore_password': 'cassandra'
                }
            })

        if nativePort:
            cluster.set_configuration_options({
                'native_transport_port': nativePort
            })

        if nativePortSSL:
            cluster.set_configuration_options({
                'native_transport_port_ssl': nativePortSSL
            })

        cluster.populate(1)
        return cluster

    def _putget(self, cluster, session, ks='ks', cf='cf'):
        self.create_ks(session, ks, 1)
        self.create_cf(session, cf, compression=None)
        putget(cluster, session, cl=ConsistencyLevel.ONE)
