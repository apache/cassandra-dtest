import os
import pytest
import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable

from dtest import Tester, create_ks, create_cf
from tools.data import putget
from tools.misc import generate_ssl_stores

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestNativeTransportSSL(Tester):
    """
    Native transport integration tests, specifically for ssl and port configurations.
    """

    def test_connect_to_ssl(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client
        """
        cluster = self._populateCluster(enableSSL=True)
        node1 = cluster.nodelist()[0]

        cluster.start()

        try:  # hack around assertRaise's lack of msg parameter
            # try to connect without ssl options
            self.patient_cql_connection(node1)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        if cluster.version() >= '4.0':
            assert len(node1.grep_log("javax.net.ssl.SSLHandshakeException")) > 0, \
                    "Missing SSL handshake exception while connecting with non-SSL enabled client"
        else:
            assert len(node1.grep_log("io.netty.handler.ssl.NotSslRecordException.*")) > 0, \
                    "Missing SSL handshake exception while connecting with non-SSL enabled client"

        # enabled ssl on the client and try again (this should work)
        session = self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(self.fixture_dtest_setup.test_path, 'ccm_node.cer')})
        self._putget(cluster, session)

    def test_connect_to_ssl_optional(self):
        """
        Connecting to SSL optional native transport port must be possible with SSL and non-SSL native clients
        @jira_ticket CASSANDRA-10559
        """
        cluster = self._populateCluster(enableSSL=True, sslOptional=True)
        node1 = cluster.nodelist()[0]

        # try to connect without ssl options
        cluster.start()
        session = self.patient_cql_connection(node1)
        self._putget(cluster, session)

        # enabled ssl on the client and try again (this should work)
        session = self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(self.fixture_dtest_setup.test_path, 'ccm_node.cer')})
        self._putget(cluster, session, ks='ks2')

    def test_use_custom_port(self):
        """
        Connect to non-default native transport port
        """
        cluster = self._populateCluster(nativePort=9567)
        node1 = cluster.nodelist()[0]

        cluster.start()
        try:  # hack around assertRaise's lack of msg parameter
            self.patient_cql_connection(node1)
            self.fail('Should not be able to connect to non-default port')
        except NoHostAvailable:
            pass

        session = self.patient_cql_connection(node1, port=9567)
        self._putget(cluster, session)

    @since('3.0')
    def test_use_custom_ssl_port(self):
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
        session = self.patient_cql_connection(node1, port=9666, ssl_opts={'ca_certs': os.path.join(self.fixture_dtest_setup.test_path, 'ccm_node.cer')})
        self._putget(cluster, session, ks='ks2')

    def _populateCluster(self, enableSSL=False, nativePort=None, nativePortSSL=None, sslOptional=False):
        cluster = self.cluster

        if enableSSL:
            generate_ssl_stores(self.fixture_dtest_setup.test_path)
            cluster.set_configuration_options({
                'client_encryption_options': {
                    'enabled': True,
                    'optional': sslOptional,
                    'keystore': os.path.join(self.fixture_dtest_setup.test_path, 'keystore.jks'),
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
        create_ks(session, ks, 1)
        create_cf(session, cf, compression=None)
        putget(cluster, session, cl=ConsistencyLevel.ONE)
