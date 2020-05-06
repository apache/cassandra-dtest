import os
import os.path
import shutil
import time
import pytest
import logging

from dtest import Tester
from tools import sslkeygen

since = pytest.mark.since
logger = logging.getLogger(__name__)

# as the error message logged will be different per netty ssl implementation (jdk vs openssl (libre vs boring vs ...)),
# the best we can do is just look for a SSLHandshakeException
_LOG_ERR_HANDSHAKE = "javax.net.ssl.SSLHandshakeException"
_LOG_ERR_GENERAL = "javax.net.ssl.SSLException"


@since('3.6')
class TestNodeToNodeSSLEncryption(Tester):

    def test_ssl_enabled(self):
        """Should be able to start with valid ssl options"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2)
        self.cluster.start()
        self.cql_connection(self.node1)

    def test_ssl_correct_hostname_with_validation(self):
        """Should be able to start with valid ssl options"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2, endpoint_verification=True)
        self.fixture_dtest_setup.allow_log_errors = False
        self.cluster.start()
        time.sleep(2)
        self.cql_connection(self.node1)

    def test_ssl_wrong_hostname_no_validation(self):
        """Should be able to start with valid ssl options"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.80")
        credNode2 = sslkeygen.generate_credentials("127.0.0.81", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2, endpoint_verification=False)
        self.cluster.start()
        time.sleep(2)
        self.cql_connection(self.node1)

    def test_ssl_wrong_hostname_with_validation(self):
        """Should be able to start with valid ssl options"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.80")
        credNode2 = sslkeygen.generate_credentials("127.0.0.81", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2, endpoint_verification=True)

        self.fixture_dtest_setup.allow_log_errors = True
        self.cluster.start(no_wait=True)

        found = self._grep_msg(self.node1, _LOG_ERR_HANDSHAKE, _LOG_ERR_GENERAL)
        assert found

        found = self._grep_msg(self.node2, _LOG_ERR_HANDSHAKE, _LOG_ERR_GENERAL)
        assert found

        self.cluster.stop()

    def test_ssl_client_auth_required_fail(self):
        """peers need to perform mutual auth (cient auth required), but do not supply the local cert"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2")

        self.setup_nodes(credNode1, credNode2, client_auth=True)

        self.fixture_dtest_setup.allow_log_errors = True
        self.cluster.start(no_wait=True)
        time.sleep(2)

        found = self._grep_msg(self.node1, _LOG_ERR_HANDSHAKE, _LOG_ERR_GENERAL)
        assert found

        found = self._grep_msg(self.node2, _LOG_ERR_HANDSHAKE, _LOG_ERR_GENERAL)
        assert found

        self.cluster.stop()
        assert found

    def test_ssl_client_auth_required_succeed(self):
        """peers need to perform mutual auth (cient auth required), but do not supply the loca cert"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)
        sslkeygen.import_cert(credNode1.basedir, 'ca127.0.0.2', credNode2.cacert, credNode1.cakeystore)
        sslkeygen.import_cert(credNode2.basedir, 'ca127.0.0.1', credNode1.cacert, credNode2.cakeystore)

        self.setup_nodes(credNode1, credNode2, client_auth=True)

        self.cluster.start()
        self.cql_connection(self.node1)

    def test_ca_mismatch(self):
        """CA mismatch should cause nodes to fail to connect"""
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2")  # mismatching CA!

        self.setup_nodes(credNode1, credNode2)

        self.fixture_dtest_setup.allow_log_errors = True
        self.cluster.start(no_wait=True)

        found = self._grep_msg(self.node1, _LOG_ERR_HANDSHAKE)
        self.cluster.stop()
        assert found

    @since('4.0')
    def test_optional_outbound_tls(self):
        """listen on TLS port, but optionally connect using TLS. this supports the upgrade case of starting with a non-encrypted cluster and then upgrading each node to use encryption.

        @jira_ticket CASSANDRA-10404
        """
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)

        # first, start cluster without TLS (either listening or connecting)
        # Optional should be true by default in 4.0 thanks to CASSANDRA-15262
        self.setup_nodes(credNode1, credNode2, internode_encryption='none')
        self.cluster.start()
        self.cql_connection(self.node1)

        # next connect with TLS for the outbound connections
        self.bounce_node_with_updated_config(credNode1, self.node1, 'all', encryption_optional=True)
        self.bounce_node_with_updated_config(credNode2, self.node2, 'all', encryption_optional=True)

        # now shutdown the plaintext port
        self.bounce_node_with_updated_config(credNode1, self.node1, 'all', encryption_optional=False)
        self.bounce_node_with_updated_config(credNode2, self.node2, 'all', encryption_optional=False)
        self.cluster.stop()

    def bounce_node_with_updated_config(self, credentials, node, internode_encryption, encryption_optional):
        node.stop()
        self.copy_cred(credentials, node, internode_encryption, encryption_optional)
        node.start(wait_for_binary_proto=True)

    def _grep_msg(self, node, *kwargs):
        tries = 30
        while tries > 0:
            try:
                for err in kwargs:
                    m = node.grep_log(err)
                    if m:
                        return True
            except IOError:
                pass  # log does not exists yet
            time.sleep(1)
            tries -= 1

        return False

    def setup_nodes(self, credentials1, credentials2, endpoint_verification=False, client_auth=False, internode_encryption='all', encryption_optional=None):
        cluster = self.cluster
        cluster = cluster.populate(2)
        self.node1 = cluster.nodelist()[0]
        self.copy_cred(credentials1, self.node1, internode_encryption, encryption_optional, endpoint_verification, client_auth)

        self.node2 = cluster.nodelist()[1]
        self.copy_cred(credentials2, self.node2, internode_encryption, encryption_optional, endpoint_verification, client_auth)

    def copy_cred(self, credentials, node, internode_encryption, encryption_optional, endpoint_verification=False, client_auth=False):
        dir = node.get_conf_dir()
        kspath = os.path.join(dir, 'keystore.jks')
        tspath = os.path.join(dir, 'truststore.jks')
        shutil.copyfile(credentials.keystore, kspath)
        shutil.copyfile(credentials.cakeystore, tspath)

        server_enc_options = {
                'internode_encryption': internode_encryption,
                'keystore': kspath,
                'keystore_password': 'cassandra',
                'truststore': tspath,
                'truststore_password': 'cassandra',
                'require_endpoint_verification': endpoint_verification,
                'require_client_auth': client_auth,
            }

        if self.cluster.version() >= '4.0' and encryption_optional is not None:
            server_enc_options['optional'] = encryption_optional

        node.set_configuration_options(values={
            'server_encryption_options': server_enc_options
        })
