import os
import os.path
import shutil
import time

from dtest import Tester
from tools import sslkeygen
from tools.decorators import since

_LOG_ERR_SIG = "^javax.net.ssl.SSLHandshakeException: sun.security.validator.ValidatorException: Certificate signature validation failed$"
_LOG_ERR_IP = "^javax.net.ssl.SSLHandshakeException: java.security.cert.CertificateException: No subject alternative names matching IP address [0-9.]+ found$"
_LOG_ERR_HOST = "^javax.net.ssl.SSLHandshakeException: java.security.cert.CertificateException: No name matching \S+ found$"
_LOG_ERR_CERT = "^javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_unknown$"


@since('3.6')
class TestNodeToNodeSSLEncryption(Tester):

    def ssl_enabled_test(self):
        """Should be able to start with valid ssl options"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2)
        self.cluster.start()
        self.cql_connection(self.node1)

    def ssl_wrong_hostname_no_validation_test(self):
        """Should be able to start with valid ssl options"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.80")
        credNode2 = sslkeygen.generate_credentials("127.0.0.81", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2, endpointVerification=False)
        self.cluster.start()
        time.sleep(2)
        self.cql_connection(self.node1)

    def ssl_wrong_hostname_with_validation_test(self):
        """Should be able to start with valid ssl options"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.80")
        credNode2 = sslkeygen.generate_credentials("127.0.0.81", credNode1.cakeystore, credNode1.cacert)

        self.setup_nodes(credNode1, credNode2, endpointVerification=True)

        self.allow_log_errors = True
        self.cluster.start(no_wait=True)

        found = self._grep_msg(self.node1, _LOG_ERR_IP, _LOG_ERR_HOST)
        self.assertTrue(found)

        found = self._grep_msg(self.node2, _LOG_ERR_IP, _LOG_ERR_HOST)
        self.assertTrue(found)

        self.cluster.stop()
        self.assertTrue(found)

    def ssl_client_auth_required_fail_test(self):
        """peers need to perform mutual auth (cient auth required), but do not supply the local cert"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2")

        self.setup_nodes(credNode1, credNode2, client_auth=True)

        self.allow_log_errors = True
        self.cluster.start(no_wait=True)
        time.sleep(2)

        found = self._grep_msg(self.node1, _LOG_ERR_CERT)
        self.assertTrue(found)

        found = self._grep_msg(self.node2, _LOG_ERR_CERT)
        self.assertTrue(found)

        self.cluster.stop()
        self.assertTrue(found)

    def ssl_client_auth_required_succeed_test(self):
        """peers need to perform mutual auth (cient auth required), but do not supply the loca cert"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)
        sslkeygen.import_cert(credNode1.basedir, 'ca127.0.0.2', credNode2.cacert, credNode1.cakeystore)
        sslkeygen.import_cert(credNode2.basedir, 'ca127.0.0.1', credNode1.cacert, credNode2.cakeystore)

        self.setup_nodes(credNode1, credNode2, client_auth=True)

        self.cluster.start()
        self.cql_connection(self.node1)

    def ca_mismatch_test(self):
        """CA mismatch should cause nodes to fail to connect"""

        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2")  # mismatching CA!

        self.setup_nodes(credNode1, credNode2)

        self.allow_log_errors = True
        self.cluster.start(no_wait=True)

        found = self._grep_msg(self.node1, _LOG_ERR_SIG)
        self.cluster.stop()
        self.assertTrue(found)

    def _grep_msg(self, node, *kwargs):
        tries = 30
        while tries > 0:
            try:
                print("Checking logs for error")
                for err in kwargs:
                    m = node.grep_log(err)
                    if m:
                        print("Found log message: {}".format(m[0]))
                        return True
            except IOError:
                pass  # log does not exists yet
            time.sleep(1)
            tries -= 1

        return False

    def setup_nodes(self, credentials1, credentials2, endpointVerification=False, client_auth=False):

        cluster = self.cluster

        def copy_cred(credentials, node):
            dir = node.get_conf_dir()
            print("Copying credentials to node %s" % dir)
            kspath = os.path.join(dir, 'keystore.jks')
            tspath = os.path.join(dir, 'truststore.jks')
            shutil.copyfile(credentials.keystore, kspath)
            shutil.copyfile(credentials.cakeystore, tspath)

            node.set_configuration_options(values={
                'server_encryption_options': {
                    'internode_encryption': 'all',
                    'keystore': kspath,
                    'keystore_password': 'cassandra',
                    'truststore': tspath,
                    'truststore_password': 'cassandra',
                    'require_endpoint_verification': endpointVerification,
                    'require_client_auth': client_auth
                }
            })

        cluster = cluster.populate(2)
        self.node1 = cluster.nodelist()[0]
        copy_cred(credentials1, self.node1)

        self.node2 = cluster.nodelist()[1]
        copy_cred(credentials2, self.node2)
