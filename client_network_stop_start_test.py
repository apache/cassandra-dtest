import logging
import os
import os.path
import pytest
import shutil
import string
import time

from ccmlib.node import TimeoutError
from distutils.version import LooseVersion
from dtest import Tester
from tools import sslkeygen

since = pytest.mark.since
logger = logging.getLogger(__name__)

# see https://issues.apache.org/jira/browse/CASSANDRA-16127
class TestClientNetworkStopStart(Tester):

    def _normalize(self, a):
        return a.translate(str.maketrans(dict.fromkeys(string.whitespace)))

    def _in(self, a, b):
        return self._normalize(a) in self._normalize(b)

    def _assert_client_active_msg(self, name, enabled, out):
        expected = "{} active: {}".format(name, str(enabled).lower())
        actived = "actived" if enabled else "deactivated"
        assert self._in(expected, out), "{} is expected to be {} ({}) but was not found in output: {}".format(name, actived, str(enabled).lower(), out)

    def _assert_watch_log_for(self, node_or_cluster, to_watch, assert_msg=None):
        if assert_msg is None:
            assert_msg = "Unable to locate '{}'".format(to_watch)
        nodelist_fn = getattr(node_or_cluster, "nodelist", None)
        logger.debug("watching for '{}'".format(to_watch))
        start = time.perf_counter()
        if callable(nodelist_fn):
            for node in nodelist_fn():
                assert node.watch_log_for_no_errors(to_watch), assert_msg
        else:
            assert node_or_cluster.watch_log_for_no_errors(to_watch), assert_msg
        logger.debug("Completed watching for '{}'; took {}s".format(to_watch, time.perf_counter() - start))

    def _assert_binary_actually_found(self, node_or_cluster):
        # ccm will silently move on if the logs don't have CQL in time, which then leads to
        # flaky tests; to avoid that force waiting to be correct and assert the log was seen.
        logger.debug("Verifying that the CQL log was seen and that ccm didn't return early...")
        self._assert_watch_log_for(node_or_cluster, "Starting listening for CQL clients on", "Binary didn't start...")

    def _assert_client_enable(self, node, native_enabled=True, thrift_enabled=False):
        out = node.nodetool("info")
        self._assert_client_active_msg("Native Transport", native_enabled, out.stdout)
        if node.get_cassandra_version() >= LooseVersion('4.0'):
            assert "Thrift" not in out.stdout, "Thrift found in output: {}".format(out.stdout)
        else:
            self._assert_client_active_msg("Thrift", thrift_enabled, out.stdout)

    def _assert_startup(self, node_or_cluster):
        """Checks to see if the startup message was found"""
        self._assert_watch_log_for(node_or_cluster, "Startup complete", "Unable to find startup message, either the process crashed or is missing CASSANDRA-16127")

    @since(['2.2', '3.0.23', '3.11.9'])
    def test_defaults(self):
        """Tests default configurations have the correct client network setup"""
        cluster = self.cluster
        logger.debug("Starting cluster..")
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(1).start(wait_for_binary_proto=True)
        self._assert_binary_actually_found(cluster)
        self._assert_startup(cluster)
        node = cluster.nodelist()[0]
        self._assert_client_enable(node)

    @since(['2.2', '3.0.23', '3.11.9'], max_version='3.11.x')
    def test_hsha_defaults(self):
        """Enables hsha"""
        cluster = self.cluster
        logger.debug("Starting cluster..")
        cluster.set_configuration_options(values={
            'rpc_server_type': 'hsha',
            # seems 1 causes a dead lock... heh...
            'rpc_min_threads': 16,
            'rpc_max_threads': 2048
        })
        cluster.populate(1).start(wait_for_binary_proto=True)
        self._assert_binary_actually_found(cluster)
        self._assert_startup(cluster)
        node = cluster.nodelist()[0]
        self._assert_client_enable(node)

    @since(['2.2', '3.0.23', '3.11.9'], max_version='3.11.x')
    def test_hsha_with_ssl(self):
        """Enables hsha with ssl"""
        cluster = self.cluster
        logger.debug("Starting cluster..")
        cred = sslkeygen.generate_credentials("127.0.0.1")
        cluster.set_configuration_options(values={
            'rpc_server_type': 'hsha',
            # seems 1 causes a dead lock... heh...
            'rpc_min_threads': 16,
            'rpc_max_threads': 2048,
            'client_encryption_options': {
                'enabled': True,
                'optional': False,
                'keystore': cred.cakeystore,
                'keystore_password': 'cassandra'
            }
        })
        cluster.populate(1).start(wait_for_binary_proto=True)
        self._assert_binary_actually_found(cluster)
        self._assert_startup(cluster)
        node = cluster.nodelist()[0]
        self._assert_client_enable(node)
