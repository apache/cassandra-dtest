# coding: utf-8

import time

from cassandra.query import dict_factory
from nose.tools import assert_equal, assert_true
from ccmlib.node import NodeError

from dtest import Tester, debug
from cassandra.protocol import ConfigurationException
from tools.decorators import since

VERSION_311 = 'github:apache/cassandra-3.11'
VERSION_TRUNK = 'github:apache/trunk'


@since('4.0')
class UpgradeSuperColumnsThrough(Tester):
    def upgrade_to_version(self, tag, start_rpc=True, wait=True, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            node.start(wait_other_notice=wait, wait_for_binary_proto=wait)

    def prepare(self, num_nodes=1, cassandra_version="github:apache/cassandra-2.2"):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version=cassandra_version)

        cluster.populate(num_nodes)

        cluster.start()
        return cluster

    def upgrade_compact_storage_test(self):
        cluster = self.prepare(cassandra_version='github:apache/cassandra-3.0')
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")

        for i in xrange(1, 5):
            session.execute("INSERT INTO ks.compact_table (pk, col1, col2) VALUES ({i}, {i}, {i})".format(i=i))

        self.upgrade_to_version(VERSION_TRUNK, wait=False)
        self.allow_log_errors = True

        time.sleep(5)
        # After restart, it won't start
        errors = len(node.grep_log("Compact Tables are not allowed in Cassandra starting with 4.0 version"))
        assert_true(errors > 0)

    def mixed_cluster_test(self):
        cluster = self.prepare(num_nodes=2, cassandra_version=VERSION_311)
        node1, node2 = self.cluster.nodelist()

        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        node1.set_install_dir(version=VERSION_TRUNK)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        session = self.patient_cql_connection(node2, row_factory=dict_factory)

        # Schema propagation will time out
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '2' };")
        thrown = False
        try:
            session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")
        except ConfigurationException:
            thrown = True

        assert_true(thrown)

    def upgrade_with_dropped_compact_storage_test(self):
        cluster = self.prepare(cassandra_version=VERSION_311)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")

        for i in xrange(1, 5):
            session.execute("INSERT INTO ks.compact_table (pk, col1, col2) VALUES ({i}, {i}, {i})".format(i=i))

        session.execute("ALTER TABLE ks.compact_table DROP COMPACT STORAGE")

        self.upgrade_to_version(VERSION_TRUNK, wait=True)

        session = self.patient_cql_connection(node, row_factory=dict_factory)
        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE pk = 1")),
                     [{u'col2': 1, u'pk': 1, u'column1': None, u'value': None, u'col1': 1}])

    def force_readd_compact_storage_test(self):
        cluster = self.prepare(cassandra_version=VERSION_311)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")

        for i in xrange(1, 5):
            session.execute("INSERT INTO ks.compact_table (pk, col1, col2) VALUES ({i}, {i}, {i})".format(i=i))

        session.execute("ALTER TABLE ks.compact_table DROP COMPACT STORAGE")

        self.upgrade_to_version(VERSION_TRUNK, wait=True)

        session = self.patient_cql_connection(node, row_factory=dict_factory)
        session.execute("update system_schema.tables set flags={} where keyspace_name='ks' and table_name='compact_table';")

        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE pk = 1")),
                     [{u'col2': 1, u'pk': 1, u'column1': None, u'value': None, u'col1': 1}])

        self.allow_log_errors = True

        node.stop(wait_other_notice=False)
        node.set_install_dir(version=VERSION_TRUNK)
        try:
            node.start(wait_other_notice=False, wait_for_binary_proto=False, verbose=False)
        except (NodeError):
            print "error"  # ignore
        time.sleep(5)
        # After restart, it won't start
        errors = len(node.grep_log("Compact Tables are not allowed in Cassandra starting with 4.0 version"))
        assert_true(errors > 0)

    def upgrade_with_dropped_compact_storage_index_test(self):
        cluster = self.prepare(cassandra_version=VERSION_311)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk ascii PRIMARY KEY, col1 ascii) WITH COMPACT STORAGE")
        session.execute("CREATE INDEX ON ks.compact_table(col1)")

        for i in xrange(1, 10):
            session.execute("INSERT INTO ks.compact_table (pk, col1) VALUES ('{pk}', '{col1}')".format(pk=i, col1=i * 10))

        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")),
                     [{u'pk': '5', u'col1': '50'}])
        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")),
                     [{u'pk': '5', u'col1': '50'}])
        session.execute("ALTER TABLE ks.compact_table DROP COMPACT STORAGE")

        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")),
                     [{u'col1': '50', u'column1': None, u'pk': '5', u'value': None}])
        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")),
                     [{u'col1': '50', u'column1': None, u'pk': '5', u'value': None}])

        self.upgrade_to_version(VERSION_TRUNK, wait=True)

        session = self.patient_cql_connection(node, row_factory=dict_factory)

        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")),
                     [{u'col1': '50', u'column1': None, u'pk': '5', u'value': None}])
        assert_equal(list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")),
                     [{u'col1': '50', u'column1': None, u'pk': '5', u'value': None}])
