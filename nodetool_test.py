import os
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import ToolError

from dtest import Tester, debug, create_ks
from tools.decorators import since
from tools.assertions import assert_none


class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a ToolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except ToolError as e:
            self.assertEqual('', e.stderr)
            self.assertTrue('Unsupported operation' in e.stdout)

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(i % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        for i, node in enumerate(cluster.nodelist()):
            out, err, _ = node.nodetool('info')
            self.assertEqual(0, len(err), err)
            debug(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    self.assertTrue(line.endswith(node.data_center),
                                    "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1]))
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    self.assertTrue(line.endswith(rack),
                                    "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1]))

    @since('3.4')
    def test_nodetool_timeout_commands(self):
        """
        @jira_ticket CASSANDRA-10953

        Test that nodetool gettimeout and settimeout work at a basic level
        """
        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        types = ['read', 'range', 'write', 'counterwrite', 'cascontention',
                 'truncate', 'misc']
        if cluster.version() < '4.0':
            types.append('streamingsocket')
    
        # read all of the timeouts, make sure we get a sane response
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* \d+ ms')

        # set all of the timeouts to 123
        for timeout_type in types:
            _, err, _ = node.nodetool('settimeout {} 123'.format(timeout_type))
            self.assertEqual(0, len(err), err)

        # verify that they're all reported as 123
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* 123 ms')

    @since('2.2')
    def test_cleanup_when_no_replica_with_index(self):
        self._cleanup_when_no_replica(True)

    @since('2.2')
    def test_cleanup_when_no_replica_without_index(self):
        self._cleanup_when_no_replica(False)

    def _cleanup_when_no_replica(self, with_index=False):
        """
        @jira_ticket CASSANDRA-13526
        Test nodetool cleanup KS to remove old data when new replicas in current node instead of directly returning success.
        """
        self.cluster.populate([1, 1]).start(wait_for_binary_proto=True, wait_other_notice=True)

        node_dc1 = self.cluster.nodelist()[0]
        node_dc2 = self.cluster.nodelist()[1]

        # init schema with rf on both data centers
        replication_factor = {'dc1': 1, 'dc2': 1}
        session = self.patient_exclusive_cql_connection(node_dc1, consistency_level=ConsistencyLevel.ALL)
        session_dc2 = self.patient_exclusive_cql_connection(node_dc2, consistency_level=ConsistencyLevel.LOCAL_ONE)
        create_ks(session, 'ks', replication_factor)
        session.execute('CREATE TABLE ks.cf (id int PRIMARY KEY, value text) with dclocal_read_repair_chance = 0 AND read_repair_chance = 0;', trace=False)
        if with_index:
            session.execute('CREATE INDEX value_by_key on ks.cf(value)', trace=False)

        # populate data
        for i in range(0, 100):
            session.execute(SimpleStatement("INSERT INTO ks.cf(id, value) VALUES({}, 'value');".format(i), consistency_level=ConsistencyLevel.ALL))

        # generate sstable
        self.cluster.flush()

        for node in self.cluster.nodelist():
            self.assertNotEqual(0, len(node.get_sstables('ks', 'cf')))
        if with_index:
            self.assertEqual(len(list(session_dc2.execute("SELECT * FROM ks.cf WHERE value = 'value'"))), 100)

        # alter rf to only dc1
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : 1, 'dc2' : 0};")

        # nodetool cleanup on dc2
        node_dc2.nodetool("cleanup ks cf")
        node_dc2.nodetool("compact ks cf")

        # check local data on dc2
        for node in self.cluster.nodelist():
            if node.data_center == 'dc2':
                self.assertEqual(0, len(node.get_sstables('ks', 'cf')))
            else:
                self.assertNotEqual(0, len(node.get_sstables('ks', 'cf')))

        # dc1 data remains
        statement = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.LOCAL_ONE)
        self.assertEqual(len(list(session.execute(statement))), 100)
        if with_index:
            statement = SimpleStatement("SELECT * FROM ks.cf WHERE value = 'value'", consistency_level=ConsistencyLevel.LOCAL_ONE)
            self.assertEqual(len(list(session.execute(statement))), 100)

        # alter rf back to query dc2, no data, no index
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : 0, 'dc2' : 1};")
        assert_none(session_dc2, "SELECT * FROM ks.cf")
        if with_index:
            assert_none(session_dc2, "SELECT * FROM ks.cf WHERE value = 'value'")

    def test_meaningless_notice_in_status(self):
        """
        @jira_ticket CASSANDRA-10176

        nodetool status don't return ownership when there is more than one user keyspace
        define (since they likely have different replication infos making ownership
        meaningless in general) and shows a helpful notice as to why it does that.
        This test checks that said notice is only printed is there is indeed more than
        one user keyspace.
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]

        notice_message = r'effective ownership information is meaningless'

        # Do a first try without any keypace, we shouldn't have the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks1 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 1 keyspace, we should still not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 2 keyspaces with the same settings, we should not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks3 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3}")

        # With a keyspace without the same replication factor, we should get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertRegexpMatches(out, notice_message)

    @since('4.0')
    def test_set_get_batchlog_replay_throttle(self):
        """
        @jira_ticket CASSANDRA-13614

        Test that batchlog replay throttle can be set and get through nodetool
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        cluster.start()

        # Test that nodetool help messages are displayed
        self.assertTrue('Set batchlog replay throttle' in node.nodetool('help setbatchlogreplaythrottle').stdout)
        self.assertTrue('Print batchlog replay throttle' in node.nodetool('help getbatchlogreplaythrottle').stdout)

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setbatchlogreplaythrottle 2048')
        self.assertTrue(len(node.grep_log('Updating batchlog replay throttle to 2048 KB/s, 1024 KB/s per endpoint',
                                          filename='debug.log')) > 0)
        self.assertTrue('Batchlog replay throttle: 2048 KB/s' in node.nodetool('getbatchlogreplaythrottle').stdout)
