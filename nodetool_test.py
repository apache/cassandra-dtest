import os
import pytest
import re
import logging
import subprocess

import ccmlib.node
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import ToolError

from dtest import Tester, create_ks
from tools.assertions import assert_all, assert_invalid, assert_none, assert_stderr_clean
from tools.jmxutils import JolokiaAgent, make_mbean

since = pytest.mark.since
logger = logging.getLogger(__name__)


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
            assert not "Expected nodetool error"
        except ToolError as e:
            assert_stderr_clean(e.stderr)
            assert 'Unsupported operation' in e.stdout

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

        cluster.start()

        for i, node in enumerate(cluster.nodelist()):
            out, err, _ = node.nodetool('info')
            assert_stderr_clean(err)
            out_str = out
            if isinstance(out, (bytes, bytearray)):
                out_str = out.decode("utf-8")
            logger.debug(out_str)
            for line in out_str.split(os.linesep):
                if line.startswith('Data Center'):
                    assert line.endswith(node.data_center), \
                        "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1])
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    assert line.endswith(rack), \
                        "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1])

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
            assert_stderr_clean(err)
            logger.debug(out)
            assert re.search(r'.* \d+ ms', out)

        # set all of the timeouts to 123
        for timeout_type in types:
            _, err, _ = node.nodetool('settimeout {} 123'.format(timeout_type))
            assert_stderr_clean(err)

        # verify that they're all reported as 123
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            assert_stderr_clean(err)
            logger.debug(out)
            assert re.search(r'.* 123 ms', out)

    @since('3.0')
    def test_cleanup_when_no_replica_with_index(self):
        self._cleanup_when_no_replica(True)

    @since('3.0')
    def test_cleanup_when_no_replica_without_index(self):
        self._cleanup_when_no_replica(False)

    def _cleanup_when_no_replica(self, with_index=False):
        """
        @jira_ticket CASSANDRA-13526
        Test nodetool cleanup KS to remove old data when new replicas in current node instead of directly returning success.
        """
        self.cluster.populate([1, 1]).start()

        node_dc1 = self.cluster.nodelist()[0]
        node_dc2 = self.cluster.nodelist()[1]

        # init schema with rf on both data centers
        replication_factor = {'dc1': 1, 'dc2': 1}
        session = self.patient_exclusive_cql_connection(node_dc1, consistency_level=ConsistencyLevel.ALL)
        session_dc2 = self.patient_exclusive_cql_connection(node_dc2, consistency_level=ConsistencyLevel.LOCAL_ONE)
        create_ks(session, 'ks', replication_factor)

        if self.cluster.version() < '4.0':
            session.execute('CREATE TABLE ks.cf (id int PRIMARY KEY, value text) with dclocal_read_repair_chance = 0 AND read_repair_chance = 0;', trace=False)
        else:
            session.execute('CREATE TABLE ks.cf (id int PRIMARY KEY, value text);', trace=False)

        if with_index:
            session.execute('CREATE INDEX value_by_key on ks.cf(value)', trace=False)

        # populate data
        for i in range(0, 100):
            session.execute(SimpleStatement("INSERT INTO ks.cf(id, value) VALUES({}, 'value');".format(i), consistency_level=ConsistencyLevel.ALL))

        # generate sstable
        self.cluster.flush()

        for node in self.cluster.nodelist():
            assert 0 != len(node.get_sstables('ks', 'cf'))
        if with_index:
            assert 100 == len(list(session_dc2.execute("SELECT * FROM ks.cf WHERE value = 'value'"))), 100

        # alter rf to only dc1
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : 1, 'dc2' : 0};")

        # nodetool cleanup on dc2
        node_dc2.nodetool("cleanup ks cf")
        node_dc2.nodetool("compact ks cf")

        # check local data on dc2
        for node in self.cluster.nodelist():
            if node.data_center == 'dc2':
                assert 0 == len(node.get_sstables('ks', 'cf'))
            else:
                assert 0 != len(node.get_sstables('ks', 'cf'))

        # dc1 data remains
        statement = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.LOCAL_ONE)
        assert 100 == len(list(session.execute(statement)))
        if with_index:
            statement = SimpleStatement("SELECT * FROM ks.cf WHERE value = 'value'", consistency_level=ConsistencyLevel.LOCAL_ONE)
            assert len(list(session.execute(statement))) == 100

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
        assert_stderr_clean(err)
        assert not re.search(notice_message, out)

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks1 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 1 keyspace, we should still not get the notice
        out, err, _ = node.nodetool('status')
        assert_stderr_clean(err)
        assert not re.search(notice_message, out)

        session.execute("CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 2 keyspaces with the same settings, we should not get the notice
        out, err, _ = node.nodetool('status')
        assert_stderr_clean(err)
        assert not re.search(notice_message, out)

        session.execute("CREATE KEYSPACE ks3 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3}")

        # With a keyspace without the same replication factor, we should get the notice
        out, err, _ = node.nodetool('status')
        assert_stderr_clean(err)
        assert re.search(notice_message, out)

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
        assert 'Set batchlog replay throttle' in node.nodetool('help setbatchlogreplaythrottle').stdout
        assert 'Print batchlog replay throttle' in node.nodetool('help getbatchlogreplaythrottle').stdout

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setbatchlogreplaythrottle 2048')
        assert len(node.grep_log('Updating batchlog replay throttle to 2048 KB/s, 1024 KB/s per endpoint',
                                 filename='debug.log')) >= 0
        assert 'Batchlog replay throttle: 2048 KB/s' in node.nodetool('getbatchlogreplaythrottle').stdout

    @since('3.0')
    def test_reloadlocalschema(self):
        """
        @jira_ticket CASSANDRA-13954

        Test that `nodetool reloadlocalschema` works as intended
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        cluster.start()

        session = self.patient_cql_connection(node)

        query = "CREATE KEYSPACE IF NOT EXISTS test WITH replication " \
                "= {'class': 'NetworkTopologyStrategy', 'datacenter1': 2};"
        session.execute(query)

        query = 'CREATE TABLE test.test (pk int, ck int, PRIMARY KEY (pk, ck));'
        session.execute(query)

        ss = make_mbean('db', type='StorageService')

        # get initial schema version
        with JolokiaAgent(node) as jmx:
            schema_version = jmx.read_attribute(ss, 'SchemaVersion')

        # manually add a regular column 'val' to test.test
        query = """
            INSERT INTO system_schema.columns
                (keyspace_name, table_name, column_name, clustering_order,
                 column_name_bytes, kind, position, type)
            VALUES
                ('test', 'test', 'val', 'none',
                 0x76616c, 'regular', -1, 'int');"""
        session.execute(query)

        # validate that schema version wasn't automatically updated
        with JolokiaAgent(node) as jmx:
            assert schema_version == jmx.read_attribute(ss, 'SchemaVersion')

        # make sure the new column wasn't automagically picked up
        assert_invalid(session, 'INSERT INTO test.test (pk, ck, val) VALUES (0, 1, 2);')

        # force the node to reload schema from disk
        node.nodetool('reloadlocalschema')

        # validate that schema version changed
        with JolokiaAgent(node) as jmx:
            assert schema_version != jmx.read_attribute(ss, 'SchemaVersion')

        # try an insert with the new column again and validate it succeeds this time
        session.execute('INSERT INTO test.test (pk, ck, val) VALUES (0, 1, 2);')
        assert_all(session, 'SELECT pk, ck, val FROM test.test;', [[0, 1, 2]])

    @since('3.0')
    def test_refresh_size_estimates_clears_invalid_entries(self):
        """
        @jira_ticket CASSANDRA-14905
         nodetool refreshsizeestimates should clear up entries for tables that no longer exist
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        cluster.start()
        session = self.patient_exclusive_cql_connection(node)
        session.execute("USE system;")
        # Valid keyspace but invalid table
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ('system_auth', 'bad_table', '-5', '5', 0, 0);")
        # Invalid keyspace and table
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ('bad_keyspace', 'bad_table', '-5', '5', 0, 0);")
        node.nodetool('refreshsizeestimates')
        assert_none(session, "SELECT * FROM size_estimates WHERE keyspace_name='system_auth' AND table_name='bad_table'")
        assert_none(session, "SELECT * FROM size_estimates WHERE keyspace_name='bad_keyspace'")

    @since('4.0')
    def test_set_get_concurrent_view_builders(self):
        """
        @jira_ticket CASSANDRA-12245

        Test that the number of concurrent view builders can be set and get through nodetool
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        cluster.start()

        # Test that nodetool help messages are displayed
        assert 'Set the number of concurrent view' in node.nodetool('help setconcurrentviewbuilders').stdout
        assert 'Get the number of concurrent view' in node.nodetool('help getconcurrentviewbuilders').stdout

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setconcurrentviewbuilders 4')
        assert 'Current number of concurrent view builders in the system is: \n4' \
               in node.nodetool('getconcurrentviewbuilders').stdout

        # Try to set an invalid zero value
        try:
            node.nodetool('setconcurrentviewbuilders 0')
        except ToolError as e:
            assert 'concurrent_view_builders should be great than 0.' in e.stdout
            assert 'Number of concurrent view builders should be greater than 0.', e.message
        else:
            pytest.fail("Expected error when setting and invalid value")

    @since('4.0')
    def test_describecluster_more_information_three_datacenters(self):
        """
        nodetool describecluster should be more informative. It should include detailes
        for total node count, list of datacenters, RF, number of nodes per dc, how many
        are down and version(s).
        @jira_ticket CASSANDRA-13853
        @expected_result This test invokes nodetool describecluster and matches the output with the expected one
        """
        cluster = self.cluster
        cluster.populate([1, 2, 1]).start()

        node1_dc1, node1_dc2, node2_dc2, node1_dc3 = cluster.nodelist()

        session_dc1 = self.patient_cql_connection(node1_dc1)
        session_dc1.execute("create KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2':5, 'dc3':1}")

        session_dc3 = self.patient_cql_connection(node1_dc3)
        session_dc3.execute("create KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2':5, 'dc3':1}")

        all_nodes = cluster.nodelist()

        out_node1_dc1, node1_dc1_sorted = self._describe(all_nodes.pop())

        for node in all_nodes:
            out, out_sorted = self._describe(node)
            assert node1_dc1_sorted == out_sorted

        logger.debug(out_node1_dc1)
        assert 'Live: 4' in out_node1_dc1
        assert 'Joining: 0' in out_node1_dc1
        assert 'Moving: 0' in out_node1_dc1
        assert 'Leaving: 0' in out_node1_dc1
        assert 'Unreachable: 0' in out_node1_dc1
        assert 'Data Centers:' in out_node1_dc1
        assert 'dc1 #Nodes: 1 #Down: 0' in out_node1_dc1
        assert 'dc2 #Nodes: 2 #Down: 0' in out_node1_dc1
        assert 'dc3 #Nodes: 1 #Down: 0' in out_node1_dc1
        assert 'Keyspaces:' in out_node1_dc1

        expected_keyspaces = [('system_schema', 'LocalStrategy', {''}),
                              ('system', 'LocalStrategy', {''}),
                              ('system_traces', 'SimpleStrategy', {'replication_factor=2'}),
                              ('system_distributed', 'SimpleStrategy', {'replication_factor=3'}),
                              ('system_auth', 'SimpleStrategy', {'replication_factor=1'}),
                              ('ks1', 'NetworkTopologyStrategy', {'dc1=3','dc2=5','dc3=1'}),
                              ('ks2', 'NetworkTopologyStrategy', {'dc1=3','dc2=5','dc3=1'})]

        for (ks, strategy, _) in expected_keyspaces:
            assert "{} -> Replication class: {}".format(ks, strategy) in out_node1_dc1 # replication factor is verified below

        # now check db versions & replication factor:
        # Database versions:
        #       4.0.0: [127.0.0.6:7000, 127.0.0.5:7000, 127.0.0.4:7000, 127.0.0.3:7000, 127.0.0.2:7000, 127.0.0.1:7000]

        lines = out_node1_dc1.splitlines()
        rex = r'(\S+)\s\[(.*)\]'
        found_keyspaces = False
        found_database = False
        verified_rfs = 0
        for i in range(0, len(lines)):
            if 'Keyspaces' in lines[i]:
                found_keyspaces = True
                for x in range(i+1, len(lines)):
                    for (ks, strategy, replication) in expected_keyspaces:
                        if "{} ->".format(ks) in lines[x]:
                            verified_rfs += 1
                            assert strategy in lines[x]
                            assert replication == self._get_replication(lines[x])

            if 'Database versions:' in lines[i]:
                found_database = True
                m = re.search(rex, lines[i+1])
                # group(1) is the version, and all nodes are on the same version
                assert "{}".format(node1_dc1.get_cassandra_version()) in m.group(1)
                nodestring = m.group(2)
                for n in cluster.nodelist():
                    assert n.address_and_port() in nodestring

        assert found_keyspaces
        assert found_database
        assert verified_rfs == len(expected_keyspaces)

    def _get_replication(self, line):
        # ks1 -> Replication class: NetworkTopologyStrategy {dc2=5, dc1=3, dc3=1}
        repl_rex = r'{(.*)}'
        repl_m = re.search(repl_rex, line)
        return {x.strip() for x in repl_m.group(1).split(",")}

    def _describe(self, node):
        node_describe, err, _ = node.nodetool('describecluster')
        assert_stderr_clean(err)
        out_sorted = node_describe.split()
        out_sorted.sort()
        return (node_describe, out_sorted)

    @since('4.0')
    def test_sjk(self):
        """
        Verify that SJK generally works.
        """

        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        out, err, _ = node.nodetool('sjk --help')
        logger.debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if "    ttop      [Thread Top] Displays threads from JVM process" == line:
                hasPattern = True
        assert hasPattern == True, "Expected help about SJK ttop"

        out, err, _ = node.nodetool('sjk')
        logger.debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if "    ttop      [Thread Top] Displays threads from JVM process" == line:
                hasPattern = True
        assert hasPattern == True, "Expected help about SJK ttop"

        out, err, _ = node.nodetool('sjk hh -n 10 --live')
        logger.debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if re.match('.*Instances.*Bytes.*Type.*', line):
                hasPattern = True
        assert hasPattern == True, "Expected 'SJK hh' output"

    @since('3.0', max_version='3.x')
    def test_jobs_option_warning(self):
        """
        Verify that nodetool -j/--jobs option warning is raised depending on the value of `concurrent_compactors` in the
        target node, independently from where the tool is used.

        Before CASSANDRA-16104 the warning was based on the local value of `concurrent_compactors`, and not in the value
        used in the target node, which is got through JMX.

        From 4.0 we have a JUnit test in place that supersedes this test.

        @jira_ticket CASSANDRA-16104
        """

        # setup a cluster with a different value for concurrent_compactors in each node
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        node1.set_configuration_options(values={'concurrent_compactors': '1'})
        node2.set_configuration_options(values={'concurrent_compactors': '10'})
        cluster.start()

        # we will invoke nodetool always from node1 environment
        tool = node1.get_tool('nodetool')
        env = node1.get_env()
        warning = 'jobs (10) is bigger than configured concurrent_compactors (1)'

        def nodetool(node):
            cmd = [tool, '-h', 'localhost', '-p', str(node.jmx_port), 'upgradesstables', '-j', '10']
            p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            return ccmlib.node.handle_external_tool_process(p, cmd)

        # from node1 environment, connect to node1 and verify that the warning is raised
        out, err, _ = nodetool(node1)
        assert_stderr_clean(err)
        assert warning in out

        # from node1 environment, connect to node2 and verify that the warning is not raised
        out, err, _ = nodetool(node2)
        assert_stderr_clean(err)
        assert warning not in out

    def test_ipv4_ipv6_host(self):
        """
        Test that both ipv4 and ipv6 hosts are accepted by nodetool
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start()

        node = cluster.nodelist()[0]
        tool = node.get_tool('nodetool')
        env = node.get_env()

        def nodetool(host):
            cmd = [tool, '-h', host, '-p', str(node.jmx_port), 'status']
            p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            return ccmlib.node.handle_external_tool_process(p, cmd)

        # ipv4 should work
        nodetool('127.0.0.1')
        # if ipv6 fails, make sure the reason is valid
        try:
            nodetool('::1')
        except ToolError as e:
            assert any(reason in e.stderr for reason in ("Connection refused", "Protocol family unavailable"))


