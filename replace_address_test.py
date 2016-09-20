import os
import tempfile
from itertools import chain
from shutil import rmtree
from unittest import skipIf

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node
from nose.plugins.attrib import attr

from dtest import CASSANDRA_VERSION_FROM_BUILD, DISABLE_VNODES, Tester, debug
from tools.assertions import assert_bootstrap_state, assert_all, assert_not_running
from tools.data import rows_to_list
from tools.decorators import known_failure, since
from tools.misc import ImmutableMapping


class NodeUnavailable(Exception):
    pass


class BaseReplaceAddressTest(Tester):
    __test__ = False
    replacement_node = None
    cluster_options = ImmutableMapping({'start_rpc': 'true'})
    ignore_log_patterns = (
        # This one occurs when trying to send the migration to a
        # node that hasn't started yet, and when it does, it gets
        # replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
        r'Migration task failed to complete',  # 10978
        # ignore streaming error during bootstrap
        r'Streaming error occurred',
        r'failed stream session'
    )

    def _setup(self, n=3, opts=None, enable_byteman=False, mixed_versions=False):
        debug("Starting cluster with {} nodes.".format(n))
        self.cluster.populate(n)
        if opts is not None:
            debug("Setting cluster options: {}".format(opts))
            self.cluster.set_configuration_options(opts)

        self.cluster.set_batch_commitlog(enabled=True)
        self.query_node = self.cluster.nodelist()[0]
        self.replaced_node = self.cluster.nodelist()[-1]

        self.cluster.seeds.remove(self.replaced_node)
        NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
        if DISABLE_VNODES:
            self.cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 1})
        else:
            self.cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})

        if enable_byteman:
            # set up byteman
            self.query_node.byteman_port = '8100'
            self.query_node.import_config_files()

        if mixed_versions:
            debug("Starting nodes on version 2.2.4")
            self.cluster.set_install_dir(version="2.2.4")

        self.cluster.start()

        if self.cluster.cassandra_version() >= '2.2.0':
            session = self.patient_cql_connection(self.query_node)
            # Change system_auth keyspace replication factor to 2, otherwise replace will fail
            session.execute("""ALTER KEYSPACE system_auth
                            WITH replication = {'class':'SimpleStrategy',
                            'replication_factor':2};""")

    def _do_replace(self, same_address=False, jvm_option='replace_address',
                    wait_other_notice=False, wait_for_binary_proto=True,
                    replace_address=None, opts=None, data_center=None,
                    extra_jvm_args=None):
        if replace_address is None:
            replace_address = self.replaced_node.address()

        # only create node if it's not yet created
        if self.replacement_node is None:
            replacement_address = '127.0.0.4'
            if same_address:
                replacement_address = self.replaced_node.address()
                self.cluster.remove(self.replaced_node)

            debug("Starting replacement node {} with jvm_option '{}={}'".format(replacement_address, jvm_option, replace_address))
            self.replacement_node = Node('replacement', cluster=self.cluster, auto_bootstrap=True,
                                         thrift_interface=(replacement_address, 9160), storage_interface=(replacement_address, 7000),
                                         jmx_port='7400', remote_debug_port='0', initial_token=None, binary_interface=(replacement_address, 9042))
            if opts is not None:
                debug("Setting options on replacement node: {}".format(opts))
                self.replacement_node.set_configuration_options(opts)
            self.cluster.add(self.replacement_node, False, data_center=data_center)

        if extra_jvm_args is None:
            extra_jvm_args = []
        extra_jvm_args.extend(["-Dcassandra.{}={}".format(jvm_option, replace_address),
                               "-Dcassandra.ring_delay_ms=10000",
                               "-Dcassandra.broadcast_interval_ms=10000"])

        self.replacement_node.start(jvm_args=extra_jvm_args,
                                    wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice)

        if self.cluster.cassandra_version() >= '2.2.8' and same_address:
            self.replacement_node.watch_log_for("Writes will not be forwarded to this node during replacement",
                                                timeout=60)

    def _stop_node_to_replace(self, gently=False, table='keyspace1.standard1', cl=ConsistencyLevel.THREE):
        if self.replaced_node.is_running():
            debug("Stopping {}".format(self.replaced_node.name))
            self.replaced_node.stop(gently=gently, wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                session = self.patient_cql_connection(self.query_node)
                query = SimpleStatement('select * from {}'.format(table), consistency_level=cl)
                session.execute(query)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

    def _insert_data(self, n='1k', rf=3, whitelist=False):
        debug("Inserting {} entries with rf={} with stress...".format(n, rf))
        self.query_node.stress(['write', 'n={}'.format(n), 'no-warmup', '-schema', 'replication(factor={})'.format(rf)],
                               whitelist=whitelist)
        self.cluster.flush()

    def _fetch_initial_data(self, table='keyspace1.standard1', cl=ConsistencyLevel.THREE, limit=10000):
        debug("Fetching initial data from {} on {} with CL={} and LIMIT={}".format(table, self.query_node.name, cl, limit))
        session = self.patient_cql_connection(self.query_node)
        query = SimpleStatement('select * from {} LIMIT {}'.format(table, limit), consistency_level=cl)
        return rows_to_list(session.execute(query))

    def _verify_data(self, initial_data, table='keyspace1.standard1', cl=ConsistencyLevel.ONE, limit=10000,
                     restart_nodes=False):
        self.assertGreater(len(initial_data), 0, "Initial data must be greater than 0")

        # query should work again
        debug("Stopping old nodes")
        for node in self.cluster.nodelist():
            if node.is_running() and node != self.replacement_node:
                debug("Stopping {}".format(node.name))
                node.stop(gently=False, wait_other_notice=True)

        debug("Verifying {} on {} with CL={} and LIMIT={}".format(table, self.replacement_node.address(), cl, limit))
        session = self.patient_exclusive_cql_connection(self.replacement_node)
        assert_all(session, 'select * from {} LIMIT {}'.format(table, limit),
                   expected=initial_data,
                   cl=cl)

    def _verify_replacement(self, node, same_address):
        if not same_address:
            if self.cluster.cassandra_version() >= '2.2.7':
                node.watch_log_for("Node /{} is replacing /{}"
                                   .format(self.replacement_node.address(),
                                           self.replaced_node.address()),
                                   timeout=60, filename='debug.log')
                node.watch_log_for("Node /{} will complete replacement of /{} for tokens"
                                   .format(self.replacement_node.address(),
                                           self.replaced_node.address()), timeout=10)
                node.watch_log_for("removing endpoint /{}".format(self.replaced_node.address()),
                                   timeout=60, filename='debug.log')
            else:
                node.watch_log_for("between /{} and /{}; /{} is the new owner"
                                   .format(self.replaced_node.address(),
                                           self.replacement_node.address(),
                                           self.replacement_node.address()),
                                   timeout=60)

    def _verify_tokens_migrated_successfully(self, previous_log_size=None):
        if DISABLE_VNODES:
            num_tokens = 1
        else:
            # a little hacky but grep_log returns the whole line...
            num_tokens = int(self.replacement_node.get_conf_option('num_tokens'))

        debug("Verifying {} tokens migrated sucessfully".format(num_tokens))
        logs = self.replacement_node.grep_log(r"Token (.*?) changing ownership from /{} to /{}"
                                              .format(self.replaced_node.address(),
                                                      self.replacement_node.address()))
        if (previous_log_size is not None):
            self.assertEquals(len(logs), previous_log_size)

        moved_tokens = set([l[1].group(1) for l in logs])
        debug("number of moved tokens: {}".format(len(moved_tokens)))
        self.assertEquals(len(moved_tokens), num_tokens)

        return len(logs)

    def _test_insert_data_during_replace(self, same_address, mixed_versions=False):
        """
        @jira_ticket CASSANDRA-8523
        """
        default_install_dir = self.cluster.get_install_dir()
        self._setup(opts={'hinted_handoff_enabled': False}, mixed_versions=mixed_versions)

        self._insert_data(n='1k')
        initial_data = self._fetch_initial_data()
        self._stop_node_to_replace()

        if mixed_versions:
            debug("Upgrading all except {} to current version".format(self.query_node.address()))
            self.cluster.set_install_dir(install_dir=default_install_dir)
            for node in self.cluster.nodelist():
                if node.is_running() and node != self.query_node:
                    debug("Upgrading {} to current version".format(node.address()))
                    node.stop(gently=True, wait_other_notice=True)
                    node.start(wait_other_notice=True, wait_for_binary_proto=True)

        # start node in current version on write survey mode
        self._do_replace(same_address=same_address, extra_jvm_args=["-Dcassandra.write_survey=true"])

        # Insert additional keys on query node
        self._insert_data(n='2k', whitelist=True)

        # If not same address or mixed versions, query node should forward writes to replacement node
        # so we update initial data to reflect additional keys inserted during replace
        if not same_address and not mixed_versions:
            initial_data = self._fetch_initial_data(cl=ConsistencyLevel.TWO)

        debug("Joining replaced node")
        self.replacement_node.nodetool("join")

        if not same_address:
            for node in self.cluster.nodelist():
                # if mixed version, query node is not upgraded so it will not print replacement log
                if node.is_running() and (not mixed_versions or node != self.query_node):
                    self._verify_replacement(node, same_address)

        self._verify_data(initial_data)


class TestReplaceAddress(BaseReplaceAddressTest):
    __test__ = True

    @known_failure(failure_source='systemic',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11652',
                   flaky=True,
                   notes='windows')
    @attr('resource-intensive')
    def replace_stopped_node_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._test_replace_node(gently=False)

    @attr('resource-intensive')
    def replace_shutdown_node_test(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._test_replace_node(gently=True)

    @attr('resource-intensive')
    def replace_stopped_node_same_address_test(self):
        """
        @jira_ticket CASSANDRA-8523
        Test that we can replace a node with the same address correctly
        """
        self._test_replace_node(gently=False, same_address=True)

    @attr('resource-intensive')
    def replace_first_boot_test(self):
        self._test_replace_node(jvm_option='replace_address_first_boot')

    def _test_replace_node(self, gently=False, jvm_option='replace_address', same_address=False):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        self._setup(n=3)
        self._insert_data()
        initial_data = self._fetch_initial_data()
        self._stop_node_to_replace(gently=gently)
        self._do_replace(same_address=same_address)

        for node in self.cluster.nodelist():
            if node.is_running() and node != self.replacement_node:
                self._verify_replacement(node, same_address)

        if not same_address:
            previous_log_size = self._verify_tokens_migrated_successfully()

        # restart replacement node
        self.replacement_node.stop(gently=False)
        self.replacement_node.start(wait_for_binary_proto=True, wait_other_notice=False)

        # we redo this check because restarting node should not result
        # in tokens being moved again, ie number should be same
        if not same_address:
            self._verify_tokens_migrated_successfully(previous_log_size)

        self._verify_data(initial_data)

    @attr('resource-intensive')
    def replace_active_node_test(self):
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Exception encountered during startup']
        self._setup(n=3)
        self._do_replace(wait_for_binary_proto=False)

        debug("Waiting for replace to fail")
        self.replacement_node.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...")
        assert_not_running(self.replacement_node)

    @attr('resource-intensive')
    def replace_nonexistent_node_test(self):
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread']
        self._setup(n=3)
        self._do_replace(replace_address='127.0.0.5', wait_for_binary_proto=False)

        debug("Waiting for replace to fail")
        self.replacement_node.watch_log_for("java.lang.RuntimeException: Cannot replace_address /127.0.0.5 because it doesn't exist in gossip")
        assert_not_running(self.replacement_node)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11691',
                   flaky=False,
                   notes='Windows')
    @since('3.6')
    def fail_without_replace_test(self):
        """
        When starting a node from a clean slate with the same address as
        an existing down node, the node should error out even when
        auto_bootstrap = false (or the node is a seed) and tell the user
        to use replace_address.
        @jira_ticket CASSANDRA-10134
        """
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Exception encountered during startup']
        self._setup(n=3)
        self._insert_data()
        node1, node2, node3 = self.cluster.nodelist()

        mark = None
        for auto_bootstrap in (True, False):
            debug("Stopping node 3.")
            node3.stop(gently=False)

            # completely delete the data, commitlog, and saved caches
            for d in chain([os.path.join(node3.get_path(), "commitlogs")],
                           [os.path.join(node3.get_path(), "saved_caches")],
                           node3.data_directories()):
                if os.path.exists(d):
                    rmtree(d)

            node3.set_configuration_options(values={'auto_bootstrap': auto_bootstrap})
            debug("Starting node 3 with auto_bootstrap = {val}".format(val=auto_bootstrap))
            node3.start(wait_other_notice=False)
            node3.watch_log_for('Use cassandra.replace_address if you want to replace this node', from_mark=mark, timeout=20)
            mark = node3.mark_log()

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11700',
                   flaky=True,
                   notes='windows')
    @since('3.6')
    def unsafe_replace_test(self):
        """
        To handle situations such as failed disk in a JBOD, it may be desirable to
        replace a node without bootstrapping. In such scenarios best practice
        advice has been to wipe the node's system keyspace data, set the initial
        tokens via cassandra.yaml, startup without bootstrap and then repair.
        Starting the node as a replacement allows the tokens to be learned from
        gossip, but previously required auto_bootstrap=true. Since CASSANDRA-10134
        replacement is allowed without bootstrapping, but it requires the operator
        to acknowledge the risk in doing so by setting the cassandra.allow_unsafe_replace
        system property at startup.

        @jira_ticket CASSANDRA-10134
        """
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Exception encountered during startup']
        self._setup(n=3)
        self._insert_data()
        initial_data = self._fetch_initial_data()
        self.replacement_node = self.replaced_node

        for set_allow_unsafe_flag in [False, True]:
            debug("Stopping {}".format(self.replaced_node.name))
            self.replaced_node.stop(gently=False)

            # completely delete the system keyspace data plus commitlog and saved caches
            for d in self.replacement_node.data_directories():
                system_data = os.path.join(d, 'system')
                if os.path.exists(system_data):
                    rmtree(system_data)

            for d in ['commitlogs', 'saved_caches']:
                p = os.path.join(self.replacement_node.get_path(), d)
                if os.path.exists(p):
                    rmtree(p)

            self.replacement_node.set_configuration_options(values={'auto_bootstrap': False})
            mark = self.replacement_node.mark_log()

            if set_allow_unsafe_flag:
                debug('Starting replacement node with auto_bootstrap = false and replace_address = {} and allow_unsafe_replace = true'.format(self.replaced_node.address()))
                self._do_replace(extra_jvm_args=['-Dcassandra.allow_unsafe_replace=true'])
                self._verify_data(initial_data)
            else:
                debug('Starting replacement node with auto_bootstrap = false and replace_address = {}'.format(self.replaced_node.address()))
                self._do_replace(wait_for_binary_proto=False)
                self.replacement_node.watch_log_for('To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true',
                                                    from_mark=mark, timeout=20)

    @skipIf(CASSANDRA_VERSION_FROM_BUILD == '3.9', "Test doesn't run on 3.9")
    @since('2.2')
    def insert_data_during_replace_same_address_test(self):
        """
        Test that replacement node with same address DOES NOT receive writes during replacement
        @jira_ticket CASSANDRA-8523
        """
        self._test_insert_data_during_replace(same_address=True)

    @skipIf(CASSANDRA_VERSION_FROM_BUILD == '3.9', "Test doesn't run on 3.9")
    @since('2.2')
    def insert_data_during_replace_different_address_test(self):
        """
        Test that replacement node with different address DOES receive writes during replacement
        @jira_ticket CASSANDRA-8523
        """
        self._test_insert_data_during_replace(same_address=False)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12085',
                   flaky=True,
                   notes='windows')
    @since('2.2')
    @attr('resource-intensive')
    def resume_failed_replace_test(self):
        """
        Test resumable bootstrap while replacing node. Feature introduced in
        2.2 with ticket https://issues.apache.org/jira/browse/CASSANDRA-8838

        @jira_ticket https://issues.apache.org/jira/browse/CASSANDRA-8838
        """
        self._test_restart_failed_replace(mode='resume')

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11835')
    @since('2.2')
    @attr('resource-intensive')
    def restart_failed_replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""
        self._test_restart_failed_replace(mode='reset_resume_state')

    @since('2.2')
    @attr('resource-intensive')
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12656',
                   flaky=True)
    def restart_failed_replace_test(self):
        """
        Test that if a node fails to replace, it can join the cluster even if the data is wiped.
        """
        self._test_restart_failed_replace(mode='wipe')

    def _test_restart_failed_replace(self, mode):
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Error while waiting on bootstrap to complete']
        self._setup(n=3, enable_byteman=True)
        self._insert_data(n="1k")

        initial_data = self._fetch_initial_data()

        self._stop_node_to_replace()

        debug("Submitting byteman script to make stream fail")
        self.query_node.byteman_submit(['./byteman/stream_failure.btm'])

        self._do_replace(jvm_option='replace_address_first_boot',
                         opts={'streaming_socket_timeout_in_ms': 1000})

        # Make sure bootstrap did not complete successfully
        assert_bootstrap_state(self, self.replacement_node, 'IN_PROGRESS')

        if mode == 'reset_resume_state':
            mark = self.replacement_node.mark_log()
            debug("Restarting replacement node with -Dcassandra.reset_bootstrap_progress=true")
            # restart replacement node with resetting bootstrap state
            self.replacement_node.stop()
            self.replacement_node.start(jvm_args=[
                                        "-Dcassandra.replace_address_first_boot={}".format(self.replaced_node.address()),
                                        "-Dcassandra.reset_bootstrap_progress=true"
                                        ],
                                        wait_for_binary_proto=True)
            # check if we reset bootstrap state
            self.replacement_node.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        elif mode == 'resume':
            debug("Resuming failed bootstrap")
            self.replacement_node.nodetool('bootstrap resume')
            # check if we skipped already retrieved ranges
            self.replacement_node.watch_log_for("already available. Skipping streaming.")
            self.replacement_node.watch_log_for("Resume complete")
        elif mode == 'wipe':
            self.replacement_node.stop()

            debug("Waiting other nodes to detect node stopped")
            self.query_node.watch_log_for("FatClient /{} has been silent for 30000ms, removing from gossip".format(self.replacement_node.address()), timeout=60)
            self.query_node.watch_log_for("Node /{} failed during replace.".format(self.replacement_node.address()), timeout=60, filename='debug.log')

            debug("Restarting node after wiping data")
            self._cleanup(self.replacement_node)
            self.replacement_node.start(jvm_args=["-Dcassandra.replace_address_first_boot={}".format(self.replaced_node.address())],
                                        wait_for_binary_proto=True)
        else:
            raise RuntimeError('invalid mode value {mode}'.format(mode))

        # check if bootstrap succeeded
        assert_bootstrap_state(self, self.replacement_node, 'COMPLETED')

        debug("Bootstrap finished successully, verifying data.")

        self._verify_data(initial_data)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12260',
                   flaky=True)
    def replace_with_insufficient_replicas_test(self):
        """
        Test that replace fails when there are insufficient replicas
        @jira_ticket CASSANDRA-11848
        """
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Unable to find sufficient sources for streaming range']
        self._setup(n=3)
        self._insert_data(rf=2)

        self._stop_node_to_replace()

        # stop other replica
        debug("Stopping other replica")
        self.query_node.stop(wait_other_notice=True)

        self._do_replace(wait_for_binary_proto=False, wait_other_notice=False)

        # replace should fail due to insufficient replicas
        self.replacement_node.watch_log_for("Unable to find sufficient sources for streaming range")
        assert_not_running(self.replacement_node)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12276',
                   flaky=False,
                   notes='windows')
    def multi_dc_replace_with_rf1_test(self):
        """
        Test that multi-dc replace works when rf=1 on each dc
        """
        self._setup(n=[1, 1])

        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """
        with tempfile.NamedTemporaryFile(mode='w+') as stress_config:
            stress_config.write(yaml_config)
            stress_config.flush()
            self.query_node.stress(['user', 'profile=' + stress_config.name, 'n=10k', 'no-warmup',
                                    'ops(insert=1)', '-rate', 'threads=50'])

        # Save initial data
        table_name = 'keyspace1.users'
        initial_data = self._fetch_initial_data(table=table_name, cl=ConsistencyLevel.TWO)

        self._stop_node_to_replace(table=table_name)

        self._do_replace(data_center='dc2')

        assert_bootstrap_state(self, self.replacement_node, 'COMPLETED')

        # Check that keyspace was replicated from dc1 to dc2
        self.assertFalse(self.replacement_node.grep_log("Unable to find sufficient sources for streaming range"))

        self._verify_data(initial_data, table=table_name, cl=ConsistencyLevel.LOCAL_ONE)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        for data_dir in node.data_directories():
            debug("Deleting {}".format(data_dir))
            rmtree(data_dir)
        rmtree(commitlog_dir)
