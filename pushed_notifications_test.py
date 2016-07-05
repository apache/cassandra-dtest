import time
from datetime import datetime
from threading import Event

from cassandra import ConsistencyLevel as CL
from cassandra import ReadFailure
from cassandra.query import SimpleStatement
from ccmlib.node import TimeoutError, Node
from nose.tools import timed

from assertions import assert_invalid
from dtest import Tester, debug
from tools import known_failure, no_vnodes, since


class NotificationWaiter(object):
    """
    A helper class for waiting for pushed notifications from
    Cassandra over the native protocol.
    """

    def __init__(self, tester, node, notification_types, keyspace=None):
        """
        `address` should be a ccmlib.node.Node instance
        `notification_types` should be a list of
        "TOPOLOGY_CHANGE", "STATUS_CHANGE", and "SCHEMA_CHANGE".
        """
        self.node = node
        self.address = node.network_interfaces['binary'][0]
        self.notification_types = notification_types
        self.keyspace = keyspace

        # get a single, new connection
        session = tester.patient_exclusive_cql_connection(node)
        connection = session.cluster.connection_factory(self.address, is_control_connection=True)

        # coordinate with an Event
        self.event = Event()

        # the pushed notification
        self.notifications = []

        # register a callback for the notification type
        for notification_type in notification_types:
            connection.register_watcher(notification_type, self.handle_notification, register_timeout=5.0)

    def handle_notification(self, notification):
        """
        Called when a notification is pushed from Cassandra.
        """
        debug("Got {} from {} at {}".format(notification, self.address, datetime.now()))

        if self.keyspace and notification['keyspace'] and self.keyspace != notification['keyspace']:
            return  # we are not interested in this schema change

        self.notifications.append(notification)
        self.event.set()

    def wait_for_notifications(self, timeout, num_notifications=1):
        """
        Waits up to `timeout` seconds for notifications from Cassandra. If
        passed `num_notifications`, stop waiting when that many notifications
        are observed.
        """

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.event.wait(deadline - time.time())
            self.event.clear()
            if len(self.notifications) >= num_notifications:
                break

        return self.notifications

    def clear_notifications(self):
        debug("Clearing notifications...")
        self.notifications = []
        self.event.clear()


class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """
    @no_vnodes()
    def move_single_node_test(self):
        """
        @jira_ticket CASSANDRA-8516
        Moving a token should result in MOVED_NODE notifications.
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in self.cluster.nodes.values()]

        # The first node sends NEW_NODE for the other 2 nodes during startup, in case they are
        # late due to network delays let's block a bit longer
        debug("Waiting for unwanted notifications....")
        waiters[0].wait_for_notifications(timeout=30, num_notifications=2)
        waiters[0].clear_notifications()

        debug("Issuing move command....")
        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(60.0)
            self.assertEquals(1, len(notifications), notifications)
            notification = notifications[0]
            change_type = notification["change_type"]
            address, port = notification["address"]
            self.assertEquals("MOVED_NODE", change_type)
            self.assertEquals(self.get_ip_from_node(node1), address)

    @no_vnodes()
    def move_single_node_localhost_test(self):
        """
        @jira_ticket  CASSANDRA-10052
        Test that we don't get NODE_MOVED notifications from nodes other than the local one,
        when rpc_address is set to localhost (127.0.0.1).

        To set-up this test we override the rpc_address to "localhost (127.0.0.1)" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(3)

        self.change_rpc_address_to_localhost()

        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in self.cluster.nodes.values()]

        # The first node sends NEW_NODE for the other 2 nodes during startup, in case they are
        # late due to network delays let's block a bit longer
        debug("Waiting for unwanted notifications...")
        waiters[0].wait_for_notifications(timeout=30, num_notifications=2)
        waiters[0].clear_notifications()

        debug("Issuing move command....")
        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(30.0)
            self.assertEquals(1 if waiter.node is node1 else 0, len(notifications), notifications)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11360',
                   flaky=True,
                   notes='2 different failures on 2.1 offheap memtables jobs. Fails on main 2.1 as well')
    def restart_node_test(self):
        """
        @jira_ticket CASSANDRA-7816
        Restarting a node should generate exactly one DOWN and one UP notification
        """

        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below
        debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        for i in range(5):
            debug("Restarting second node...")
            node2.stop(wait_other_notice=True)
            node2.start(wait_other_notice=True)
            debug("Waiting for notifications from {}".format(waiter.address))
            notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=2)
            self.assertEquals(2, len(notifications), notifications)
            for notification in notifications:
                self.assertEquals(self.get_ip_from_node(node2), notification["address"][0])
            self.assertEquals("DOWN", notifications[0]["change_type"])
            self.assertEquals("UP", notifications[1]["change_type"])
            waiter.clear_notifications()

    def restart_node_localhost_test(self):
        """
        Test that we don't get client notifications when rpc_address is set to localhost.
        @jira_ticket  CASSANDRA-10052

        To set-up this test we override the rpc_address to "localhost" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        self.change_rpc_address_to_localhost()

        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # restart node 2
        debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start(wait_other_notice=True)

        # check that node1 did not send UP or DOWN notification for node2
        debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=2)
        self.assertEquals(0, len(notifications), notifications)

    @since("2.2")
    def add_and_remove_node_test(self):
        """
        Test that NEW_NODE and REMOVED_NODE are sent correctly as nodes join and leave.
        @jira_ticket CASSANDRA-11038
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below
        debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        debug("Adding second node...")
        node2 = Node('node2', self.cluster, True, ('127.0.0.2', 9160), ('127.0.0.2', 7000), '7200', '0', None, ('127.0.0.2', 9042))
        self.cluster.add(node2, False)
        node2.start(wait_other_notice=True)
        debug("Waiting for notifications from {}".format(waiter.address))
        notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=2)
        self.assertEquals(2, len(notifications), notifications)
        for notification in notifications:
            self.assertEquals(self.get_ip_from_node(node2), notification["address"][0])
            self.assertEquals("NEW_NODE", notifications[0]["change_type"])
            self.assertEquals("UP", notifications[1]["change_type"])

        debug("Removing second node...")
        waiter.clear_notifications()
        node2.decommission()
        node2.stop(gently=False)
        debug("Waiting for notifications from {}".format(waiter.address))
        notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=2)
        self.assertEquals(2, len(notifications), notifications)
        for notification in notifications:
            self.assertEquals(self.get_ip_from_node(node2), notification["address"][0])
            self.assertEquals("REMOVED_NODE", notifications[0]["change_type"])
            self.assertEquals("DOWN", notifications[1]["change_type"])

    def change_rpc_address_to_localhost(self):
        """
        change node's 'rpc_address' from '127.0.0.x' to 'localhost (127.0.0.1)', increase port numbers
        """
        cluster = self.cluster

        i = 0
        for node in cluster.nodelist():
            debug('Set 127.0.0.1 to prevent IPv6 java prefs, set rpc_address: localhost in cassandra.yaml')
            node.network_interfaces['thrift'] = ('127.0.0.1', node.network_interfaces['thrift'][1] + i)
            node.network_interfaces['binary'] = ('127.0.0.1', node.network_interfaces['thrift'][1] + 1)
            node.import_config_files()  # this regenerates the yaml file and sets 'rpc_address' to the 'thrift' address
            node.set_configuration_options(values={'rpc_address': 'localhost'})
            debug(node.show())
            i += 2

    @since("3.0")
    def schema_changes_test(self):
        """
        @jira_ticket CASSANDRA-10328
        Creating, updating and dropping a keyspace, a table and a materialized view
        will generate the correct schema change notifications.
        """

        self.cluster.populate(2).start(wait_for_binary_proto=True)
        node1, node2 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        waiter = NotificationWaiter(self, node2, ["SCHEMA_CHANGE"], keyspace='ks')

        self.create_ks(session, 'ks', 3)
        session.execute("create TABLE t (k int PRIMARY KEY , v int)")
        session.execute("alter TABLE t add v1 int;")

        session.execute("create MATERIALIZED VIEW mv as select * from t WHERE v IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v, k)")
        session.execute(" alter materialized view mv with min_index_interval = 100")

        session.execute("drop MATERIALIZED VIEW mv")
        session.execute("drop TABLE t")
        session.execute("drop KEYSPACE ks")

        debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=8)
        self.assertEquals(8, len(notifications), notifications)
        self.assertDictContainsSubset({'change_type': u'CREATED', 'target_type': u'KEYSPACE'}, notifications[0])
        self.assertDictContainsSubset({'change_type': u'CREATED', 'target_type': u'TABLE', u'table': u't'}, notifications[1])
        self.assertDictContainsSubset({'change_type': u'UPDATED', 'target_type': u'TABLE', u'table': u't'}, notifications[2])
        self.assertDictContainsSubset({'change_type': u'CREATED', 'target_type': u'TABLE', u'table': u'mv'}, notifications[3])
        self.assertDictContainsSubset({'change_type': u'UPDATED', 'target_type': u'TABLE', u'table': u'mv'}, notifications[4])
        self.assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'TABLE', u'table': u'mv'}, notifications[5])
        self.assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'TABLE', u'table': u't'}, notifications[6])
        self.assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'KEYSPACE'}, notifications[7])


class TestVariousNotifications(Tester):
    """
    Tests for various notifications/messages from Cassandra.
    """

    @since('2.2')
    def tombstone_failure_threshold_message_test(self):
        """
        Ensure nodes return an error message in case of TombstoneOverwhelmingExceptions rather
        than dropping the request. A drop makes the coordinator waits for the specified
        read_request_timeout_in_ms.
        @jira_ticket CASSANDRA-7886
        """

        self.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={
                'tombstone_failure_threshold': 500,
                'read_request_timeout_in_ms': 30000,  # 30 seconds
                'range_request_timeout_in_ms': 40000
            }
        )
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'test', 3)
        session.execute(
            "CREATE TABLE test ( "
            "id int, mytext text, col1 int, col2 int, col3 int, "
            "PRIMARY KEY (id, mytext) )"
        )

        # Add data with tombstones
        values = map(lambda i: str(i), range(1000))
        for value in values:
            session.execute(SimpleStatement(
                "insert into test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        failure_msg = ("Scanned over.* tombstones.* query aborted")

        @timed(25)
        def read_failure_query():
            assert_invalid(
                session, SimpleStatement("select * from test where id in (1,2,3,4,5)", consistency_level=CL.ALL),
                expected=ReadFailure
            )

        read_failure_query()

        # In almost all cases, we should find the failure message on node1 within a few seconds.
        # If it is not on node1, we grep all logs, as it *absolutely* should be somewhere.
        # If we still cannot find it then, we fail the test, as this is a problem.
        try:
            node1.watch_log_for(failure_msg, timeout=5)
        except TimeoutError:
            failure = (node1.grep_log(failure_msg) or
                       node2.grep_log(failure_msg) or
                       node3.grep_log(failure_msg))

            self.assertTrue(failure, ("Cannot find tombstone failure threshold error in log "
                                      "after failed query"))

        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()

        @timed(35)
        def range_request_failure_query():
            assert_invalid(
                session, SimpleStatement("select * from test", consistency_level=CL.ALL),
                expected=ReadFailure
            )

        range_request_failure_query()

        # In almost all cases, we should find the failure message on node1 within a few seconds.
        # If it is not on node1, we grep all logs, as it *absolutely* should be somewhere.
        # If we still cannot find it then, we fail the test, as this is a problem.
        try:
            node1.watch_log_for(failure_msg, from_mark=mark1, timeout=5)
        except TimeoutError:
            failure = (node1.grep_log(failure_msg, from_mark=mark1) or
                       node2.grep_log(failure_msg, from_mark=mark2) or
                       node3.grep_log(failure_msg, from_mark=mark3))

            self.assertTrue(failure, ("Cannot find tombstone failure threshold error in log "
                                      "after range_request_timeout_query"))
