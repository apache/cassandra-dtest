import time
import pytest
import logging

from datetime import datetime
from distutils.version import LooseVersion
from threading import Event

from cassandra import ConsistencyLevel as CL
from cassandra import ReadFailure
from cassandra.query import SimpleStatement
from ccmlib.node import Node, TimeoutError

from dtest import Tester, get_ip_from_node, get_port_from_node, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


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
        version = 5 if node.get_cassandra_version() >= LooseVersion('4.0') else None
        session = tester.patient_exclusive_cql_connection(node, protocol_version=version)
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
        logger.debug("Got {} from {} at {}".format(notification, self.address, datetime.now()))
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
        logger.debug("Clearing notifications...")
        self.notifications = []
        self.event.clear()


class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    @pytest.mark.no_vnodes
    def test_move_single_node(self):
        """
        @jira_ticket CASSANDRA-8516
        Moving a token should result in MOVED_NODE notifications.
        """
        self.cluster.populate(3).start()

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in list(self.cluster.nodes.values())]

        # The first node sends NEW_NODE for the other 2 nodes during startup, in case they are
        # late due to network delays let's block a bit longer
        logger.debug("Waiting for unwanted notifications....")
        waiters[0].wait_for_notifications(timeout=30, num_notifications=2)
        waiters[0].clear_notifications()

        logger.debug("Issuing move command....")
        node1 = list(self.cluster.nodes.values())[0]
        node1.move("123")

        for waiter in waiters:
            logger.debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(60.0)
            assert 1 == len(notifications), notifications
            notification = notifications[0]
            change_type = notification["change_type"]
            address, port = notification["address"]
            assert "MOVED_NODE" == change_type
            assert get_ip_from_node(node1) == address

    @pytest.mark.no_vnodes
    def test_move_single_node_localhost(self):
        """
        Test that we don't get NODE_MOVED notifications from nodes other than the local one,
        when rpc_address is set to localhost (127.0.0.1) Pre 4.0.
        Test that we get NODE_MOVED notifications from nodes other than the local one,
        when rpc_address is set to localhost (127.0.0.1) Post 4.0.
        @jira_ticket  CASSANDRA-10052
        @jira_ticket  CASSANDRA-15677

        To set-up this test we override the rpc_address to "localhost (127.0.0.1)" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(3)

        self.change_rpc_address_to_localhost()

        cluster.start()

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in list(self.cluster.nodes.values())]

        # The first node sends NEW_NODE for the other 2 nodes during startup, in case they are
        # late due to network delays let's block a bit longer
        logger.debug("Waiting for unwanted notifications...")
        waiters[0].wait_for_notifications(timeout=30, num_notifications=2)
        waiters[0].clear_notifications()

        logger.debug("Issuing move command....")
        node1 = list(self.cluster.nodes.values())[0]
        node1.move("123")

        version = self.cluster.cassandra_version()
        for waiter in waiters:
            logger.debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(30.0)
            if version >= '4.0':
                # CASSANDRA-15677 Post 4.0 we'll get the notifications. Check that they are for the right node.
                assert 1 == len(notifications), notifications
                notification = notifications[0]
                change_type = notification["change_type"]
                address, port = notification["address"]
                assert "MOVED_NODE" == change_type
                assert get_ip_from_node(node1) == address
                assert get_port_from_node(node1) == port
            else:
                assert 1 if waiter.node is node1 else 0 == len(notifications), notifications


    def test_restart_node(self):
        """
        @jira_ticket CASSANDRA-7816
        Restarting a node should generate exactly one DOWN and one UP notification
        """
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below.
        logger.debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        # On versions prior to 2.2, an additional NEW_NODE notification is sent when a node
        # is restarted. This bug was fixed in CASSANDRA-11038 (see also CASSANDRA-11360)
        version = self.cluster.cassandra_version()
        expected_notifications = 2 if version >= '2.2' else 3
        for i in range(5):
            logger.debug("Restarting second node...")
            node2.stop(wait_other_notice=True)
            node2.start()
            logger.debug("Waiting for notifications from {}".format(waiter.address))
            notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=expected_notifications)
            assert expected_notifications, len(notifications) == notifications
            for notification in notifications:
                assert get_ip_from_node(node2) == notification["address"][0]
            assert "DOWN" == notifications[0]["change_type"]
            if version >= '2.2':
                assert "UP" == notifications[1]["change_type"]
            else:
                # pre 2.2, we'll receive both a NEW_NODE and an UP notification,
                # but the order is not guaranteed
                assert {"NEW_NODE", "UP"} == set([n["change_type"] for n in notifications[1:]])

            waiter.clear_notifications()

    def test_restart_node_localhost(self):
        """
        Test that we don't get client notifications when rpc_address is set to localhost Pre 4.0.
        Test that we get correct client notifications when rpc_address is set to localhost Post 4.0.
        @jira_ticket  CASSANDRA-10052
        @jira_ticket  CASSANDRA-15677

        To set-up this test we override the rpc_address to "localhost" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        self.change_rpc_address_to_localhost()

        cluster.start()

        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # restart node 2
        version = self.cluster.cassandra_version()
        if version >= '4.0':
            # >=4.0 we wait for the NEW_NODE and UP notifications to reach us
            waiter.wait_for_notifications(timeout=30.0, num_notifications=2)
            waiter.clear_notifications()

        logger.debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start()

        # check that node1 did not send UP or DOWN notification for node2
        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=2)

        if version >= '4.0':
            # CASSANDRA-15677 Post 4.0 we'll get the notifications. Check that they are for the right node.
            for notification in notifications:
                address, port = notification["address"]
                assert get_ip_from_node(node2) == address
                assert get_port_from_node(node2) == port
            assert "DOWN" == notifications[0]["change_type"], notifications
            assert "UP" == notifications[1]["change_type"], notifications
        else:
            assert 0 == len(notifications), notifications

    @since("2.2")
    def test_add_and_remove_node(self):
        """
        Test that NEW_NODE and REMOVED_NODE are sent correctly as nodes join and leave.
        @jira_ticket CASSANDRA-11038
        """
        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])

        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below
        logger.debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        session = self.patient_cql_connection(node1)
        # reduce system_distributed RF to 2 so we don't require forceful decommission
        session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")
        session.execute("ALTER KEYSPACE system_traces WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        logger.debug("Adding second node...")
        node2 = Node('node2', self.cluster, True, None, ('127.0.0.2', 7000), '7200', '0', None, binary_interface=('127.0.0.2', 9042))
        self.cluster.add(node2, False, data_center="dc1")
        node2.start()
        logger.debug("Waiting for notifications from {}".format(waiter.address))
        notifications = waiter.wait_for_notifications(timeout=120.0, num_notifications=2)
        assert 2 == len(notifications), notifications
        for notification in notifications:
            assert get_ip_from_node(node2) == notification["address"][0]
            assert "NEW_NODE" == notifications[0]["change_type"]
            assert "UP" == notifications[1]["change_type"]

        logger.debug("Removing second node...")
        waiter.clear_notifications()
        node2.decommission()
        node2.stop(gently=False)
        logger.debug("Waiting for notifications from {}".format(waiter.address))
        notifications = waiter.wait_for_notifications(timeout=120.0, num_notifications=2)
        assert 2 == len(notifications), notifications
        for notification in notifications:
            assert get_ip_from_node(node2) == notification["address"][0]
            assert "REMOVED_NODE" == notifications[0]["change_type"]
            assert "DOWN" == notifications[1]["change_type"]

    def change_rpc_address_to_localhost(self):
        """
        change node's 'rpc_address' from '127.0.0.x' to 'localhost (127.0.0.1)', increase port numbers
        """
        cluster = self.cluster

        i = 0
        for node in cluster.nodelist():
            logger.debug('Set 127.0.0.1 to prevent IPv6 java prefs, set rpc_address: localhost in cassandra.yaml')
            if cluster.version() < '4':
                node.network_interfaces['thrift'] = ('127.0.0.1', node.network_interfaces['thrift'][1] + i)
            node.network_interfaces['binary'] = ('127.0.0.1', node.network_interfaces['binary'][1] + i)
            node.import_config_files()  # this regenerates the yaml file and sets 'rpc_address' to the 'thrift' address
            node.set_configuration_options(values={'rpc_address': 'localhost'})
            logger.debug(node.show())
            i += 2

    @since("3.0")
    def test_schema_changes(self):
        """
        @jira_ticket CASSANDRA-10328
        Creating, updating and dropping a keyspace, a table and a materialized view
        will generate the correct schema change notifications.
        """
        self.cluster.set_configuration_options({'enable_materialized_views': 'true'})
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        waiter = NotificationWaiter(self, node2, ["SCHEMA_CHANGE"], keyspace='ks')

        create_ks(session, 'ks', 3)
        session.execute("create TABLE t (k int PRIMARY KEY , v int)")
        session.execute("alter TABLE t add v1 int;")

        session.execute("create MATERIALIZED VIEW mv as select * from t WHERE v IS NOT NULL AND k IS NOT NULL PRIMARY KEY (v, k)")
        session.execute(" alter materialized view mv with min_index_interval = 100")

        session.execute("drop MATERIALIZED VIEW mv")
        session.execute("drop TABLE t")
        session.execute("drop KEYSPACE ks")

        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=8)
        assert 8 == len(notifications), notifications
        # assert dict contains subset
        expected = {'change_type': 'CREATED', 'target_type': 'KEYSPACE'}
        assert set(notifications[0].keys()) >= expected.keys() and {k: notifications[0][k] for k in expected if
                                                                    k in notifications[0]} == expected
        expected = {'change_type': 'CREATED', 'target_type': 'TABLE', 'table': 't'}
        assert set(notifications[1].keys()) >= expected.keys() and {k: notifications[1][k] for k in expected if
                                                                    k in notifications[1]} == expected
        expected = {'change_type': 'UPDATED', 'target_type': 'TABLE', 'table': 't'}
        assert set(notifications[2].keys()) >= expected.keys() and {k: notifications[2][k] for k in expected if
                                                                    k in notifications[2]} == expected
        expected = {'change_type': 'CREATED', 'target_type': 'TABLE', 'table': 'mv'}
        assert set(notifications[3].keys()) >= expected.keys() and {k: notifications[3][k] for k in expected if
                                                                    k in notifications[3]} == expected
        expected = {'change_type': 'UPDATED', 'target_type': 'TABLE', 'table': 'mv'}
        assert set(notifications[4].keys()) >= expected.keys() and {k: notifications[4][k] for k in expected if
                                                                    k in notifications[4]} == expected
        expected = {'change_type': 'DROPPED', 'target_type': 'TABLE', 'table': 'mv'}
        assert set(notifications[5].keys()) >= expected.keys() and {k: notifications[5][k] for k in expected if
                                                                    k in notifications[5]} == expected
        expected = {'change_type': 'DROPPED', 'target_type': 'TABLE', 'table': 't'}
        assert set(notifications[6].keys()) >= expected.keys() and {k: notifications[6][k] for k in expected if
                                                                    k in notifications[6]} == expected
        expected = {'change_type': 'DROPPED', 'target_type': 'KEYSPACE'}
        assert set(notifications[7].keys()) >= expected.keys() and {k: notifications[7][k] for k in expected if
                                                                    k in notifications[7]} == expected


class TestVariousNotifications(Tester):
    """
    Tests for various notifications/messages from Cassandra.
    """

    @since('2.2')
    def test_tombstone_failure_threshold_message(self):
        """
        Ensure nodes return an error message in case of TombstoneOverwhelmingExceptions rather
        than dropping the request. A drop makes the coordinator waits for the specified
        read_request_timeout_in_ms.
        @jira_ticket CASSANDRA-7886
        """
        have_v5_protocol = self.supports_v5_protocol(self.cluster.version())

        self.fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={
                'tombstone_failure_threshold': 500,
                'read_request_timeout_in_ms': 30000,  # 30 seconds
                'range_request_timeout_in_ms': 40000
            }
        )
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        proto_version = 5 if have_v5_protocol else None
        session = self.patient_cql_connection(node1, protocol_version=proto_version)

        create_ks(session, 'test', 3)
        session.execute(
            "CREATE TABLE test ( "
            "id int, mytext text, col1 int, col2 int, col3 int, "
            "PRIMARY KEY (id, mytext) )"
        )

        # Add data with tombstones
        values = [str(i) for i in range(1000)]
        for value in values:
            session.execute(SimpleStatement(
                "insert into test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        failure_msg = ("Scanned over.* tombstones.* query aborted")

        @pytest.mark.timeout(25)
        def read_failure_query():
            try:
                session.execute(SimpleStatement("select * from test where id in (1,2,3,4,5)", consistency_level=CL.ALL))
            except ReadFailure as exc:
                if have_v5_protocol:
                    # at least one replica should have responded with a tombstone error
                    assert exc.error_code_map is not None
                    assert 0x0001 == list(exc.error_code_map.values())[0]
            except Exception:
                raise
            else:
                pytest.fail('Expected ReadFailure')

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

            assert failure, "Cannot find tombstone failure threshold error in log after failed query"

        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()

        @pytest.mark.timeout(35)
        def range_request_failure_query():
            try:
                session.execute(SimpleStatement("select * from test", consistency_level=CL.ALL))
            except ReadFailure as exc:
                if have_v5_protocol:
                    # at least one replica should have responded with a tombstone error
                    assert exc.error_code_map is not None
                    assert 0x0001 == list(exc.error_code_map.values())[0]
            except Exception:
                raise
            else:
                pytest.fail('Expected ReadFailure')

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

            assert failure == "Cannot find tombstone failure threshold error in log after range_request_timeout_query"
