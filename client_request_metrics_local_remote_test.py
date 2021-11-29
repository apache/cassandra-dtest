from dtest import Tester, create_ks
from tools.jmxutils import JolokiaAgent, make_mbean
import pytest

since = pytest.mark.since


@since('4.1')
class TestClientRequestMetricsLocalRemote(Tester):

    def test_write_and_read(self):
        session, node = setup(self)

        read_metrics = ClientRequestMetricsContainer(node, 'Read')
        write_metrics = ClientRequestMetricsContainer(node, 'Write')

        # Get initial results:
        r1_r = ClientRequestMetricsSnapshot(read_metrics)
        r1_w = ClientRequestMetricsSnapshot(write_metrics)

        # Run Write test:
        for i in murmur3_hashes.keys():
            session.execute(
                "INSERT INTO ks.test (id, ord, val) VALUES ({}, 1, 'aaaa');".format(i)
            )

        # Collect results:
        r2_r = ClientRequestMetricsSnapshot(read_metrics)
        r2_w = ClientRequestMetricsSnapshot(write_metrics)

        # Run Read test:
        for i in murmur3_hashes.keys():
            session.execute(
                "SELECT (id, ord, val) FROM ks.test WHERE id={};".format(i)
            )

        # Collect results:
        r3_r = ClientRequestMetricsSnapshot(read_metrics)
        r3_w = ClientRequestMetricsSnapshot(write_metrics)

        assert 0 <= (r2_w.remote_requests - r1_w.remote_requests)
        assert 0 <= (r2_r.local_requests - r1_r.local_requests)
        assert 0 == (r2_r.remote_requests - r1_r.remote_requests)

        assert 0 == (r3_w.local_requests - r2_w.local_requests)
        assert 0 == (r3_w.remote_requests - r2_w.remote_requests)
        assert 0 < (r3_r.remote_requests - r2_r.remote_requests)

    def test_batch_and_slice(self):
        session, node = setup(self)

        read_metrics = ClientRequestMetricsContainer(node, 'Read')
        write_metrics = ClientRequestMetricsContainer(node, 'Write')

        # Get initial results:
        r1_r = ClientRequestMetricsSnapshot(read_metrics)
        r1_w = ClientRequestMetricsSnapshot(write_metrics)

        # Run batch test:
        query = 'BEGIN BATCH '
        for i in murmur3_hashes.keys():
            for y in range(0, 50):
                query += "INSERT INTO ks.test (id, ord, val) VALUES ({}, {}, 'aaa')".format(i, y)
        query += 'APPLY BATCH;'
        session.execute(query)

        # Collect results:
        r2_r = ClientRequestMetricsSnapshot(read_metrics)
        r2_w = ClientRequestMetricsSnapshot(write_metrics)

        # Run read range test:
        for i in murmur3_hashes.keys():
            session.execute("""
                            SELECT (id, ord, val) FROM ks.test
                            WHERE id={}
                            AND ord > 0
                            AND ord < 100;
                            """.format(i))
        # Collect results:
        r3_r = ClientRequestMetricsSnapshot(read_metrics)
        r3_w = ClientRequestMetricsSnapshot(write_metrics)

        assert 0 <= (r2_w.remote_requests - r1_w.remote_requests)
        assert 0 <= (r2_r.local_requests - r1_r.local_requests)
        assert 0 == (r2_r.remote_requests - r1_r.remote_requests)

        assert 0 == (r3_w.local_requests - r2_w.local_requests)
        assert 0 == (r3_w.remote_requests - r2_w.remote_requests)
        assert 0 < (r3_r.remote_requests - r2_r.remote_requests)

    def test_paxos(self):
        session, node = setup(self)

        read_metrics = ClientRequestMetricsContainer(node, 'Read')
        write_metrics = ClientRequestMetricsContainer(node, 'Write')

        # Get initial results:
        r1_r = ClientRequestMetricsSnapshot(read_metrics)
        r1_w = ClientRequestMetricsSnapshot(write_metrics)

        # Run write test:
        for i in murmur3_hashes.keys():
            session.execute(
                "UPDATE ks.test SET val='aaa' WHERE id={} AND ord=0".format(i)
            )

        # Collect results:
        r2_r = ClientRequestMetricsSnapshot(read_metrics)
        r2_w = ClientRequestMetricsSnapshot(write_metrics)

        assert 0 <= (r2_w.remote_requests - r1_w.remote_requests)
        assert 0 <= (r2_r.local_requests - r1_r.local_requests)
        assert 0 == (r2_r.remote_requests - r1_r.remote_requests)


murmur3_hashes = {
    5: -7509452495886106294,
    10: -6715243485458697746,
    16: -5477287129830487822,
    13: -5034495173465742853,
    11: -4156302194539278891,
    1: -4069959284402364209,
    19: -3974532302236993209,
    8: -3799847372828181882,
    2: -3248873570005575792,
    4: -2729420104000364805,
    18: -2695747960476065067,
    15: -1191135763843456182,
    20: 1388667306199997068,
    7: 1634052884888577606,
    6: 2705480034054113608,
    9: 3728482343045213994,
    14: 4279681877540623768,
    17: 5467144456125416399,
    12: 8582886034424406875,
    3: 9010454139840013625
}


class ClientRequestMetricsContainer:

    def __init__(self, node, scope):
        self.node = node

        self.local_requests_mbean = make_mbean(
            'metrics',
            type='ClientRequest',
            scope=scope,
            name='LocalRequests'
        )

        self.remote_requests_mbean = make_mbean(
            'metrics',
            type='ClientRequest',
            scope=scope,
            name='RemoteRequests'
        )

    def get_local_requests(self):
        with JolokiaAgent(self.node) as jmx:
            return jmx.read_attribute(self.local_requests_mbean, 'Count')

    def get_remote_requests(self):
        with JolokiaAgent(self.node) as jmx:
            return jmx.read_attribute(self.remote_requests_mbean, 'Count')


class ClientRequestMetricsSnapshot:

    def __init__(self, client_request_metrics):
        self.local_requests = client_request_metrics.get_local_requests()
        self.remote_requests = client_request_metrics.get_remote_requests()


def setup_schema(session):
    create_ks(session, 'ks', 1)
    session.execute("CREATE TABLE test (id int,ord int,val varchar,PRIMARY KEY (id, ord));""")


def setup(obj):
    cluster = obj.cluster
    cluster.populate(2)

    node = cluster.nodelist()[0]
    node2 = cluster.nodelist()[1]

    cluster.start()

    session = obj.patient_exclusive_cql_connection(node)
    obj.patient_exclusive_cql_connection(node2)

    setup_schema(session)

    return session, node
