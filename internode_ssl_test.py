from dtest import Tester, debug
from tools import generate_ssl_stores, putget


class TestInternodeSSL(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def putget_with_internode_ssl_test(self):
        """
        Simple putget test with internode ssl enabled
        @jira_ticket CASSANDRA-9884
        """
        cluster = self.cluster

        debug("***using internode ssl***")
        generate_ssl_stores(self.test_path)
        self.cluster.enable_internode_ssl(self.test_path)

        cluster.populate(3).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', compression=None)
        putget(cluster, session)
