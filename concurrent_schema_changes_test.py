import glob
import os
import pprint
import re
import time
import pytest
import logging

from random import randrange
from threading import Thread

from cassandra.concurrent import execute_concurrent
from ccmlib.node import Node

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


def wait(delay=2):
    """
    An abstraction so that the sleep delays can easily be modified.
    """
    time.sleep(delay)


@pytest.mark.skip(reason='awaiting CASSANDRA-10699')
class TestConcurrentSchemaChanges(Tester):
    allow_log_errors = True

    def prepare_for_changes(self, session, namespace='ns1'):
        """
        prepares for schema changes by creating a keyspace and column family.
        """
        logger.debug("prepare_for_changes() " + str(namespace))
        # create a keyspace that will be used
        create_ks(session, "ks_%s" % namespace, 2)
        session.execute('USE ks_%s' % namespace)

        # create a column family with an index and a row of data
        query = """
            CREATE TABLE cf_%s (
                col1 text PRIMARY KEY,
                col2 text,
                col3 text
            );
        """ % namespace
        session.execute(query)
        wait(1)
        session.execute("INSERT INTO cf_%s (col1, col2, col3) VALUES ('a', 'b', 'c');"
                        % namespace)

        # create an index
        session.execute("CREATE INDEX index_%s ON cf_%s(col2)" % (namespace, namespace))

        # create a column family that can be deleted later.
        query = """
            CREATE TABLE cf2_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text
            );
        """ % namespace
        session.execute(query)

        # make a keyspace that can be deleted
        create_ks(session, "ks2_%s" % namespace, 2)

    def make_schema_changes(self, session, namespace='ns1'):
        """
        makes a heap of changes.

        create keyspace
        drop keyspace
        create column family
        drop column family
        update column family
        drop index
        create index (modify column family and add a key)
        rebuild index (via jmx)
        set default_validation_class
        """
        logger.debug("make_schema_changes() " + str(namespace))
        session.execute('USE ks_%s' % namespace)
        # drop keyspace
        session.execute('DROP KEYSPACE ks2_%s' % namespace)
        wait(2)

        # create keyspace
        create_ks(session, "ks3_%s" % namespace, 2)
        session.execute('USE ks_%s' % namespace)

        wait(2)
        # drop column family
        session.execute("DROP COLUMNFAMILY cf2_%s" % namespace)

        # create column family
        query = """
            CREATE TABLE cf3_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text,
                col4 text
            );
        """ % (namespace)
        session.execute(query)

        # alter column family
        query = """
            ALTER COLUMNFAMILY cf_{}
            ADD col4 text;
        """.format(namespace)
        session.execute(query)

        # add index
        session.execute("CREATE INDEX index2_{} ON cf_{}(col3)".format(namespace, namespace))

        # remove an index
        session.execute("DROP INDEX index_{}".format(namespace))

    def validate_schema_consistent(self, node):
        """ Makes sure that there is only one schema """
        logger.debug("validate_schema_consistent() " + node.name)

        response = node.nodetool('describecluster').stdout
        schemas = response.split('Schema versions:')[1].strip()
        num_schemas = len(re.findall(r'\[.*?\]', schemas))
        assert num_schemas, 1 == "There were multiple schema versions: {}".format(pprint.pformat(schemas))

    def test_create_lots_of_tables_concurrently(self):
        """
        create tables across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_tables")
        wait(5)

        cmds = [("create table t_{0} (id uuid primary key, c1 text, c2 text, c3 text, c4 text)".format(n), ()) for n in range(250)]
        results = execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=200)

        for (success, result) in results:
            assert success, "didn't get success on table create: {}".format(result)

        wait(10)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_tables"].tables
        assert 250 == len(table_meta)
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2)
        self.validate_schema_consistent(node3)

    def test_create_lots_of_alters_concurrently(self):
        """
        create alters across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_alters WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_alters")
        for n in range(10):
            session.execute("create table base_{0} (id uuid primary key)".format(n))
        wait(5)

        cmds = [("alter table base_{0} add c_{1} int".format(randrange(0, 10), n), ()) for n in range(500)]

        logger.debug("executing 500 alters")
        results = execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=150)

        for (success, result) in results:
            assert success, "didn't get success on table create: {}".format(result)

        logger.debug("waiting for alters to propagate")
        wait(30)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_alters"].tables
        column_ct = sum([len(table.columns) for table in list(table_meta.values())])

        # primary key + alters
        assert 510 == column_ct
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2)
        self.validate_schema_consistent(node3)

    def test_create_lots_of_indexes_concurrently(self):
        """
        create indexes across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(2).start()

        node1, node2 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_indexes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_indexes")
        for n in range(5):
            session.execute("create table base_{0} (id uuid primary key, c1 int, c2 int)".format(n))
            for ins in range(1000):
                session.execute("insert into base_{0} (id, c1, c2) values (uuid(), {1}, {2})".format(n, ins, ins))
        wait(5)

        logger.debug("creating indexes")
        cmds = []
        for n in range(5):
            cmds.append(("create index ix_base_{0}_c1 on base_{0} (c1)".format(n), ()))
            cmds.append(("create index ix_base_{0}_c2 on base_{0} (c2)".format(n), ()))

        results = execute_concurrent(session, cmds, raise_on_first_error=True)

        for (success, result) in results:
            assert success, "didn't get success on table create: {}".format(result)

        wait(5)

        logger.debug("validating schema and index list")
        session.cluster.control_connection.wait_for_schema_agreement()
        session.cluster.refresh_schema_metadata()
        index_meta = session.cluster.metadata.keyspaces["lots_o_indexes"].indexes
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2)
        assert 10 == len(index_meta)
        for n in range(5):
            assert "ix_base_{0}_c1".format(n) in index_meta
            assert "ix_base_{0}_c2".format(n) in index_meta

        logger.debug("waiting for indexes to fill in")
        wait(45)
        logger.debug("querying all values by secondary index")
        for n in range(5):
            for ins in range(1000):
                assert 1 == len(list(session.execute("select * from base_{0} where c1 = {1}".format(n, ins))))
                assert 1 == len(list(session.execute("select * from base_{0} where c2 = {1}".format(n, ))))

    @since('3.0')
    def test_create_lots_of_mv_concurrently(self):
        """
        create materialized views across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.set_configuration_options({'enable_materialized_views': 'true'})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_views WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_views")
        wait(10)
        session.execute("create table source_data (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);")
        insert_stmt = session.prepare("insert into source_data (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) values (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
        wait(10)
        for n in range(4000):
            session.execute(insert_stmt, [n] * 10)

        wait(10)
        for n in range(1, 11):
            session.execute(("CREATE MATERIALIZED VIEW src_by_c{0} AS SELECT * FROM source_data "
                             "WHERE c{0} IS NOT NULL AND id IS NOT NULL PRIMARY KEY (c{0}, id)".format(n)))
            session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("waiting for indexes to fill in")
        wait(60)
        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='lots_o_views' AND base_table_name='source_data' ALLOW FILTERING")))
        assert 10, len(result) == "missing some mv from source_data table"

        for n in range(1, 11):
            result = list(session.execute("select * from src_by_c{0}".format(n)))
            assert 4000 == len(result)

    def _do_lots_of_schema_actions(self, session):
        for n in range(20):
            session.execute("create table alter_me_{0} (id uuid primary key, s1 int, s2 int, s3 int, s4 int, s5 int, s6 int, s7 int);".format(n))
            session.execute("create table index_me_{0} (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int);".format(n))

        wait(10)
        cmds = []
        for n in range(20):
            cmds.append(("create table new_table_{0} (id uuid primary key, c1 int, c2 int, c3 int, c4 int);".format(n), ()))
            for a in range(1, 8):
                cmds.append(("alter table alter_me_{0} drop s{1};".format(n, a), ()))
                cmds.append(("alter table alter_me_{0} add c{1} int;".format(n, a), ()))
                cmds.append(("create index ix_index_me_{0}_c{1} on index_me_{0} (c{1});".format(n, a), ()))

        results = execute_concurrent(session, cmds, concurrency=100, raise_on_first_error=True)
        for (success, result) in results:
            assert success, "didn't get success: {}".format(result)

    def _verify_lots_of_schema_actions(self, session):
        session.cluster.control_connection.wait_for_schema_agreement()

        # the above should guarentee this -- but to be sure
        node1, node2, node3 = self.cluster.nodelist()
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2)
        self.validate_schema_consistent(node3)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_churn"].tables
        errors = []
        for n in range(20):
            assert "new_table_{0}".format(n) in table_meta

            if 7 != len(table_meta["index_me_{0}".format(n)].indexes):
                errors.append("index_me_{0} expected indexes ix_index_me_c0->7, got: {1}".format(n, sorted(list(table_meta["index_me_{0}".format(n)].indexes))))
            altered = table_meta["alter_me_{0}".format(n)]
            for col in altered.columns:
                if not col.startswith("c") and col != "id":
                    errors.append("alter_me_{0} column[{1}] does not start with c and should have been dropped: {2}".format(n, col, sorted(list(altered.columns))))
            if 8 != len(altered.columns):
                errors.append("alter_me_{0} expected c1 -> c7, id, got: {1}".format(n, sorted(list(altered.columns))))

        assert 0 == len(errors), "\n".join(errors)

    def test_create_lots_of_schema_churn(self):
        """
        create tables, indexes, alters across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_churn WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_churn")

        self._do_lots_of_schema_actions(session)
        logger.debug("waiting for things to settle and sync")
        wait(60)
        self._verify_lots_of_schema_actions(session)

    def test_create_lots_of_schema_churn_with_node_down(self):
        """
        create tables, indexes, alters across multiple threads concurrently with a node down
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_churn WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_churn")

        node2.stop()
        self._do_lots_of_schema_actions(session)
        wait(15)
        node2.start()
        logger.debug("waiting for things to settle and sync")
        wait(120)
        self._verify_lots_of_schema_actions(session)

    def test_basic(self):
        """
        make several schema changes on the same node.
        """
        logger.debug("basic_test()")

        cluster = self.cluster
        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        wait(2)
        session = self.cql_connection(node1)

        self.prepare_for_changes(session, namespace='ns1')

        self.make_schema_changes(session, namespace='ns1')

    def test_changes_to_different_nodes(self):
        logger.debug("changes_to_different_nodes_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.cql_connection(node1)
        self.prepare_for_changes(session, namespace='ns1')
        self.make_schema_changes(session, namespace='ns1')
        wait(3)
        self.validate_schema_consistent(node1)

        # wait for changes to get to the first node
        wait(20)

        session = self.cql_connection(node2)
        self.prepare_for_changes(session, namespace='ns2')
        self.make_schema_changes(session, namespace='ns2')
        wait(3)
        self.validate_schema_consistent(node1)
        # check both, just because we can
        self.validate_schema_consistent(node2)

    def test_changes_while_node_down(self):
        """
        makes schema changes while a node is down.
        Make schema changes to node 1 while node 2 is down.
        Then bring up 2 and make sure it gets the changes.
        """
        logger.debug("changes_while_node_down_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.patient_cql_connection(node2)

        self.prepare_for_changes(session, namespace='ns2')
        node1.stop()
        wait(2)
        self.make_schema_changes(session, namespace='ns2')
        wait(2)
        node2.stop()
        wait(2)
        node1.start()
        node2.start()
        wait(20)
        self.validate_schema_consistent(node1)

    def test_changes_while_node_toggle(self):
        """
        makes schema changes while a node is down.

        Bring down 1 and change 2.
        Bring down 2, bring up 1, and finally bring up 2.
        1 should get the changes.
        """
        logger.debug("changes_while_node_toggle_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.patient_cql_connection(node2)

        self.prepare_for_changes(session, namespace='ns2')
        node1.stop()
        wait(2)
        self.make_schema_changes(session, namespace='ns2')
        wait(2)
        node2.stop()
        wait(2)
        node1.start()
        node2.start()
        wait(20)
        self.validate_schema_consistent(node1)

    def test_decommission_node(self):
        logger.debug("decommission_node_test()")
        cluster = self.cluster

        cluster.populate(1)
        # create and add a new node, I must not be a seed, otherwise
        # we get schema disagreement issues for awhile after decommissioning it.
        node2 = Node('node2',
                     cluster,
                     True,
                     ('127.0.0.2', 9160),
                     ('127.0.0.2', 7000),
                     '7200',
                     '0',
                     None,
                     binary_interface=('127.0.0.2', 9042))
        cluster.add(node2, False)

        node1, node2 = cluster.nodelist()
        node1.start(wait_for_binary_proto=True)
        node2.start(wait_for_binary_proto=True)
        wait(2)

        session = self.patient_cql_connection(node1)
        self.prepare_for_changes(session)

        node2.decommission()
        wait(30)

        self.validate_schema_consistent(node1)
        self.make_schema_changes(session, namespace='ns1')

        # create and add a new node
        node3 = Node('node3',
                     cluster,
                     True,
                     ('127.0.0.3', 9160),
                     ('127.0.0.3', 7000),
                     '7300',
                     '0',
                     None,
                     binary_interface=('127.0.0.3', 9042))

        cluster.add(node3, True)
        node3.start(wait_for_binary_proto=True)

        wait(30)
        self.validate_schema_consistent(node1)

    def test_snapshot(self):
        logger.debug("snapshot_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.cql_connection(node1)
        self.prepare_for_changes(session, namespace='ns2')

        wait(2)
        cluster.flush()

        wait(2)
        node1.nodetool('snapshot -t testsnapshot')
        node2.nodetool('snapshot -t testsnapshot')

        wait(2)
        self.make_schema_changes(session, namespace='ns2')

        wait(2)

        cluster.stop()

        # restore the snapshots
        # clear the commitlogs and data
        dirs = ('%s/commitlogs' % node1.get_path(),
                '%s/commitlogs' % node2.get_path(),
                '%s/data/ks_ns2/cf_*/*' % node1.get_path(),
                '%s/data/ks_ns2/cf_*/*' % node2.get_path(),
                )
        for dirr in dirs:
            for f in glob.glob(os.path.join(dirr)):
                if os.path.isfile(f):
                    os.unlink(f)

        # copy the snapshot. TODO: This could be replaced with the creation of hard links.
        os.system('cp -p %s/data/ks_ns2/cf_*/snapshots/testsnapshot/* %s/data/ks_ns2/cf_*/' % (node1.get_path(), node1.get_path()))
        os.system('cp -p %s/data/ks_ns2/cf_*/snapshots/testsnapshot/* %s/data/ks_ns2/cf_*/' % (node2.get_path(), node2.get_path()))

        # restart the cluster
        cluster.start()

        wait(2)
        self.validate_schema_consistent(node1)

    def test_load(self):
        """
        apply schema changes while the cluster is under load.
        """
        logger.debug("load_test()")

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        wait(2)
        session = self.cql_connection(node1)

        def stress(args=[]):
            logger.debug("Stressing")
            node1.stress(args)
            logger.debug("Done Stressing")

        def compact():
            logger.debug("Compacting...")
            node1.nodetool('compact')
            logger.debug("Done Compacting.")

        # put some data into the cluster
        stress(['write', 'n=30000', 'no-warmup', '-rate', 'threads=8'])

        # now start stressing and compacting at the same time
        tcompact = Thread(target=compact)
        tcompact.start()
        wait(1)

        # now the cluster is under a lot of load. Make some schema changes.
        session.execute('USE keyspace1')
        wait(1)
        session.execute('DROP TABLE standard1')
        wait(3)
        session.execute('CREATE TABLE standard1 (KEY text PRIMARY KEY)')

        tcompact.join()
