import time, os, pprint, glob, re
from threading import Thread

from dtest import debug, Tester
from ccmlib.node import Node

def wait(delay=2):
    """
    An abstraction so that the sleep delays can easily be modified.
    """
    time.sleep(delay)

class TestConcurrentSchemaChanges(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestConcurrentSchemaChanges, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def prepare_for_changes(self, session, namespace='ns1'):
        """
        prepares for schema changes by creating a keyspace and column family.
        """
        debug("prepare_for_changes() " + str(namespace))
        # create a keyspace that will be used
        self.create_ks(session, "ks_%s" % namespace, 2)
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
        session.execute("CREATE INDEX index_%s ON cf_%s(col2)"%(namespace, namespace))

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
        self.create_ks(session, "ks2_%s" % namespace, 2)

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
        debug("make_schema_changes() " + str(namespace))
        session.execute('USE ks_%s' % namespace)
        # drop keyspace
        session.execute('DROP KEYSPACE ks2_%s' % namespace)
        wait(2)

        # create keyspace
        self.create_ks(session, "ks3_%s" % namespace, 2)
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
            ALTER COLUMNFAMILY cf_%s
            ADD col4 text;
        """ % namespace
        session.execute(query)

        # add index
        session.execute("CREATE INDEX index2_%s ON cf_%s(col3)"%(namespace, namespace))

        # remove an index
        session.execute("DROP INDEX index_%s" % namespace)


    def validate_schema_consistent(self, node):
        """ Makes sure that there is only one schema """
        debug("validate_schema_consistent() " + node.name)

        response = node.nodetool('describecluster', True)[0]
        schemas = response.split('Schema versions:')[1].strip()
        num_schemas = len(re.findall('\[.*?\]', schemas))
        assert num_schemas == 1, "There were multiple schema versions: " + pprint.pformat(schemas)


    def basic_test(self):
        """
        make several schema changes on the same node.
        """
        debug("basic_test()")

        cluster = self.cluster
        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        wait(2)
        session = self.cql_connection(node1)

        self.prepare_for_changes(session, namespace='ns1')

        self.make_schema_changes(session, namespace='ns1')


    def changes_to_different_nodes_test(self):
        debug("changes_to_different_nodes_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(2)
        cursor = self.cql_connection(node1)
        self.prepare_for_changes(cursor, namespace='ns1')
        self.make_schema_changes(cursor, namespace='ns1')
        wait(3)
        self.validate_schema_consistent(node1)

        # wait for changes to get to the first node
        wait(20)

        cursor = self.cql_connection(node2)
        self.prepare_for_changes(cursor, namespace='ns2')
        self.make_schema_changes(cursor, namespace='ns2')
        wait(3)
        self.validate_schema_consistent(node1)
        # check both, just because we can
        self.validate_schema_consistent(node2)


    def changes_while_node_down_test(self):
        """
        makes schema changes while a node is down.
        Make schema changes to node 1 while node 2 is down.
        Then bring up 2 and make sure it gets the changes.
        """
        debug("changes_while_node_down_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(2)
        cursor = self.patient_cql_connection(node2)

        self.prepare_for_changes(cursor, namespace='ns2')
        node1.stop()
        wait(2)
        self.make_schema_changes(cursor, namespace='ns2')
        wait(2)
        node2.stop()
        wait(2)
        node1.start()
        node2.start()
        wait(20)
        self.validate_schema_consistent(node1)


    def changes_while_node_toggle_test(self):
        """
        makes schema changes while a node is down.

        Bring down 1 and change 2.
        Bring down 2, bring up 1, and finally bring up 2.
        1 should get the changes.
        """
        debug("changes_while_node_toggle_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(2)
        cursor = self.patient_cql_connection(node2)

        self.prepare_for_changes(cursor, namespace='ns2')
        node1.stop()
        wait(2)
        self.make_schema_changes(cursor, namespace='ns2')
        wait(2)
        node2.stop()
        wait(2)
        node1.start()
        node2.start()
        wait(20)
        self.validate_schema_consistent(node1)


    def decommission_node_test(self):
        debug("decommission_node_test()")
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
                    None)
        cluster.add(node2, False)

        [node1, node2] = cluster.nodelist()
        node1.start()
        node2.start()
        wait(2)

        cursor = self.patient_cql_connection(node1)
        self.prepare_for_changes(cursor)

        node2.decommission()
        wait(30)

        self.validate_schema_consistent(node1)
        self.make_schema_changes(cursor, namespace='ns1')

        # create and add a new node
        node3 = Node('node3',
                    cluster,
                    True,
                    ('127.0.0.3', 9160),
                    ('127.0.0.3', 7000),
                    '7300',
                     '0',
                    None)

        cluster.add(node3, True)
        node3.start()

        wait(30)
        self.validate_schema_consistent(node1)


    def snapshot_test(self):
        debug("snapshot_test()")
        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(2)
        cursor = self.cql_connection(node1)
        self.prepare_for_changes(cursor, namespace='ns2')

        wait(2)
        cluster.flush()

        wait(2)
        node1.nodetool('snapshot -t testsnapshot')
        node2.nodetool('snapshot -t testsnapshot')

        wait(2)
        self.make_schema_changes(cursor, namespace='ns2')

        wait(2)

        cluster.stop()

        ### restore the snapshots ##
        # clear the commitlogs and data
        dirs = (    '%s/commitlogs' % node1.get_path(),
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



    def load_test(self):
        """
        apply schema changes while the cluster is under load.
        """
        debug("load_test()")

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        version = cluster.version()
        wait(2)
        cursor = self.cql_connection(node1)

        def stress(args=[]):
            debug("Stressing")
            node1.stress(args)
            debug("Done Stressing")

        def compact():
            debug("Compacting...")
            node1.nodetool('compact')
            debug("Done Compacting.")

        # put some data into the cluster
        if version < "2.1":
            stress(['--num-keys=30000'])
        else:
            stress(['write', 'n=30000', '-rate', 'threads=8'])

        # now start stressing and compacting at the same time
        tcompact = Thread(target=compact)
        tcompact.start()
        wait(1)

        # now the cluster is under a lot of load. Make some schema changes.
        if version >= "1.2":
            cursor.execute('USE "Keyspace1"')
            wait(1)
            cursor.execute('DROP COLUMNFAMILY "Standard1"')
            wait(3)
            cursor.execute('CREATE COLUMNFAMILY "Standard1" (KEY text PRIMARY KEY)')
        else:
            cursor.execute('USE Keyspace1')
            wait(1)
            cursor.execute('DROP COLUMNFAMILY Standard1')
            wait(3)
            cursor.execute('CREATE COLUMNFAMILY Standard1 (KEY text PRIMARY KEY)')

        tcompact.join()


