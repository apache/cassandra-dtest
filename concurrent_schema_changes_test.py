from dtest import Tester
from ccmlib.cluster import Cluster
from ccmlib.node import Node
import random
import time
import os
import sys
from threading import Thread

def wait(delay=2):
    """
    An abstraction so that the sleep delays can easily be modified.
    """
    time.sleep(delay)

class TestUpgrade(Tester):

    def prepare_for_changes(self, cursor, namespace='ns1', version='v1'):
        """
        prepares for schema changes by creating a keyspace and column family.
        """
        # create a keyspace that will be used
        query = """CREATE KEYSPACE ks_%s WITH strategy_class=SimpleStrategy AND 
                strategy_options:replication_factor=2""" % (namespace)
        cursor.execute(query)
        cursor.execute('USE ks_%s' % namespace)

        # make a keyspace that can be deleted
        query = """CREATE KEYSPACE ks2_%s WITH strategy_class=SimpleStrategy AND 
                strategy_options:replication_factor=2""" % (namespace)
        cursor.execute(query)

        # create a column family with an index and a row of data
        query = """
            CREATE TABLE cf_%s (
                col1 text PRIMARY KEY,
                col2 text,
                col3 text
            );
        """ % namespace
        cursor.execute(query)
        wait(1)
        cursor.execute("INSERT INTO cf_%s (col1, col2, col3) VALUES ('a', 'b', 'c');" 
                % namespace)

        # create an index
        cursor.execute("CREATE INDEX index_%s ON cf_%s(col2)"%(namespace, namespace))

        # create a column family that can be deleted later.
        query = """
            CREATE TABLE cf2_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text
            );
        """ % namespace
        cursor.execute(query)

        wait(3)


    def make_schema_changes(self, cursor, namespace='ns1', version='v1'):
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
        cursor.execute('USE ks_%s' % namespace)
        # drop keyspace
        cursor.execute('DROP KEYSPACE ks2_%s' % namespace)
        wait(2)

        # create keyspace
        query = """CREATE KEYSPACE ks3_%s WITH strategy_class=SimpleStrategy AND
                strategy_options:replication_factor=2""" % namespace
        cursor.execute(query)

        wait(2)
        # drop column family
        cursor.execute("DROP COLUMNFAMILY cf2_%s" % namespace)

        wait(2)

        # create column family
        query = """
            CREATE TABLE cf3_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text,
                %s text
            );
        """ % (namespace, version)
        cursor.execute(query)

        # alter column family
        query = """
            ALTER COLUMNFAMILY cf_%s
            ADD col4 text;
        """ % namespace
        cursor.execute(query)

        # add index
        cursor.execute("CREATE INDEX index2_%s ON cf_%s(col3)"%(namespace, namespace))

        # remove an index
        cursor.execute("DROP INDEX index_%s" % namespace)



    def validate_schema_consistent(self, keyspace_name, nodes):
        """
        does a "DESCRIBE KEYSPACE" on each node and makes sure that the 
        keyspaces are consistent.

        keyspace_name is a string
        nodes is a list of ccm nodes
        """

        last_result = None
        last_node = None
        for node in nodes:
            cli = node.cli()
            wait(3) # not sleeping till the cli is running causes wierd errors
            cli.do("DESCRIBE %s" % keyspace_name)
            result = cli.last_output()
            if cli.has_errors():
                raise Exception("error when describing the keyspace: %s" % cli.last_error())
            if last_result:
                # the keyspaces can be the same and still have things listed 
                # in a different order. By sorting the lines, we throw out the
                # order. This could potentially allow uncaught problems, like
                # a column being listed on the wrong column family. 
                if sorted(result.splitlines()) != sorted(last_result.splitlines()):
                    raise Exception("Schemas don't match between nodes %s and %s!"
                            "\nschema1: %s\nschema2: %s" % (last_node.name, node.name,
                            last_result, result))
            last_result = result
            last_node = node

    
    def decommission_node_tes(self):
        cluster = self.cluster

        cluster.set_cassandra_dir(git_branch='cassandra-1.1')

        cluster.populate(2).start()

        [node1, node2] = cluster.nodelist()

        wait(2)
        cursor = self.cql_connection(node1).cursor()
        self.prepare_for_changes(cursor)
        wait(2)

        node2.decommission()

        self.make_schema_changes(cursor, namespace='ns1')
        wait(2)

        # create and add a new node
        node3 = Node('node3', 
                    cluster,
                    True,
                    ('127.0.0.3', 9160),
                    ('127.0.0.3', 7000),
                    '7300',
                    None)

        cluster.add(node3, True)

        node3.start()

        wait(2)
        self.validate_schema_consistent('ks_ns1', [node1, node3])

        cluster.cleanup()


    def snapshot_tes(self):
        cluster = self.cluster
        cluster.set_cassandra_dir(git_branch='cassandra-1.1')
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(2)
        cursor = self.cql_connection(node1).cursor()
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
        # clear the commitlogs
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
        os.system('rm %s/commitlogs/*' % node1.get_path())
        os.system('rm %s/commitlogs/*' % node2.get_path())

        # clear the data. Note that this will leave the subdirectories.
        os.system('rm %s/data/ks_ns2/cf_ns2/*' % node1.get_path())
        os.system('rm %s/data/ks_ns2/cf_ns2/*' % node2.get_path())
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        # copy the snapshot
        os.system('cp -p %s/data/ks_ns2/cf_ns2/snapshots/testsnapshot/* %s/data/ks_ns2/cf_ns2/' % (node1.get_path(), node1.get_path()))
        os.system('cp -p %s/data/ks_ns2/cf_ns2/snapshots/testsnapshot/* %s/data/ks_ns2/cf_ns2/' % (node2.get_path(), node2.get_path()))

        # restart the cluster
        cluster.start()

        wait(2)
        self.validate_schema_consistent('ks_ns2', [node1, node2])

        cluster.cleanup()


    def load_test(self):
        """
        applies schema changes while the cluster is under load.
        """


        cluster = self.cluster
        cluster.set_cassandra_dir(git_branch='cassandra-1.1')
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        wait(2)
        cursor = self.cql_connection(node1).cursor()

#        self.prepare_for_changes(cursor)
#        wait(2)

        def stress(args=[]):
            print "Stressing"
            node1.stress(args)
            print "Done Stressing"

        def compact():
            print "Compacting..."
            node1.nodetool('compact')
            print "Done Compacting."

        # put some data into the cluster
        stress(['--num-keys=1000000'])

        # now start stressing and compacting at the same time
        tcompact = Thread(target=compact)
        tcompact.start()
        wait(1)

        # now the cluster is under a lot of load. Make some schema changes.
        cursor.execute("USE Keyspace1")
        wait(1)
        cursor.execute("DROP COLUMNFAMILY Standard1")

        wait(3)

        cursor.execute("CREATE COLUMNFAMILY Standard1 (KEY text PRIMARY KEY)")

        tcompact.join()



