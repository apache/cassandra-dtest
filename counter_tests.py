from dtest import Tester
from datatool import create_rows

import random, time, uuid

class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        cursor = self.patient_cql_connection(nodes[0]).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', validation="CounterColumnType", columns={'c': 'counter'})
        cursor.close()


        cursors = [ self.patient_cql_connection(node, 'ks').cursor() for node in nodes ]
        nb_increment=50
        nb_counter=10

        for i in xrange(0, nb_increment):
            for c in xrange(0, nb_counter):
                cursor = cursors[(i + c) % len(nodes)]
                if cluster.version() >= '1.2':
                    cursor.execute("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level='QUORUM')
                else:
                    cursor.execute("UPDATE cf USING CONSISTENCY QUORUM SET c = c + 1 WHERE key = 'counter%i'" % c)

            cursor = cursors[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
            if cluster.version() >= '1.2':
                cursor.execute("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level='QUORUM')
            else:
                cursor.execute("SELECT key, c FROM cf USING CONSISTENCY QUORUM WHERE key IN (%s)" % keys)
            res = cursor.fetchall()
            assert len(res) == nb_counter
            for c in xrange(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][0])

    def upgrade_test(self):
        """ Test for bug of #4436 """

        cluster = self.cluster

        cluster.populate(2).start()
        nodes = cluster.nodelist()

        cql_version=None

        cursor = self.patient_cql_connection(nodes[0], version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 2)

        query = """
            CREATE TABLE counterTable (
                k int PRIMARY KEY,
                c counter
            )
        """
        if cluster.version() >= '1.2':
            query = query +  "WITH compression = { 'sstable_compression' : 'SnappyCompressor' }"
        else:
            query = query +  "WITH compression_parameters:sstable_compression='SnappyCompressor'"

        cursor.execute(query)

        keys = range(0, 4)
        updates = 50

        def make_updates():
            cursor = self.patient_cql_connection(nodes[0], keyspace='ks', version=cql_version).cursor()
            upd = "UPDATE counterTable SET c = c + 1 WHERE k = %d;"
            #upd = "UPDATE counterTable SET c = c + 1 WHERE k = :k%d;"
            if cluster.version() >= '1.2':
                batch = " ".join(["BEGIN COUNTER BATCH"] + [upd % x for x in keys] + ["APPLY BATCH;"])
            else:
                batch = " ".join(["BEGIN BATCH USING CONSISTENCY LEVEL QUORUM"] + [upd % x for x in keys] + ["APPLY BATCH;"])

            #query = cursor.prepare_query(batch)

            kmap = { "k%d" % i : i for i in keys }
            for i in range(0, updates):
                if cluster.version() >= '1.2':
                    cursor.execute(batch, consistency_level='QUORUM')
                else:
                    cursor.execute(batch)

                #cursor.execute_prepared(query, kmap)

        def check(i):
            cursor = self.patient_cql_connection(nodes[0], keyspace='ks', version=cql_version).cursor()
            if cluster.version() >= '1.2':
                cursor.execute("SELECT * FROM counterTable", consistency_level='QUORUM')
            else:
                cursor.execute("SELECT * FROM counterTable USING CONSISTENCY QUORUM")
            assert cursor.rowcount == len(keys), "Expected %d rows, got %d: %s" % (len(keys), cursor.rowcount, str(cursor.fetchall()))
            for row in cursor:
                assert row[1] == i * updates, "Unexpected value %s" % str(row)

        def rolling_restart():
            # Rolling restart
            for i in range(0, 2):
                time.sleep(.2)
                nodes[i].nodetool("drain")
                nodes[i].stop(wait_other_notice=True)
                nodes[i].start(wait_other_notice=True)
                time.sleep(.2)

        make_updates()
        check(1)
        rolling_restart()

        make_updates()
        check(2)
        rolling_restart()

        make_updates()
        check(3)
        rolling_restart()

        check(3)

    def counter_consistency_test(self):
        """
        Do a bunch of writes with ONE, read back with ALL and check results.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'counter_tests', 3)
        
        stmt = """
              CREATE TABLE counter_table (
              id uuid PRIMARY KEY,
              counter_one COUNTER,
              counter_two COUNTER,
              )
           """
        cursor.execute(stmt)
        
        counters = []
        # establish 1000 counters (2x500 rows)
        for i in xrange(25):
            _id = str(uuid.uuid4())
            counters.append(
                {_id: {'counter_one':1, 'counter_two':1}}
            )
        
            cursor.execute("""
                UPDATE counter_table
                SET counter_one = counter_one + 1, counter_two = counter_two + 1
                where id = {uuid}""".format(uuid=_id), consistency_level='ONE')
        
        # increment a bunch of counters with CL.ONE
        for i in xrange(10000):
            counter = counters[random.randint(0, len(counters)-1)]
            counter_id = counter.keys()[0]

            cursor.execute("""
                UPDATE counter_table
                SET counter_one = counter_one + 2
                where id = {uuid}""".format(uuid=counter_id), consistency_level='ONE')
            
            cursor.execute("""
                UPDATE counter_table
                SET counter_two = counter_two + 10
                where id = {uuid}""".format(uuid=counter_id), consistency_level='ONE')
            
            cursor.execute("""
                UPDATE counter_table
                SET counter_one = counter_one - 1
                where id = {uuid}""".format(uuid=counter_id), consistency_level='ONE')
            
            cursor.execute("""
                UPDATE counter_table
                SET counter_two = counter_two - 5
                where id = {uuid}""".format(uuid=counter_id), consistency_level='ONE')
            
            # update expectations to match (assumed) db state
            counter[counter_id]['counter_one'] += 1
            counter[counter_id]['counter_two'] += 5
        
        # let's verify the counts are correct, using CL.ALL
        for counter_dict in counters:
            counter_id = counter_dict.keys()[0]
            
            cursor.execute("""
                SELECT counter_one, counter_two
                FROM counter_table WHERE id = {uuid}
                """.format(uuid=counter_id), consistency_level='ALL')
            
            counter_one_actual, counter_two_actual = cursor.fetchone()
            
            self.assertEqual(counter_one_actual, counter_dict[counter_id]['counter_one'])
            self.assertEqual(counter_two_actual, counter_dict[counter_id]['counter_two'])
