from dtest import Tester, debug
from pytools import rows_to_list
import tempfile, os

class TestJson(Tester):

    def json_tools_test(self):

        debug("Starting cluster...")
        cluster = self.cluster
        cluster.populate(1).start()

        debug("Version: " + cluster.version())

        debug("Getting CQLSH...")
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        debug("Inserting data...")
        self.create_ks(cursor, 'Test', 1)

        cursor.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        cursor.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('frodo', 'pass@', 'male', 'CA', 1985);")
        cursor.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('sam', '@pass', 'male', 'NY', 1980);")

        res = cursor.execute("SELECT * FROM Test. users")

        self.assertItemsEqual(rows_to_list(res),
           [ [ u'frodo', 1985, u'male', u'pass@', u'CA' ],
              [u'sam', 1980, u'male', u'@pass', u'NY' ] ] )

        debug("Flushing and stopping cluster...")
        node1.flush()
        cluster.stop()

        debug("Exporting to JSON file...")
        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as f:
            node1.run_sstable2json(f)

        debug("Deleting cluster and creating new...")
        cluster.clear()
        cluster.start()

        debug("Inserting data...")
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'Test', 1)

        cursor.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        cursor.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('gandalf', 'p@$$', 'male', 'WA', 1955);")
        node1.flush()
        cluster.stop()

        debug("Importing JSON file...")
        with open(json_path) as f:
            node1.run_json2sstable(f, "test", "users")
        os.remove(json_path)

        debug("Verifying import...")
        cluster.start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        res = cursor.execute("SELECT * FROM Test. users")

        debug("data: " + str(res))

        self.assertItemsEqual(rows_to_list(res),
           [ [ u'frodo', 1985, u'male', u'pass@', u'CA' ],
                [u'sam', 1980, u'male', u'@pass', u'NY' ],
                [u'gandalf', 1955, u'male', u'p@$$', u'WA'] ] )
