import csv
from uuid import uuid4
from tempfile import NamedTemporaryFile

from cassandra.concurrent import execute_concurrent_with_args

from dtest import debug, Tester


class CqlshCopyTest(Tester):

    def test_copy_to(self):
        self.cluster.populate(1).start()
        node1, = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testcopyto"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        node1.run_cqlsh(cmds="COPY ks.testcopyto TO '{name}'".format(name=tempfile.name))

        # session
        with open(tempfile.name, 'r') as csvfile:
            row_count = 0
            csvreader = csv.reader(csvfile)
            for cql_row, csv_row in zip(results, csvreader):
                self.assertEquals(map(str, cql_row), csv_row)
                row_count += 1

            self.assertEquals(len(results), row_count)

        # import the CSV file with COPY FROM
        session.execute("TRUNCATE ks.testcopyto")
        node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{name}'".format(name=tempfile.name))
        new_results = list(session.execute("SELECT * FROM testcopyto"))
        self.assertEquals(results, new_results)
