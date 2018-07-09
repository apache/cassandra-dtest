import os
import tempfile
import pytest
import logging

from dtest import Tester, create_ks
from tools.data import rows_to_list
from tools.assertions import assert_lists_equal_ignoring_order

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('0', max_version='2.2.X')
class TestJson(Tester):

    def test_json_tools(self):

        logger.debug("Starting cluster...")
        cluster = self.cluster
        cluster.set_batch_commitlog(enabled=True)
        cluster.populate(1).start()

        logger.debug("Version: " + cluster.version().vstring)

        logger.debug("Getting CQLSH...")
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        logger.debug("Inserting data...")
        create_ks(session, 'Test', 1)

        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) "
                        "VALUES ('frodo', 'pass@', 'male', 'CA', 1985);")
        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) "
                        "VALUES ('sam', '@pass', 'male', 'NY', 1980);")

        res = session.execute("SELECT * FROM Test. users")

        assert_lists_equal_ignoring_order(rows_to_list(res), [['frodo', 1985, 'male', 'pass@', 'CA'],
                                                              ['sam', 1980, 'male', '@pass', 'NY']])

        logger.debug("Flushing and stopping cluster...")
        node1.flush()
        cluster.stop()

        logger.debug("Exporting to JSON file...")
        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as f:
            node1.run_sstable2json(f)

        with open(json_path, 'r') as fin:
            data = fin.read().splitlines(True)
        if data[0][0] == 'W':
            with open(json_path, 'w') as fout:
                fout.writelines(data[1:])

        logger.debug("Deleting cluster and creating new...")
        cluster.clear()
        cluster.start()

        logger.debug("Inserting data...")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'Test', 1)

        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) "
                        "VALUES ('gandalf', 'p@$$', 'male', 'WA', 1955);")
        node1.flush()
        cluster.stop()

        logger.debug("Importing JSON file...")
        with open(json_path) as f:
            node1.run_json2sstable(f, "test", "users")
        os.remove(json_path)

        logger.debug("Verifying import...")
        cluster.start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        res = session.execute("SELECT * FROM Test. users")

        logger.debug("data: " + str(res))

        assert_lists_equal_ignoring_order(rows_to_list(res), [['frodo', 1985, 'male', 'pass@', 'CA'],
                                                              ['sam', 1980, 'male', '@pass', 'NY'],
                                                              ['gandalf', 1955, 'male', 'p@$$', 'WA']])
