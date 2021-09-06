"""
Home for upgrade-related tests that don't fit in with the core upgrade testing in dtest.upgrade_through_versions
"""
import glob
import os
import re
import time
import pytest
import logging

from cassandra import ConsistencyLevel as CL

from dtest import RUN_STATIC_UPGRADE_MATRIX
from tools.jmxutils import (JolokiaAgent, make_mbean)
from tools.misc import add_skip
from .upgrade_base import UpgradeTester
from .upgrade_manifest import build_upgrade_pairs

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestForRegressions(UpgradeTester):
    """
    Catch-all class for regression tests on specific versions.
    """
    NODES, RF, __test__, CL = 2, 1, False, CL.ONE

    def test_10822(self):
        """
        @jira_ticket CASSANDRA-10822

        Original issue was seen when upgrading from 2.1 to 3.X versions.
        """
        session = self.prepare()

        session.execute("CREATE KEYSPACE financial WITH replication={'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute("""
        create table if not exists financial.symbol_history (
          symbol text,
          name text,
          year int,
          month int,
          day int,
          volume bigint,
          close double,
          open double,
          low double,
          high double,
          primary key((symbol, year), month, day)
        ) with CLUSTERING ORDER BY (month desc, day desc);
        """)

        symbol_years = [('CORP', 2004), ('BLAH', 2005), ('FOO', 2006), ('BAR', 2007), ('HUH', 2008)]

        for symbol, year in symbol_years:
            for month in range(0, 50):
                session.execute("INSERT INTO financial.symbol_history (symbol, name, year, month, day, volume) VALUES ('{}', 'MegaCorp', {}, {}, 1, 100)".format(symbol, year, month))

        for symbol, year in symbol_years:
            session.execute("DELETE FROM financial.symbol_history WHERE symbol='{}' and year = {} and month=25;".format(symbol, year, month))

        sessions = self.do_upgrade(session)

        for s in sessions:
            expected_rows = 49

            for symbol, year in symbol_years:
                count = s[1].execute("select count(*) from financial.symbol_history where symbol='{}' and year={};".format(symbol, year))[0][0]
                assert count == expected_rows, "actual {} did not match expected {}".format(count, expected_rows)

    def test13294(self):
        """
        Tests upgrades with files having a bunch of files with the same prefix as another file

        this file is then compacted and we verify that no other sstables are removed

        @jira_ticket CASSANDRA-13294
        """
        cluster = self.cluster
        cluster.set_datadir_count(1)  # we want the same prefix for all sstables
        session = self.prepare()
        session.execute("CREATE KEYSPACE test13294 WITH replication={'class':'SimpleStrategy', 'replication_factor': 2};")
        session.execute("CREATE TABLE test13294.t (id int PRIMARY KEY, d int) WITH compaction = {'class': 'SizeTieredCompactionStrategy','enabled':'false'}")
        for x in range(0, 5):
            session.execute("INSERT INTO test13294.t (id, d) VALUES (%d, %d)" % (x, x))
            cluster.flush()

        node1 = cluster.nodelist()[0]

        sstables = node1.get_sstables('test13294', 't')
        node1.stop(wait_other_notice=True)
        generation_re = re.compile(r'(.*-)(\d+)(-.*)')
        mul = 1
        first_sstable = ''
        for sstable in sstables:
            res = generation_re.search(sstable)
            if res:
                glob_for = "%s%s-*" % (res.group(1), res.group(2))
                for f in glob.glob(glob_for):
                    res2 = generation_re.search(f)
                    new_filename = "%s%s%s" % (res2.group(1), mul, res2.group(3))
                    os.rename(f, new_filename)
                    if first_sstable == '' and '-Data' in new_filename:
                        first_sstable = new_filename  # we should compact this
                mul = mul * 10
        node1.start()
        sessions = self.do_upgrade(session)
        checked = False
        for is_upgraded, cursor in sessions:
            if is_upgraded:
                sstables_before = self.get_all_sstables(node1)
                self.compact_sstable(node1, first_sstable)
                time.sleep(2)  # wait for sstables to get physically removed
                sstables_after = self.get_all_sstables(node1)
                # since autocompaction is disabled and we compact a single sstable above
                # the number of sstables after should be the same as before.
                assert len(sstables_before) == len(sstables_after)
                checked = True
        assert checked

    @since('3.0.14', max_version='3.0.99')
    def test_schema_agreement(self):
        """
        Test that nodes agree on the schema during an upgrade in the 3.0.x series.

        Create a table before upgrading the cluster and wait for schema agreement.
        Upgrade one node and create one more table, wait for schema agreement and check
        the schema versions with nodetool describecluster.

        We know that schemas will not necessarily agree from 2.1/2.2 to 3.0.x or from 3.0.x to 3.x
        and upwards, so we only test the 3.0.x series for now. We start with 3.0.13 because
        there is a problem in 3.0.13, see CASSANDRA-12213 and 13559.

        @jira_ticket CASSANDRA-13559
        """
        session = self.prepare(nodes=5)
        session.execute("CREATE TABLE schema_agreement_test_1 ( id int PRIMARY KEY, value text )")
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=30)

        def validate_schema_agreement(n, is_upgr):
            logger.debug("querying node {} for schema information, upgraded: {}".format(n.name, is_upgr))

            response = n.nodetool('describecluster').stdout
            logger.debug(response)
            schemas = response.split('Schema versions:')[1].strip()
            num_schemas = len(re.findall(r'\[.*?\]', schemas))
            assert num_schemas == 1, "There were multiple schema versions during an upgrade: {}" \
                             .format(schemas)

        for node in self.cluster.nodelist():
            validate_schema_agreement(node, False)

        for is_upgraded, session, node in self.do_upgrade(session, return_nodes=True):
            validate_schema_agreement(node, is_upgraded)
            if is_upgraded:
                session.execute("CREATE TABLE schema_agreement_test_2 ( id int PRIMARY KEY, value text )")
                session.cluster.control_connection.wait_for_schema_agreement(wait_time=30)
                validate_schema_agreement(node, is_upgraded)

    def compact_sstable(self, node, sstable):
        mbean = make_mbean('db', type='CompactionManager')
        with JolokiaAgent(node) as jmx:
            jmx.execute_method(mbean, 'forceUserDefinedCompaction', [sstable])

    def get_all_sstables(self, node):
        # note that node.get_sstables(...) only returns current version sstables
        keyspace_dirs = [os.path.join(node.get_path(), "data{0}".format(x), "test13294") for x in range(0, node.cluster.data_dir_count)]
        files = []
        for d in keyspace_dirs:
            for f in glob.glob(d + "/*/*Data*"):
                files.append(f)
        return files


for path in build_upgrade_pairs():
    gen_class_name = TestForRegressions.__name__ + path.name
    assert gen_class_name not in globals()
    spec = {'UPGRADE_PATH': path,
            '__test__': True}

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or path.upgrade_meta.matches_current_env_version_family
    cls = type(gen_class_name, (TestForRegressions,), spec)
    if not upgrade_applies_to_env:
        add_skip(cls, 'test not applicable to env.')
    globals()[gen_class_name] = cls
