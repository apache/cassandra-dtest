"""
Home for upgrade-related tests that don't fit in with the core upgrade testing in dtest.upgrade_through_versions
"""
from unittest import skipUnless

from cassandra import ConsistencyLevel as CL
from nose.tools import assert_not_in

from dtest import RUN_STATIC_UPGRADE_MATRIX
from upgrade_base import UpgradeTester
from upgrade_manifest import build_upgrade_pairs


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
                self.assertEqual(count, expected_rows, "actual {} did not match expected {}".format(count, expected_rows))


for path in build_upgrade_pairs():
    gen_class_name = TestForRegressions.__name__ + path.name
    assert_not_in(gen_class_name, globals())
    spec = {'UPGRADE_PATH': path,
            '__test__': True}

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or path.upgrade_meta.matches_current_env_version_family
    globals()[gen_class_name] = skipUnless(upgrade_applies_to_env, 'test not applicable to env.')(type(gen_class_name, (TestForRegressions,), spec))
