import glob
import os
import re
import subprocess
import time
import uuid
import pytest
import parse
import logging

from ccmlib import common

from tools.misc import ImmutableMapping
from dtest_setup_overrides import DTestSetupOverrides
from dtest import Tester, create_ks, create_cf
from tools.assertions import assert_length_equal, assert_stderr_clean

since = pytest.mark.since
logger = logging.getLogger(__name__)

KEYSPACE = 'ks'


class TestHelper(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        """
        disable JBOD configuration for scrub tests.
        range-aware JBOD can skip generation in SSTable,
        and some tests rely on generation numbers/
        (see CASSANDRA-11693 and increase_sstable_generations)
        """
        fixture_dtest_setup.cluster.set_datadir_count(1)


    def get_table_paths(self, table):
        """
        Return the path where the table sstables are located
        """
        node1 = self.cluster.nodelist()[0]
        paths = []
        for data_dir in node1.data_directories():
            basepath = os.path.join(data_dir, KEYSPACE)
            for x in os.listdir(basepath):
                if x.startswith(table):
                    paths.append(os.path.join(basepath, x))
                    break
        return paths

    def get_index_paths(self, table, index):
        """
        Return the paths where the index sstables are located
        """
        paths = self.get_table_paths(table)
        index_paths = []
        for path in paths:
            index_paths.append(os.path.join(path, '.' + index))
        return paths

    def get_sstable_files(self, paths):
        """
        Return the sstable files at a specific location
        """
        ret = []
        logger.debug('Checking sstables in {}'.format(paths))

        for ext in ('*.db', '*.txt', '*.adler32', '*.sha1'):
            for path in paths:
                for fname in glob.glob(os.path.join(path, ext)):
                    bname = os.path.basename(fname)
                    logger.debug('Found sstable file {}'.format(bname))
                    ret.append(bname)
        return ret

    def delete_non_essential_sstable_files(self, table):
        """
        Delete all sstable files except for the -Data.db file and the
        -Statistics.db file (only available in >= 3.0)
        """
        for fname in self.get_sstable_files(self.get_table_paths(table)):
            if not fname.endswith("-Data.db") and not fname.endswith("-Statistics.db"):
                paths = self.get_table_paths(table)
                for path in paths:
                    fullname = os.path.join(path, fname)
                    if (os.path.exists(fullname)):
                        logger.debug('Deleting {}'.format(fullname))
                        os.remove(fullname)

    def get_sstables(self, table, indexes):
        """
        Return the sstables for a table and the specified indexes of this table
        """
        sstables = {}
        table_sstables = self.get_sstable_files(self.get_table_paths(table))
        assert len(table_sstables) > 0
        sstables[table] = sorted(table_sstables)

        for index in indexes:
            index_sstables = self.get_sstable_files(self.get_index_paths(table, index))
            assert len(index_sstables) > 0
            sstables[index] = sorted('{}/{}'.format(index, sstable) for sstable in index_sstables)

        return sstables

    def launch_nodetool_cmd(self, cmd):
        """
        Launch a nodetool command and check the result is empty (no error)
        """
        node1 = self.cluster.nodelist()[0]
        response = node1.nodetool(cmd).stdout
        if not common.is_win():  # nodetool always prints out on windows
            assert_length_equal(response, 0)  # nodetool does not print anything unless there is an error

    def launch_standalone_scrub(self, ks, cf, reinsert_overflowed_ttl=False, no_validate=False):
        """
        Launch the standalone scrub
        """
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        scrub_bin = node1.get_tool('sstablescrub')
        logger.debug(scrub_bin)

        args = [scrub_bin]
        if reinsert_overflowed_ttl:
            args += ['--reinsert-overflowed-ttl']
        if no_validate:
            args += ['--no-validate']
        args += [ks, cf] if reinsert_overflowed_ttl else [ks, cf]
        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        logger.debug(out.decode("utf-8"))
        # if we have less than 64G free space, we get this warning - ignore it
        if err and "Consider adding more capacity" not in err.decode("utf-8"):
            logger.debug(err.decode("utf-8"))
            assert_stderr_clean(err.decode("utf-8"))

    def perform_node_tool_cmd(self, cmd, table, indexes):
        """
        Perform a nodetool command on a table and the indexes specified
        """
        self.launch_nodetool_cmd('{} {} {}'.format(cmd, KEYSPACE, table))
        for index in indexes:
            self.launch_nodetool_cmd('{} {} {}.{}'.format(cmd, KEYSPACE, table, index))

    def flush(self, table, *indexes):
        """
        Flush table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd('flush', table, indexes)
        return self.get_sstables(table, indexes)

    def scrub(self, table, *indexes):
        """
        Scrub table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd('scrub', table, indexes)
        time.sleep(.1)
        return self.get_sstables(table, indexes)

    def standalonescrub(self, table, *indexes):
        """
        Launch standalone scrub on table and indexes, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.launch_standalone_scrub(KEYSPACE, table)
        for index in indexes:
            self.launch_standalone_scrub(KEYSPACE, '{}.{}'.format(table, index))
        return self.get_sstables(table, indexes)

    def increment_generation_by(self, sstable, generation_increment):
        """
        Set the generation number for an sstable file name
        """
        return re.sub('(\d(?!\d))\-', lambda x: str(int(x.group(1)) + generation_increment) + '-', sstable)

    def increase_sstable_generations(self, sstables):
        """
        After finding the number of existing sstables, increase all of the
        generations by that amount.
        """
        for table_or_index, table_sstables in list(sstables.items()):
            increment_by = len(set(parse.search('{}-{increment_by}-{suffix}.{file_extention}', s).named['increment_by'] for s in table_sstables))
            sstables[table_or_index] = [self.increment_generation_by(s, increment_by) for s in table_sstables]

        logger.debug('sstables after increment {}'.format(str(sstables)))

    def uuid_sstable_identifiers_enabled(self, session):
        return 'true' == session.execute("select value from system_views.settings where name = 'uuid_sstable_identifiers_enabled'")[0][0]


@since('2.2')
class TestScrubIndexes(TestHelper):
    """
    Test that we scrub indexes as well as their parent tables
    Only valid for legacy secondary indexes
    """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()

        if dtest_config.cassandra_version_from_build >= '5.0':
            dtest_setup_overrides.cluster_options = ImmutableMapping({'default_secondary_index': 'legacy_local_table'})
        return dtest_setup_overrides

    def create_users(self, session):
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        create_cf(session, 'users', columns=columns)

        session.execute("CREATE INDEX gender_idx ON users (gender)")
        session.execute("CREATE INDEX state_idx ON users (state)")
        session.execute("CREATE INDEX birth_year_idx ON users (birth_year)")

    def update_users(self, session):
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user5', 'ch@ngem3e', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user6', 'ch@ngem3f', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user7', 'ch@ngem3g', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user8', 'ch@ngem3h', 'm', 'CA', 1982)")

        session.execute("DELETE FROM users where KEY = 'user1'")
        session.execute("DELETE FROM users where KEY = 'user5'")
        session.execute("DELETE FROM users where KEY = 'user7'")

    def query_users(self, session):
        ret = list(session.execute("SELECT * FROM users"))
        ret.extend(list(session.execute("SELECT * FROM users WHERE state='TX'")))
        ret.extend(list(session.execute("SELECT * FROM users WHERE gender='f'")))
        ret.extend(list(session.execute("SELECT * FROM users WHERE birth_year=1978")))
        assert_length_equal(ret, 8)
        return ret

    def test_scrub_static_table(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush('users', 'gender_idx', 'state_idx', 'birth_year_idx')
        scrubbed_sstables = self.scrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        users = self.query_users(session)
        assert initial_users == users

        # Scrub and check sstables and data again
        scrubbed_sstables = self.scrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        users = self.query_users(session)
        assert initial_users == users

        # Restart and check data again
        cluster.stop()
        cluster.start()

        session = self.patient_cql_connection(node1)
        session.execute('USE {}'.format(KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users

    def test_standalone_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        cluster.stop()

        scrubbed_sstables = self.standalonescrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute('USE {}'.format(KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users

    def test_scrub_collections_table(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        session.execute("CREATE TABLE users (user_id uuid PRIMARY KEY, email text, uuids list<uuid>)")
        session.execute("CREATE INDEX user_uuids_idx on users (uuids)")

        _id = uuid.uuid4()
        num_users = 100
        for i in range(0, num_users):
            user_uuid = uuid.uuid4()
            session.execute(("INSERT INTO users (user_id, email) values ({user_id}, 'test@example.com')").format(user_id=user_uuid))
            session.execute(("UPDATE users set uuids = [{id}] where user_id = {user_id}").format(id=_id, user_id=user_uuid))

        initial_users = list(session.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id)))
        assert num_users == len(initial_users)

        initial_sstables = self.flush('users', 'user_uuids_idx')
        scrubbed_sstables = self.scrub('users', 'user_uuids_idx')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        users = list(session.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id)))
        assert initial_users == users

        scrubbed_sstables = self.scrub('users', 'user_uuids_idx')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        users = list(session.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id)))

        assert initial_users == users


class TestScrub(TestHelper):
    """
    Generic tests for scrubbing
    """

    def create_users(self, session):
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        create_cf(session, 'users', columns=columns)

    def update_users(self, session):
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user5', 'ch@ngem3e', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user6', 'ch@ngem3f', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user7', 'ch@ngem3g', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user8', 'ch@ngem3h', 'm', 'CA', 1982)")

        session.execute("DELETE FROM users where KEY = 'user1'")
        session.execute("DELETE FROM users where KEY = 'user5'")
        session.execute("DELETE FROM users where KEY = 'user7'")

    def query_users(self, session):
        ret = list(session.execute("SELECT * FROM users"))
        assert_length_equal(ret, 5)
        return ret

    def test_nodetool_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        # we don't want automatic minor compaction because we want to block on
        # compactions
        node1.nodetool('disableautocompaction')

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush('users')
        scrubbed_sstables = self.scrub('users')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        users = self.query_users(session)
        assert initial_users == users

        # Scrub and check sstables and data again
        scrubbed_sstables = self.scrub('users')

        if should_assert_sstables:
           self.increase_sstable_generations(initial_sstables)
           assert initial_sstables == scrubbed_sstables

        users = self.query_users(session)
        assert initial_users == users

        # Restart and check data again
        cluster.stop()
        cluster.start()

        session = self.patient_cql_connection(node1)
        session.execute('USE {}'.format(KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users

    def test_standalone_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush('users')

        cluster.stop()

        scrubbed_sstables = self.standalonescrub('users')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute('USE {}'.format(KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users

    def test_standalone_scrub_essential_files_only(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        should_assert_sstables = cluster.version() < '5.0' or (cluster.version() >= '5.0' and not self.uuid_sstable_identifiers_enabled(session))
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush('users')

        cluster.stop()

        self.delete_non_essential_sstable_files('users')

        scrubbed_sstables = self.standalonescrub('users')

        if should_assert_sstables:
            self.increase_sstable_generations(initial_sstables)
            assert initial_sstables == scrubbed_sstables

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute('USE {}'.format(KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users

    def test_scrub_with_UDT(self):
        """
        @jira_ticket CASSANDRA-7665
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
        session.execute("use test;")
        session.execute("CREATE TYPE point_t (x double, y double);")

        node1.nodetool("scrub")
        time.sleep(2)
        match = node1.grep_log("org.apache.cassandra.serializers.MarshalException: Not enough bytes to read a set")
        assert len(match) == 0
