from dtest import Tester
from tools import since, require
from assertions import assert_invalid


@since('3.0')
@require('6477')
class TestGlobalIndexes(Tester):

    def prepare(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(session, 'users', columns=columns)

        # create index
        session.execute("CREATE GLOBAL INDEX ON ks.users (state) DENORMALIZED (password, session_token);")

        return session

    def test_create_index(self):
        session = self.prepare()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

    def test_index_insert(self):
        session = self.prepare()

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

    def test_index_query(self):
        session = self.prepare()

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        result = session.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='TX';")
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='CA';")
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='MA';")
        assert len(result) == 0, "Expecting 0 users, got" + str(result)

    def test_index_prepared_statement(self):
        session = self.prepare()

        insertPrepared = session.prepare("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);")
        selectPrepared = session.prepare("SELECT state, password, session_token FROM users WHERE state=?;")

        # insert data
        session.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        session.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        session.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        session.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = session.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['TX']))
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['FL']))
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['MA']))
        assert len(result) == 0, "Expecting 0 users, got" + str(result)

    def test_drop_index(self):
        session = self.prepare()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        session.execute("DROP GLOBAL INDEX ks.users_state_idx")

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)

    def test_drop_indexed_column(self):
        session = self.prepare()

        assert_invalid(session, "ALTER TABLE ks.users DROP state")
        assert_invalid(session, "ALTER TABLE ks.users ALTER state TYPE blob")

    def test_double_indexing_column(self):
        session = self.prepare()

        assert_invalid(session, "CREATE INDEX ON ks.users (state)")
        assert_invalid(session, "CREATE GLOBAL INDEX ON ks.users (state) DENORMALIZED (password)")

        session.execute("CREATE INDEX ON ks.users (gender)")
        assert_invalid(session, "CREATE GLOBAL INDEX ON ks.users (gender) DENORMALIZED (birth_year)")

    def test_drop_indexed_table(self):
        session = self.prepare()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        session.execute("DROP TABLE ks.users")

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)
