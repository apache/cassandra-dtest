import random, re, time, uuid

from dtest import Tester, debug
from tools import since, require
from assertions import assert_invalid
from cassandra import InvalidRequest
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.protocol import ConfigurationException


class TestGlobalIndexes(Tester):

    def prepare(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(cursor, 'users', columns=columns)

        # create index
        cursor.execute("CREATE GLOBAL INDEX ON ks.users (state) DENORMALIZED (password, session_token);")

        return cursor
    
    @since('3.0')
    @require('6477')
    def test_create_index(self):
        cursor = self.prepare()
        
        result = cursor.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)


    @since('3.0')
    @require('6477')
    def test_index_insert(self):
        cursor = self.prepare()

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")


    @since('3.0')
    @require('6477')
    def test_index_query(self):
        cursor = self.prepare()
        
        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        result = cursor.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = cursor.execute("SELECT state, password, session_token FROM users WHERE state='TX';")
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = cursor.execute("SELECT state, password, session_token FROM users WHERE state='CA';")
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = cursor.execute("SELECT state, password, session_token FROM users WHERE state='MA';")
        assert len(result) == 0, "Expecting 0 users, got" + str(result)


    @since('3.0')
    @require('6477')
    def test_index_prepared_statement(self):
        cursor = self.prepare()
        
        insertPrepared = cursor.prepare("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);")
        selectPrepared = cursor.prepare("SELECT state, password, session_token FROM users WHERE state=?;")

        # insert data
        cursor.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        cursor.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        cursor.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        cursor.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = cursor.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = cursor.execute(selectPrepared.bind(['TX']))
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = cursor.execute(selectPrepared.bind(['FL']))
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = cursor.execute(selectPrepared.bind(['MA']))
        assert len(result) == 0, "Expecting 0 users, got" + str(result)

    @since('3.0')
    @require('6477')
    def test_drop_index(self):
        cursor = self.prepare()

        result = cursor.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        cursor.execute("DROP GLOBAL INDEX ks.users_state_idx")

        result = cursor.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)

    @since('3.0')
    @require('6477')
    def test_drop_indexed_column(self):
        cursor = self.prepare()

        assert_invalid(cursor, "ALTER TABLE ks.users DROP state")
        assert_invalid(cursor, "ALTER TABLE ks.users ALTER state TYPE blob")

    @since('3.0')
    @require('6477')
    def test_double_indexing_column(self):
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE INDEX ON ks.users (state)")
        assert_invalid(cursor, "CREATE GLOBAL INDEX ON ks.users (state) DENORMALIZED (password)")

        cursor.execute("CREATE INDEX ON ks.users (gender)")
        assert_invalid(cursor, "CREATE GLOBAL INDEX ON ks.users (gender) DENORMALIZED (birth_year)")

    @since('3.0')
    @require('6477')
    def test_drop_indexed_table(self):
        cursor = self.prepare()

        result = cursor.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        cursor.execute("DROP TABLE ks.users")

        result = cursor.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)
