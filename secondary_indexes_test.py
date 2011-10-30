from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestSecondaryIndexes(Tester):

    def bug3367_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)

        columns={ "password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(cursor, 'users', columns=columns)

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', '1968');")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', '1971');");

        # create index
        cursor.execute("CREATE INDEX gender_key ON users (gender);")
        cursor.execute("CREATE INDEX state_key ON users (state);")
        cursor.execute("CREATE INDEX birth_year_key ON users (birth_year);")

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', '1978');")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', '1974');")

        cursor.execute("SELECT * FROM users;");
        result = cursor.fetchall()
        assert len(result) == 4, "Expecting 4 users, got" + str(result)

        cursor.execute("SELECT * FROM users WHERE state='TX';")
        result = cursor.fetchall()
        assert len(result) == 2, "Expecting 2 users, got" + str(result)

        cursor.execute("SELECT * FROM users WHERE state='CA';")
        result = cursor.fetchall()
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
