import re
import time
import uuid

from cassandra import ConsistencyLevel, Unauthorized
from cassandra.query import SimpleStatement

from assertions import assert_invalid
from dtest import Tester
from tools import since


def listify(item):
    """
    listify a query result consisting of user types

    returns nested arrays representing user type ordering
    """
    if isinstance(item, (tuple, list)):
        return [listify(i) for i in item]
    else:
        return item


class TestUserTypes(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def assertUnauthorized(self, session, query, message):
        with self.assertRaises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, cm.exception.message), "Expected: %s" % message

    def assertNoTypes(self, session):
        for keyspace in session.cluster.metadata.keyspaces.values():
            self.assertEqual(0, len(keyspace.user_types))

    def test_type_dropping(self):
        """
        Tests that a type cannot be dropped when in use, and otherwise can be dropped.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_type_dropping', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              USE user_type_dropping
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        session.execute(stmt)
        # Make sure the schema propagate
        time.sleep(2)

        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 1}});
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(session, stmt, 'Cannot drop user type user_type_dropping.simple_type as it is still used by table user_type_dropping.simple_table')

        # now that we've confirmed that a user type cannot be dropped while in use
        # let's remove the offending table

        # TODO: uncomment below after CASSANDRA-6472 is resolved
        # and add another check to make sure the table/type drops succeed
        stmt = """
              DROP TABLE simple_table;
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        session.execute(stmt)

        # now let's have a look at the system schema and make sure no user types are defined
        self.assertNoTypes(session)

    def test_nested_type_dropping(self):
        """
        Confirm a user type can't be dropped when being used by another user type.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'nested_user_type_dropping', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              USE nested_user_type_dropping
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int,
              user_text text
              )
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE another_type (
              somefield frozen<simple_type>
              )
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(session, stmt, 'Cannot drop user type nested_user_type_dropping.simple_type as it is still used by user type another_type')

        # drop the type that's impeding the drop, and then try again
        stmt = """
              DROP TYPE another_type;
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        session.execute(stmt)

        # now let's have a look at the system schema and make sure no user types are defined
        self.assertNoTypes(session)

    def test_type_enforcement(self):
        """
        Confirm error when incorrect data type used for user type
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        self.create_ks(session, 'user_type_enforcement', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              USE user_type_enforcement
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        session.execute(stmt)
        # Make sure the schema propagate
        time.sleep(2)

        # here we will attempt an insert statement which should fail
        # because the user type is an int, but the insert statement is
        # providing text
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 'uh oh....this is not a number'}});
           """.format(id=_id)
        assert_invalid(session, stmt, 'field user_number is not of type int')

        # let's check the rowcount and make sure the data
        # didn't get inserted when the exception asserted above was thrown
        stmt = """
              SELECT * FROM simple_table;
           """
        rows = list(session.execute(stmt))
        self.assertEqual(0, len(rows))

    def test_nested_user_types(self):
        """Tests user types within user types"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_types', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              USE user_types
           """
        session.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        session.execute(stmt)

        # Create a user type to contain the item:
        stmt = """
              CREATE TYPE container (
              stuff text,
              more_stuff frozen<item>
              )
           """
        session.execute(stmt)

        # Create a table that holds and item, a container, and a
        # list of containers:
        stmt = """
              CREATE TABLE bucket (
               id uuid PRIMARY KEY,
               primary_item frozen<item>,
               other_items frozen<container>,
               other_containers list<frozen<container>>
              )
           """
        session.execute(stmt)
        # Make sure the schema propagate
        time.sleep(2)

        # Insert some data:
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO bucket (id, primary_item)
              VALUES ({id}, {{sub_one: 'test', sub_two: 'test2'}});
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_items = {{stuff: 'stuff', more_stuff: {{sub_one: 'one', sub_two: 'two'}}}}
              WHERE id={id};
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [
                   {{
                       stuff: 'stuff2',
                       more_stuff: {{sub_one: 'one_other', sub_two: 'two_other'}}
                   }}
              ]
              WHERE id={id};
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [
                  {{
                      stuff: 'stuff3',
                      more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}
                  }},
                  {{stuff: 'stuff4',
                    more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}
                  }}
              ]
              WHERE id={id};
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              SELECT primary_item, other_items, other_containers from bucket where id={id};
           """.format(id=_id)
        rows = list(session.execute(stmt))

        primary_item, other_items, other_containers = rows[0]
        self.assertEqual(listify(primary_item), [u'test', u'test2'])
        self.assertEqual(listify(other_items), [u'stuff', [u'one', u'two']])
        self.assertEqual(listify(other_containers), [[u'stuff2', [u'one_other', u'two_other']], [u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])

        #  Generate some repetitive data and check it for it's contents:
        for x in xrange(50):

            # Create row:
            _id = uuid.uuid4()
            stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [
                  {{
                      stuff: 'stuff3',
                      more_stuff: {{
                          sub_one: 'one_2_other', sub_two: 'two_2_other'
                      }}
                  }},
                  {{
                      stuff: 'stuff4',
                      more_stuff: {{
                          sub_one: 'one_3_other', sub_two: 'two_3_other'
                      }}
                  }}
              ]
              WHERE id={id};
           """.format(id=_id)
            session.execute(stmt)

            time.sleep(0.1)

            # Check it:
            stmt = """
              SELECT other_containers from bucket WHERE id={id}
            """.format(id=_id)
            rows = list(session.execute(stmt))

            items = rows[0][0]
            self.assertEqual(listify(items), [[u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])

    def test_type_as_part_of_pkey(self):
        """Tests user types as part of a composite pkey"""
        # make sure we can define a table with a user type as part of the pkey
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_type_pkeys', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid,
              name frozen<t_person_name>,
              like text,
              PRIMARY KEY ((id, name))
              )
           """
        session.execute(stmt)
        # Make sure the schema propagate
        time.sleep(2)

        _id = uuid.uuid4()

        stmt = """
              INSERT INTO person_likes (id, name, like)
              VALUES ({id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """.format(id=_id)
        session.execute(stmt)

        # attempt to query without the user type portion of the pkey and confirm there is an error
        stmt = """
              SELECT id, name.first from person_likes where id={id};
           """.format(id=_id)

        if self.cluster.version() >= '2.2':
            assert_invalid(session, stmt, 'Partition key parts: name must be restricted as other parts are')
        else:
            assert_invalid(session, stmt, 'Partition key part name must be restricted since preceding part is')

        stmt = """
              SELECT id, name.first, like from person_likes where id={id} and name = {{first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'}};
           """.format(id=_id)
        rows = session.execute(stmt)

        row_uuid, first_name, like = rows[0]
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')

    def test_type_secondary_indexing(self):
        """
        Confirm that user types are secondary-indexable
        Similar procedure to TestSecondaryIndexesOnCollections.test_list_indexes
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_type_indexing', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid PRIMARY KEY,
              name frozen<t_person_name>,
              like text
              )
           """
        session.execute(stmt)
        # Make sure the schema propagate
        time.sleep(2)

        # add index and query (even though there are no rows in the table yet)
        stmt = """
              CREATE INDEX person_likes_name on person_likes (name);
            """
        session.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """
        rows = list(session.execute(stmt))
        self.assertEqual(0, len(rows))

        # add a row which doesn't specify data for the indexed column, and query again
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO person_likes (id, like)
              VALUES ({id}, 'long walks on the beach');
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Bob', middle: 'Testy', last: 'McTesterson'};
            """

        rows = list(session.execute(stmt))
        self.assertEqual(0, len(rows))

        # finally let's add a queryable row, and get it back using the index
        _id = uuid.uuid4()

        stmt = """
              INSERT INTO person_likes (id, name, like)
              VALUES ({id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
              SELECT id, name.first, like from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
           """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')

        # rename a field in the type and make sure the index still works
        stmt = """
            ALTER TYPE t_person_name rename first to first_name;
            """
        session.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')

        # add another row to be sure the index is still adding new data
        _id = uuid.uuid4()

        stmt = """
              INSERT INTO person_likes (id, name, like)
              VALUES ({id}, {{first_name:'Abraham', middle:'', last:'Lincoln'}}, 'preserving unions');
           """.format(id=_id)
        session.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Abraham', middle:'', last:'Lincoln'};
            """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Abraham')
        self.assertEqual(like, u'preserving unions')

    def test_type_keyspace_permission_isolation(self):
        """
        Confirm permissions are respected for types in different keyspaces
        """
        self.ignore_log_patterns = [
            # I think this happens when permissions change and a node becomes temporarily unavailable
            # and it's probably ok to ignore on this test, as I can see the schema changes propogating
            # almost immediately after
            r'Can\'t send migration request: node.*is down',
        ]

        cluster = self.cluster
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': 0}
        cluster.set_configuration_options(values=config)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        # need a bit of time for user to be created and propagate
        time.sleep(5)

        # do setup that requires a super user
        superuser_session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        superuser_session.execute("create user ks1_user with password 'cassandra' nosuperuser;")
        superuser_session.execute("create user ks2_user with password 'cassandra' nosuperuser;")
        self.create_ks(superuser_session, 'ks1', 2)
        self.create_ks(superuser_session, 'ks2', 2)
        superuser_session.execute("grant all permissions on keyspace ks1 to ks1_user;")
        superuser_session.execute("grant all permissions on keyspace ks2 to ks2_user;")

        user1_session = self.patient_cql_connection(node1, user='ks1_user', password='cassandra')
        user2_session = self.patient_cql_connection(node1, user='ks2_user', password='cassandra')

        # first make sure the users can't create types in each other's ks
        self.assertUnauthorized(user1_session, "CREATE TYPE ks2.simple_type (user_number int, user_text text );", 'User ks1_user has no CREATE permission on <keyspace ks2> or any of its parents')

        self.assertUnauthorized(user2_session, "CREATE TYPE ks1.simple_type (user_number int, user_text text );", 'User ks2_user has no CREATE permission on <keyspace ks1> or any of its parents')

        # now, actually create the types in the correct keyspaces
        user1_session.execute("CREATE TYPE ks1.simple_type (user_number int, user_text text );")
        user2_session.execute("CREATE TYPE ks2.simple_type (user_number int, user_text text );")

        # each user now has a type belonging to their granted keyspace
        # let's make sure they can't drop each other's types (for which they have no permissions)

        self.assertUnauthorized(user1_session, "DROP TYPE ks2.simple_type;", 'User ks1_user has no DROP permission on <keyspace ks2> or any of its parents')

        self.assertUnauthorized(user2_session, "DROP TYPE ks1.simple_type;", 'User ks2_user has no DROP permission on <keyspace ks1> or any of its parents')

        # let's make sure they can't rename each other's types (for which they have no permissions)
        self.assertUnauthorized(user1_session, "ALTER TYPE ks2.simple_type RENAME user_number TO user_num;", 'User ks1_user has no ALTER permission on <keyspace ks2> or any of its parents')

        self.assertUnauthorized(user2_session, "ALTER TYPE ks1.simple_type RENAME user_number TO user_num;", 'User ks2_user has no ALTER permission on <keyspace ks1> or any of its parents')

        # rename the types using the correct user w/permissions to do so
        user1_session.execute("ALTER TYPE ks1.simple_type RENAME user_number TO user_num;")
        user2_session.execute("ALTER TYPE ks2.simple_type RENAME user_number TO user_num;")

        # finally, drop the types using the correct user w/permissions to do so, consistency all avoids using a sleep
        user1_session.execute(SimpleStatement("DROP TYPE ks1.simple_type;", consistency_level=ConsistencyLevel.ALL))
        user2_session.execute(SimpleStatement("DROP TYPE ks2.simple_type;", consistency_level=ConsistencyLevel.ALL))

        time.sleep(5)

        # verify user type metadata is gone from the system schema
        self.assertNoTypes(superuser_session)

    def test_nulls_in_user_types(self):
        """Tests user types with null values"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_types', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              USE user_types
           """
        session.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        session.execute(stmt)

        # Create a table that holds an item
        stmt = """
              CREATE TABLE bucket (
               id int PRIMARY KEY,
               my_item frozen<item>,
              )
           """
        session.execute(stmt)
        # Make sure the schema propagates
        time.sleep(2)

        # Adds an explicit null
        session.execute("INSERT INTO bucket (id, my_item) VALUES (0, {sub_one: 'test', sub_two: null})")
        # Adds with an implicit null
        session.execute("INSERT INTO bucket (id, my_item) VALUES (1, {sub_one: 'test'})")

        rows = list(session.execute("SELECT my_item FROM bucket WHERE id=0"))
        self.assertEqual(listify(rows[0]), [[u'test', None]])

        rows = list(session.execute("SELECT my_item FROM bucket WHERE id=1"))
        self.assertEqual(listify(rows[0]), [[u'test', None]])

    def test_no_counters_in_user_types(self):
        # CASSANDRA-7672
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_types', 1)

        stmt = """
            USE user_types
         """
        session.execute(stmt)

        stmt = """
            CREATE TYPE t_item (
            sub_one COUNTER )
         """

        assert_invalid(session, stmt, 'A user type cannot contain counters')

    def test_type_as_clustering_col(self):
        """Tests user types as clustering column"""
        # make sure we can define a table with a user type as a clustering column
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_type_pkeys', 2)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stmt = """
              CREATE TYPE t_letterpair (
              first text,
              second text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE letters (
              id int,
              letterpair frozen<t_letterpair>,
              PRIMARY KEY (id, letterpair)
              )
           """
        session.execute(stmt)
        # address CASSANDRA-11498
        session.cluster.control_connection.wait_for_schema_agreement()

        # create a bit of data and expect a natural order based on clustering user types

        ids = range(1, 10)

        for _id in ids:
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'a', second:'z'}})".format(_id))
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'z', second:'a'}})".format(_id))
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'f'}})".format(_id))
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'a'}})".format(_id))
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'z'}})".format(_id))
            session.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'d', second:'e'}})".format(_id))

        for _id in ids:
            res = list(session.execute("SELECT letterpair FROM letters where id = {}".format(_id)))

            self.assertEqual(listify(res), [[[u'a', u'z']], [[u'c', u'a']], [[u'c', u'f']], [[u'c', u'z']], [[u'd', u'e']], [[u'z', u'a']]])

    @since('3.6')
    def udt_subfield_test(self):
        """
        @jira_ticket CASSANDRA-7423
        @since 3.6
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_types', 1)
        session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

        # Check we can create non-frozen table
        session.execute("CREATE TYPE udt (first ascii, second int, third int)")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v udt)")

        # Fill in a full UDT across two statements
        # Ensure all subfields are set
        session.execute("INSERT INTO t (id, v) VALUES (0, {third: 2, second: 1})")
        session.execute("UPDATE t set v.first = 'a' WHERE id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id = 0"))
        self.assertEqual(listify(rows), [[0, ['a', 1, 2]]])

        # Create a full udt
        # Update a subfield on the udt
        # Read back the updated udt
        session.execute("INSERT INTO t (id, v) VALUES (0, {first: 'c', second: 3, third: 33})")
        session.execute("UPDATE t set v.second = 5 where id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[0, ['c', 5, 33]]])

        # Rewrite the entire udt
        # Read back
        session.execute("INSERT INTO t (id, v) VALUES (0, {first: 'alpha', second: 111, third: 100})")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[0, ['alpha', 111, 100]]])

        # Send three subfield updates to udt
        # Read back
        session.execute("UPDATE t set v.first = 'beta' WHERE id=0")
        session.execute("UPDATE t set v.first = 'delta' WHERE id=0")
        session.execute("UPDATE t set v.second = -10 WHERE id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[0, ['delta', -10, 100]]])

        # Send conflicting updates serially to different nodes
        # Read back
        session1 = self.exclusive_cql_connection(node1)
        session2 = self.exclusive_cql_connection(node2)
        session3 = self.exclusive_cql_connection(node3)

        session1.execute("UPDATE user_types.t set v.third = 101 WHERE id=0")
        session2.execute("UPDATE user_types.t set v.third = 102 WHERE id=0")
        session3.execute("UPDATE user_types.t set v.third = 103 WHERE id=0")
        query = SimpleStatement("SELECT * FROM t WHERE id = 0", consistency_level=ConsistencyLevel.ALL)
        rows = list(session.execute(query))
        self.assertEqual(listify(rows), [[0, ['delta', -10, 103]]])
        session1.shutdown()
        session2.shutdown()
        session3.shutdown()

        # Write full UDT, set one field to null, read back
        session.execute("INSERT INTO t (id, v) VALUES (0, {first:'cass', second:3, third:0})")
        session.execute("UPDATE t SET v.first = null WHERE id = 0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[0, [None, 3, 0]]])

        rows = list(session.execute("SELECT v.first FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[None]])
        rows = list(session.execute("SELECT v.second FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[3]])
        rows = list(session.execute("SELECT v.third FROM t WHERE id=0"))
        self.assertEqual(listify(rows), [[0]])

    @since('2.2')
    def test_user_type_isolation(self):
        """
        Ensure UDT cannot be used from another keyspace
        @jira_ticket CASSANDRA-9409
        @since 2.2
        """

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'user_types', 1)

        # create a user defined type in a keyspace
        session.execute("CREATE TYPE udt (first text, second int, third int)")

        # ensure we cannot use a udt from another keyspace
        self.create_ks(session, 'user_ks', 1)
        assert_invalid(
            session,
            "CREATE TABLE t (id int PRIMARY KEY, v frozen<user_types.udt>)",
            "Statement on keyspace user_ks cannot refer to a user type in keyspace user_types"
        )
