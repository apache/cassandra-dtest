import struct
import time
import uuid
from dtest import Tester, debug
from pytools import since
from pyassertions import assert_invalid


def decode_text(string):
    """
    decode bytestring as utf-8
    """
    return string.decode('utf-8')


def len_unpacker(val):
    return struct.Struct('>i').unpack(val)[0]


def unpack(bytestr):
    # The composite format for each component is:
    #   <len>   <value>
    # 4 bytes | <len> bytes
    components = []
    while bytestr:
        length = len_unpacker(bytestr[:4])
        if length < 0:
            components.append(None)
            bytestr = bytestr[4:]
        else:
            components.append(decode_text(bytestr[4:4 + length]))
            bytestr = bytestr[4 + length:]
    return tuple(components)


def decode(item):
    """
    decode a query result consisting of user types

    returns nested arrays representing user type ordering
    """
    decoded = []

    if isinstance(item, tuple) or isinstance(item, list):
        for i in item:
            decoded.extend(decode(i))
    else:
        if item is not None and item.startswith('\x00'):
            unpacked = unpack(item)
            decoded.append(decode(unpacked))
        else:
            decoded.append(item)

    return decoded


class TestUserTypes(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    # We've removed type renaming for now (CASSANDRA-6940)
    #@since('2.1')
    #def test_type_renaming(self):
    #  """
    #  Confirm that types can be renamed and the proper associations are updated.
    #  """
    #  cluster = self.cluster
    #  cluster.populate(3).start()
    #  node1, node2, node3 = cluster.nodelist()
    #  cursor = self.patient_cql_connection(node1)
    #  self.create_ks(cursor, 'user_type_renaming', 2)

    #  stmt = """
    #        CREATE TYPE simple_type (
    #        user_number int
    #        )
    #     """
    #  cursor.execute(stmt)

    #  stmt = """
    #        CREATE TABLE simple_table (
    #        id uuid PRIMARY KEY,
    #        number simple_type
    #        )
    #     """
    #  cursor.execute(stmt)

    #  stmt = """
    #      ALTER TYPE simple_type rename to renamed_type;
    #     """
    #  cursor.execute(stmt)

    #  stmt = """
    #      SELECT type_name from system.schema_usertypes;
    #     """
    #  cursor.execute(stmt)
    #  # we should only have one user type in this test
    #  self.assertEqual(1, cursor.rowcount)

    #  # finally let's look for the new type name
    #  self.assertEqual(cursor.fetchone()[0], u'renamed_type')

    #@since('2.1')
    #def test_nested_type_renaming(self):
    #    """
    #    Confirm type renaming works as expected on nested types.
    #    """
    #    cluster = self.cluster
    #    cluster.populate(3).start()
    #    node1, node2, node3 = cluster.nodelist()
    #    cursor = self.patient_cql_connection(node1)
    #    self.create_ks(cursor, 'nested_user_type_renaming', 2)

    #    stmt = """
    #          USE nested_user_type_renaming
    #       """
    #    cursor.execute(stmt)

    #    stmt = """
    #          CREATE TYPE simple_type (
    #          user_number int,
    #          user_text text
    #          )
    #       """
    #    cursor.execute(stmt)

    #    stmt = """
    #          CREATE TYPE another_type (
    #          somefield simple_type
    #          )
    #       """
    #    cursor.execute(stmt)

    #    stmt = """
    #          CREATE TYPE yet_another_type (
    #          some_other_field another_type
    #          )
    #       """
    #    cursor.execute(stmt)

    #    stmt = """
    #          CREATE TABLE uses_nested_type (
    #          id uuid PRIMARY KEY,
    #          field_name yet_another_type
    #          )
    #       """
    #    cursor.execute(stmt)

    #    # let's insert some basic data using the nested types
    #    _id = uuid.uuid4()
    #    stmt = """
    #          INSERT INTO uses_nested_type (id, field_name)
    #          VALUES (%s, {some_other_field: {somefield: {user_number: 1, user_text: 'original'}}});
    #       """ % _id
    #    cursor.execute(stmt)

    #    # rename one of the types used in the nesting
    #    stmt = """
    #          ALTER TYPE another_type rename to another_type2;
    #       """
    #    cursor.execute(stmt)

    #    # confirm nested data can be queried without error
    #    stmt = """
    #          SELECT field_name FROM uses_nested_type where id = {id}
    #       """.format(id=_id)
    #    cursor.execute(stmt)

    #    data = cursor.fetchone()[0]
    #    self.assertIn('original', data)

    #    # confirm we can alter/query the data after altering the type
    #    stmt = """
    #          UPDATE uses_nested_type
    #          SET field_name = {some_other_field: {somefield: {user_number: 2, user_text: 'altered'}}}
    #          WHERE id=%s;
    #       """ % _id
    #    cursor.execute(stmt)

    #    stmt = """
    #          SELECT field_name FROM uses_nested_type where id = {id}
    #       """.format(id=_id)
    #    cursor.execute(stmt)

    #    data = cursor.fetchone()[0]
    #    self.assertIn('altered', data)

    #    # and confirm we can add/query new data after the type rename
    #    _id = uuid.uuid4()
    #    stmt = """
    #          INSERT INTO uses_nested_type (id, field_name)
    #          VALUES (%s, {some_other_field: {somefield: {user_number: 1, user_text: 'inserted'}}});
    #       """ % _id
    #    cursor.execute(stmt)

    #    stmt = """
    #          SELECT field_name FROM uses_nested_type where id = {id}
    #       """.format(id=_id)
    #    cursor.execute(stmt)

    #    data = cursor.fetchone()[0]
    #    self.assertIn('inserted', data)

    @since('2.1')
    def test_type_dropping(self):
        """
        Tests that a type cannot be dropped when in use, and otherwise can be dropped.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_type_dropping', 2)

        stmt = """
              USE user_type_dropping
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        cursor.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 1}});
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(cursor, stmt, 'Cannot drop user type user_type_dropping.simple_type as it is still used by table user_type_dropping.simple_table')

        # now that we've confirmed that a user type cannot be dropped while in use
        # let's remove the offending table

        # TODO: uncomment below after CASSANDRA-6472 is resolved
        # and add another check to make sure the table/type drops succeed
        stmt = """
              DROP TABLE simple_table;
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        cursor.execute(stmt)

        # now let's have a look at the system schema and make sure no user types are defined
        stmt = """
              SELECT type_name from system.schema_usertypes;
           """
        rows = cursor.execute(stmt)
        self.assertEqual(0, len(rows))

    @since('2.1')
    def test_nested_type_dropping(self):
        """
        Confirm a user type can't be dropped when being used by another user type.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'nested_user_type_dropping', 2)

        stmt = """
              USE nested_user_type_dropping
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int,
              user_text text
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE another_type (
              somefield frozen<simple_type>
              )
           """
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(cursor, stmt, 'Cannot drop user type nested_user_type_dropping.simple_type as it is still used by user type another_type')

        # drop the type that's impeding the drop, and then try again
        stmt = """
              DROP TYPE another_type;
           """
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        cursor.execute(stmt)

        # now let's have a look at the system schema and make sure no user types are defined
        stmt = """
              SELECT type_name from system.schema_usertypes;
           """
        rows = cursor.execute(stmt)
        self.assertEqual(0, len(rows))

    @since('2.1')
    def test_type_enforcement(self):
        """
        Confirm error when incorrect data type used for user type
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.cql_connection(node1)
        self.create_ks(cursor, 'user_type_enforcement', 2)

        stmt = """
              USE user_type_enforcement
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        cursor.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        # here we will attempt an insert statement which should fail
        # because the user type is an int, but the insert statement is
        # providing text
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 'uh oh....this is not a number'}});
           """.format(id=_id)
        assert_invalid( cursor, stmt, 'field user_number is not of type int')

        # let's check the rowcount and make sure the data
        # didn't get inserted when the exception asserted above was thrown
        stmt = """
              SELECT * FROM simple_table;
           """
        rows = cursor.execute(stmt)
        self.assertEqual(0, len(rows))

    @since('2.1')
    def test_nested_user_types(self):
        """Tests user types within user types"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_types', 2)

        stmt = """
              USE user_types
           """
        cursor.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        cursor.execute(stmt)

        # Create a user type to contain the item:
        stmt = """
              CREATE TYPE container (
              stuff text,
              more_stuff frozen<item>
              )
           """
        cursor.execute(stmt)

        #  Create a table that holds and item, a container, and a
        #  list of containers:
        stmt = """
              CREATE TABLE bucket (
               id uuid PRIMARY KEY,
               primary_item frozen<item>,
               other_items frozen<container>,
               other_containers list<frozen<container>>
              )
           """
        cursor.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        #  Insert some data:
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO bucket (id, primary_item)
              VALUES ({id}, {{sub_one: 'test', sub_two: 'test2'}});
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_items = {{stuff: 'stuff', more_stuff: {{sub_one: 'one', sub_two: 'two'}}}}
              WHERE id={id};
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff2', more_stuff: {{sub_one: 'one_other', sub_two: 'two_other'}}}}]
              WHERE id={id};
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff3', more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}}}, {{stuff: 'stuff4', more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}}}]
              WHERE id={id};
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              SELECT primary_item, other_items, other_containers from bucket where id={id};
           """.format(id=_id)
        rows = cursor.execute(stmt)

        primary_item, other_items, other_containers = rows[0]
        self.assertEqual(decode(primary_item), [[u'test', u'test2']])
        self.assertEqual(decode(other_items), [[u'stuff', [u'one', u'two']]])
        self.assertEqual(decode(other_containers), [[u'stuff2', [u'one_other', u'two_other']], [u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])

        #  Generate some repetitive data and check it for it's contents:
        for x in xrange(50):

            # Create row:
            _id = uuid.uuid4()
            stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff3', more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}}}, {{stuff: 'stuff4', more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}}}]
              WHERE id={id};
           """.format(id=_id)
            cursor.execute(stmt)

            time.sleep(0.1)

            # Check it:
            stmt = """
              SELECT other_containers from bucket WHERE id={id}
            """.format(id=_id)
            rows = cursor.execute(stmt)

            items = rows[0][0]
            self.assertEqual(decode(items), [[u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])

    @since('2.1')
    def test_type_as_part_of_pkey(self):
        """Tests user types as part of a composite pkey"""
        # make sure we can define a table with a user type as part of the pkey
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_type_pkeys', 2)

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid,
              name frozen<t_person_name>,
              like text,
              PRIMARY KEY ((id, name))
              )
           """
        cursor.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        _id = uuid.uuid4()

        stmt = """
              INSERT INTO person_likes (id, name, like)
              VALUES ({id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """.format(id=_id)
        cursor.execute(stmt)

        # attempt to query without the user type portion of the pkey and confirm there is an error
        stmt = """
              SELECT id, name.first from person_likes where id={id};
           """.format(id=_id)

        assert_invalid(cursor, stmt, 'Partition key part name must be restricted since preceding part is')

        stmt = """
              SELECT id, name.first, like from person_likes where id={id} and name = {{first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'}};
           """.format(id=_id)
        rows = cursor.execute(stmt)

        row_uuid, first_name, like = rows[0]
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')

    @since('2.1')
    def test_type_secondary_indexing(self):
        """
        Confirm that user types are secondary-indexable
        Similar procedure to TestSecondaryIndexesOnCollections.test_list_indexes
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_type_indexing', 2)

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid PRIMARY KEY,
              name frozen<t_person_name>,
              like text
              )
           """
        cursor.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        # no index present yet, make sure there's an error trying to query column
        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """

        assert_invalid(cursor, stmt, 'No indexed columns present in by-columns clause')

        # add index and query again (even though there are no rows in the table yet)
        stmt = """
              CREATE INDEX person_likes_name on person_likes (name);
            """
        cursor.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """
        rows = cursor.execute(stmt)
        self.assertEqual(0, len(rows))

        # add a row which doesn't specify data for the indexed column, and query again
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO person_likes (id, like)
              VALUES ({id}, 'long walks on the beach');
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Bob', middle: 'Testy', last: 'McTesterson'};
            """

        rows = cursor.execute(stmt)
        self.assertEqual(0, len(rows))

        # finally let's add a queryable row, and get it back using the index
        _id = uuid.uuid4()

        stmt = """
              INSERT INTO person_likes (id, name, like)
              VALUES ({id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              SELECT id, name.first, like from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
           """

        rows = cursor.execute(stmt)

        row_uuid, first_name, like = rows[0]

        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')

        # rename a field in the type and make sure the index still works
        stmt = """
            ALTER TYPE t_person_name rename first to first_name;
            """
        cursor.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """

        rows = cursor.execute(stmt)

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
        cursor.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Abraham', middle:'', last:'Lincoln'};
            """

        rows = cursor.execute(stmt)

        row_uuid, first_name, like = rows[0]

        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Abraham')
        self.assertEqual(like, u'preserving unions')

    @since('2.1')
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
        superuser_cursor = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        superuser_cursor.execute("create user ks1_user with password 'cassandra' nosuperuser;")
        superuser_cursor.execute("create user ks2_user with password 'cassandra' nosuperuser;")
        self.create_ks(superuser_cursor, 'ks1', 2)
        self.create_ks(superuser_cursor, 'ks2', 2)
        superuser_cursor.execute("grant all permissions on keyspace ks1 to ks1_user;")
        superuser_cursor.execute("grant all permissions on keyspace ks2 to ks2_user;")

        user1_cursor = self.patient_cql_connection(node1, user='ks1_user', password='cassandra')
        user2_cursor = self.patient_cql_connection(node1, user='ks2_user', password='cassandra')

        # first make sure the users can't create types in each other's ks
        assert_invalid(user1_cursor, "CREATE TYPE ks2.simple_type (user_number int, user_text text );", 'User ks1_user has no CREATE permission on <keyspace ks2> or any of its parents')

        assert_invalid(user2_cursor, "CREATE TYPE ks1.simple_type (user_number int, user_text text );", 'User ks2_user has no CREATE permission on <keyspace ks1> or any of its parents')

        # now, actually create the types in the correct keyspaces
        user1_cursor.execute("CREATE TYPE ks1.simple_type (user_number int, user_text text );")
        user2_cursor.execute("CREATE TYPE ks2.simple_type (user_number int, user_text text );")

        # each user now has a type belonging to their granted keyspace
        # let's make sure they can't drop each other's types (for which they have no permissions)

        assert_invalid(user1_cursor, "DROP TYPE ks2.simple_type;", 'User ks1_user has no DROP permission on <keyspace ks2> or any of its parents')

        assert_invalid(user2_cursor, "DROP TYPE ks1.simple_type;", 'User ks2_user has no DROP permission on <keyspace ks1> or any of its parents')

        # let's make sure they can't rename each other's types (for which they have no permissions)
        assert_invalid(user1_cursor, "ALTER TYPE ks2.simple_type RENAME user_number TO user_num;", 'User ks1_user has no ALTER permission on <keyspace ks2> or any of its parents')

        assert_invalid(user2_cursor, "ALTER TYPE ks1.simple_type RENAME user_number TO user_num;", 'User ks2_user has no ALTER permission on <keyspace ks1> or any of its parents')

        # rename the types using the correct user w/permissions to do so
        user1_cursor.execute("ALTER TYPE ks1.simple_type RENAME user_number TO user_num;")
        user2_cursor.execute("ALTER TYPE ks2.simple_type RENAME user_number TO user_num;")

        # finally, drop the types using the correct user w/permissions to do so
        user1_cursor.execute("DROP TYPE ks1.simple_type;")
        user2_cursor.execute("DROP TYPE ks2.simple_type;")

        #verify user type metadata is gone from the system schema
        rows = superuser_cursor.execute("SELECT * from system.schema_usertypes")
        self.assertEqual(0, len(rows))

    @since('2.1')
    def test_nulls_in_user_types(self):
        """Tests user types with null values"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_types', 2)

        stmt = """
              USE user_types
           """
        cursor.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        cursor.execute(stmt)

        # Create a table that holds an item
        stmt = """
              CREATE TABLE bucket (
               id int PRIMARY KEY,
               my_item frozen<item>,
              )
           """
        cursor.execute(stmt)
        # Make sure the schema propagates
        time.sleep(2)

        # Adds an explicit null
        cursor.execute("INSERT INTO bucket (id, my_item) VALUES (0, {sub_one: 'test', sub_two: null})")
        # Adds with an implicit null
        cursor.execute("INSERT INTO bucket (id, my_item) VALUES (1, {sub_one: 'test'})")

        rows = cursor.execute("SELECT my_item FROM bucket WHERE id=0")
        self.assertEqual(decode(rows[0]), [[u'test', None]])

        rows = cursor.execute("SELECT my_item FROM bucket WHERE id=1")
        self.assertEqual(decode(rows[0]), [[u'test', None]])

    @since('2.1')
    def test_no_counters_in_user_types(self):
        # CASSANDRA-7672
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_types', 1)

        stmt = """
            USE user_types
         """
        cursor.execute(stmt)

        stmt = """
            CREATE TYPE t_item (
            sub_one COUNTER )
         """

        assert_invalid(cursor, stmt, 'A user type cannot contain counters')

    @since('2.1')
    def test_type_as_clustering_col(self):
        """Tests user types as clustering column"""
        # make sure we can define a table with a user type as a clustering column
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'user_type_pkeys', 2)

        stmt = """
              CREATE TYPE t_letterpair (
              first text,
              second text
            )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE letters (
              id int,
              letterpair frozen<t_letterpair>,
              PRIMARY KEY (id, letterpair)
              )
           """
        cursor.execute(stmt)

        # create a bit of data and expect a natural order based on clustering user types

        ids = range(1, 10)

        for _id in ids:
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'a', second:'z'}})".format(_id))
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'z', second:'a'}})".format(_id))
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'f'}})".format(_id))
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'a'}})".format(_id))
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'c', second:'z'}})".format(_id))
            cursor.execute("INSERT INTO letters (id, letterpair) VALUES ({}, {{first:'d', second:'e'}})".format(_id))

        for _id in ids:
            res = cursor.execute("SELECT letterpair FROM letters where id = {}".format(_id))
            self.assertEqual(decode(res), [[u'a', u'z'], [u'c', u'a'], [u'c', u'f'], [u'c', u'z'], [u'd', u'e'], [u'z', u'a']])
