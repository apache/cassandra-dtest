import os
import datetime
import random
import struct
import time
import uuid
from cql import ProgrammingError
from dtest import Tester, debug
from tools import since


def decode_text(string):
    """
    decode bytestring as utf-8
    """
    return string.decode('utf-8')

def len_unpacker(val):
    return struct.Struct('>H').unpack(val)[0]

def unpack(bytestr):
    # The composite format for each component is:
    #   <len>   <value>   <eoc>
    # 2 bytes | ? bytes | 1 byte
    components = []
    while bytestr:
        length = len_unpacker(bytestr[:2])
        components.append(decode_text(bytestr[2:2 + length]))
        bytestr = bytestr[3 + length:]
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
        if item.startswith('\x00'):
            unpacked = unpack(item)
            decoded.append(decode(unpacked))
        else:
            decoded.append(item)
    
    return decoded


class TestUserTypes(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    @since('2.1')
    def test_type_renaming(self):
      """
      Confirm that types can be renamed and the proper associations are updated.
      """
      cluster = self.cluster
      cluster.populate(3).start()
      node1, node2, node3 = cluster.nodelist()
      cursor = self.patient_cql_connection(node1).cursor()
      self.create_ks(cursor, 'user_type_renaming', 2)

      stmt = """
            CREATE TYPE simple_type (
            user_number int
            )
         """
      cursor.execute(stmt)

      stmt = """
            CREATE TABLE simple_table (
            id uuid PRIMARY KEY,
            number simple_type
            )
         """
      cursor.execute(stmt)

      stmt = """
          ALTER TYPE simple_type rename to renamed_type;
         """
      cursor.execute(stmt)

      stmt = """
          SELECT type_name from system.schema_usertypes;
         """
      cursor.execute(stmt)
      # we should only have one user type in this test
      self.assertEqual(1, cursor.rowcount)

      # finally let's look for the new type name
      self.assertEqual(cursor.fetchone()[0], u'renamed_type')

    @since('2.1')
    def test_nested_type_renaming(self):
        """
        Confirm type renaming works as expected on nested types.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'nested_user_type_renaming', 2)

        stmt = """
              USE nested_user_type_renaming
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
              somefield simple_type
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE yet_another_type (
              some_other_field another_type
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE uses_nested_type (
              id uuid PRIMARY KEY,
              field_name yet_another_type
              )
           """
        cursor.execute(stmt)

        # let's insert some basic data using the nested types
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO uses_nested_type (id, field_name)
              VALUES (%s, {some_other_field: {somefield: {user_number: 1, user_text: 'original'}}});
           """ % _id
        cursor.execute(stmt)

        # rename one of the types used in the nesting
        stmt = """
              ALTER TYPE another_type rename to another_type2;
           """
        cursor.execute(stmt)

        # confirm nested data can be queried without error
        stmt = """
              SELECT field_name FROM uses_nested_type where id = {id}
           """.format(id=_id)
        cursor.execute(stmt)

        data = cursor.fetchone()[0]
        self.assertIn('original', data)

        # confirm we can alter/query the data after altering the type
        stmt = """
              UPDATE uses_nested_type
              SET field_name = {some_other_field: {somefield: {user_number: 2, user_text: 'altered'}}}
              WHERE id=%s;
           """ % _id
        cursor.execute(stmt)

        stmt = """
              SELECT field_name FROM uses_nested_type where id = {id}
           """.format(id=_id)
        cursor.execute(stmt)

        data = cursor.fetchone()[0]
        self.assertIn('altered', data)

        # and confirm we can add/query new data after the type rename
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO uses_nested_type (id, field_name)
              VALUES (%s, {some_other_field: {somefield: {user_number: 1, user_text: 'inserted'}}});
           """ % _id
        cursor.execute(stmt)

        stmt = """
              SELECT field_name FROM uses_nested_type where id = {id}
           """.format(id=_id)
        cursor.execute(stmt)

        data = cursor.fetchone()[0]
        self.assertIn('inserted', data)

    @since('2.1')
    def test_type_dropping(self):
        """
        Tests that a type cannot be dropped when in use, and otherwise can be dropped.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
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
              number simple_type
              )
           """
        cursor.execute(stmt)

        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 1}});
           """.format(id=_id)
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        with self.assertRaisesRegexp(ProgrammingError, 'Cannot drop user type user_type_dropping.simple_type as it is still used by table user_type_dropping.simple_table'):
            cursor.execute(stmt)

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
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

    @since('2.1')
    def test_nested_type_dropping(self):
        """
        Confirm a user type can't be dropped when being used by another user type. 
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
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
              somefield simple_type
              )
           """
        cursor.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        with self.assertRaisesRegexp(ProgrammingError, 'Cannot drop user type nested_user_type_dropping.simple_type as it is still used by user type another_type'):
            cursor.execute(stmt)

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
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

    @since('2.1')
    def test_type_enforcement(self):
        """
        Confirm error when incorrect data type used for user type
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.cql_connection(node1).cursor()
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
              number simple_type
              )
           """
        cursor.execute(stmt)

        # here we will attempt an insert statement which should fail
        # because the user type is an int, but the insert statement is
        # providing text
        _id = uuid.uuid4()
        stmt = """
              INSERT INTO simple_table (id, number)
              VALUES ({id}, {{user_number: 'uh oh....this is not a number'}});
           """.format(id=_id)
        with self.assertRaisesRegexp(ProgrammingError, 'field user_number is not of type int'):
            cursor.execute(stmt)

        # let's check the rowcount and make sure the data
        # didn't get inserted when the exception asserted above was thrown
        stmt = """
              SELECT * FROM simple_table;
           """
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

    @since('2.1')
    def test_nested_user_types(self):
        """Tests user types within user types"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'user_types', 2)

        stmt = """
              USE user_types
           """
        cursor.execute(stmt)

        #### Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        cursor.execute(stmt)

        #### Create a user type to contain the item:
        stmt = """
              CREATE TYPE container (
              stuff text,
              more_stuff item
              )
           """
        cursor.execute(stmt)

        ### Create a table that holds and item, a container, and a
        ### list of containers:
        stmt = """
              CREATE TABLE bucket (
               id uuid PRIMARY KEY,
               primary_item item,
               other_items container,
               other_containers list<container>
              )
           """
        cursor.execute(stmt)

        ### Insert some data:
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
        cursor.execute(stmt)
        
        primary_item, other_items, other_containers = cursor.fetchone()
        
        self.assertEqual(decode(primary_item), [[u'test', u'test2']])
        self.assertEqual(decode(other_items), [[u'stuff', [u'one', u'two']]])
        self.assertEqual(decode(other_containers), [[u'stuff2', [u'one_other', u'two_other']], [u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])
        
        ### Generate some repetitive data and check it for it's contents:
        for x in xrange(50):

            ### Create row:
            _id = uuid.uuid4()
            stmt = """
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff3', more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}}}, {{stuff: 'stuff4', more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}}}]
              WHERE id={id};
           """.format(id=_id)
            cursor.execute(stmt)
            
            time.sleep(0.1)
            
            ### Check it:
            stmt = """
              SELECT other_containers from bucket WHERE id={id}
            """.format(id=_id)
            cursor.execute(stmt)

            items = cursor.fetchone()[0]
            self.assertEqual(decode(items), [[u'stuff3', [u'one_2_other', u'two_2_other']], [u'stuff4', [u'one_3_other', u'two_3_other']]])

    @since('2.1')
    def test_type_as_part_of_pkey(self):
        """Tests user types as part of a composite pkey"""
        # make sure we can define a table with a user type as part of the pkey
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
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
              name t_person_name,
              like text,
              PRIMARY KEY ((id, name))
              )
           """
        cursor.execute(stmt)

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
        with self.assertRaisesRegexp(ProgrammingError, 'Partition key part name must be restricted since preceding part is'):
            cursor.execute(stmt)
            
        stmt = """
              SELECT id, name.first, like from person_likes where id={id} and name = {{first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'}};
           """.format(id=_id)
        cursor.execute(stmt)
        
        row_uuid, first_name, like = cursor.fetchone()
        
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
        node1,node2,node3 = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
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
              name t_person_name,
              like text
              )
           """
        cursor.execute(stmt)
        
        # no index present yet, make sure there's an error trying to query column
        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)
        
        # add index and query again (even though there are no rows in the table yet)
        stmt = """
              CREATE INDEX person_likes_name on person_likes (name);
            """
        cursor.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

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
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)
        
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
        cursor.execute(stmt)
        
        row_uuid, first_name, like = cursor.fetchone()
        
        self.assertEqual(str(row_uuid), str(_id))
        self.assertEqual(first_name, u'Nero')
        self.assertEqual(like, u'arson')