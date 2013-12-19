from dtest import Tester, debug
from tools import since
import os
import datetime
import random
import uuid

class TestUserTypes(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    @since('2.1')
    def test_nested_user_types(self):
        """Tests user types within user types"""
        cluster = self.cluster
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'user_types', 2)
        
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
            
            ### Check it:
            stmt = """
              SELECT other_containers from bucket WHERE id={id}
            """.format(id=_id)
            cursor.execute(stmt)

            try:
                items = cursor.fetchone()[0]
            except TypeError:
                print stmt
                raise
            print items
            self.assertEqual(len(items), 2)
            # Item 1:
            self.assertIn('stuff3', items[0])
            self.assertIn('one_2_other', items[0])
            self.assertIn('two_2_other', items[0])
            # Item 2:
            self.assertIn('stuff4', items[1])
            self.assertIn('one_3_other', items[1])
            self.assertIn('two_3_other', items[1])
            
