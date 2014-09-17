from dtest import Tester
from pyassertions import assert_invalid

import os, sys, time
from ccmlib.cluster import Cluster

class TestUDTEncoding(Tester):

    def udt_test(self):
        """ Test (somewhat indirectly) that user queries involving UDT's are properly encoded (due to driver not recognizing UDT syntax) """
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)

        # create udt and insert correctly (should be successful)
        cursor.execute('CREATE TYPE address (city text,zip int);')
        cursor.execute('CREATE TABLE user_profiles (login text PRIMARY KEY, addresses map<text, frozen<address>>);')
        cursor.execute("INSERT INTO user_profiles(login, addresses) VALUES ('tsmith', { 'home': {city: 'San Fransisco',zip: 94110 }});")

        #note here address looks likes a map -> which is what the driver thinks it is. udt is encoded server side, we test that if addresses is changed slightly whether encoder recognizes the errors

        # try adding a field - see if will be encoded to a udt (should return error)
        assert_invalid(cursor, "INSERT INTO user_profiles(login, addresses) VALUES ('jsmith', { 'home': {street: 'El Camino Real', city: 'San Fransisco', zip: 94110 }});", "Unknown field 'street' in value of user defined type address")

        # try modifying a field name - see if will be encoded to a udt (should return error)
        assert_invalid(cursor, "INSERT INTO user_profiles(login, addresses) VALUES ('fsmith', { 'home': {cityname: 'San Fransisco', zip: 94110 }});", "Unknown field 'cityname' in value of user defined type address")

        # try modifying a type within the collection - see if will be encoded to a udt (should return error)
        assert_invalid(cursor, "INSERT INTO user_profiles(login, addresses) VALUES ('fsmith', { 'home': {city: 'San Fransisco', zip: '94110' }});", "Invalid map literal for addresses")
