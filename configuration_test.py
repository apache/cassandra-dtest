from dtest import Tester

import re, ast
from ccmlib.cluster import Cluster

class TestConfiguration(Tester):

    def compression_chunk_length_test(self):
        """ Verify the setting of compression chunk_length [#3558]"""
        cluster = self.cluster

        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        cursor = self.patient_cql_connection(node)
        self.create_ks(cursor, 'ks', 1)

        create_table_query = "CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name));"
        alter_chunk_len_query = "ALTER TABLE test_table WITH compression = {{'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : {chunk_length}}};"

        cursor.execute( create_table_query)

        cursor.execute( alter_chunk_len_query.format(chunk_length=32) )
        self._check_chunk_length( cursor, 32 )

        cursor.execute( alter_chunk_len_query.format(chunk_length=64) )
        self._check_chunk_length( cursor, 64 )


    def _check_chunk_length(self, cursor, value):
        describe_table_query = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='ks' AND columnfamily_name='test_table';"
        rows = cursor.execute( describe_table_query )
        results = rows[0]
        #Now extract the param list
        params = ''
        for result in results:
            if 'sstable_compression' in str(result):
                params = result

        assert params is not '', "Looking for a row with the string 'sstable_compression' in system.schema_columnfamilies, but could not find it."

        params = ast.literal_eval( params )
        chunk_length = int( params['chunk_length_kb'] )

        assert chunk_length == value, "Expected chunk_length: %s.  We got: %s" % (value, chunk_length)
