import os
import time

import binascii

from dtest import Tester
from ccmlib.node import Node
import struct

class TestCommitLogFailurePolicy(Tester):

    def test_bad_crc(self):
        """
        if the commit log header crc is doesn't the data, and the commit_failure_policy is stop, C* shouldn't startup
        """
        self.cluster.populate(nodes=1)
        node = self.cluster.nodelist()[0]
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop', 'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        self.create_ks(cursor, 'ks', 1)
        cursor.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = cursor.execute("SELECT * FROM ks.tbl")
        self.assertEqual(len(results), 10)

        time.sleep(1)

        node.stop(gently=False)

        # check that ks.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = path + '/data/ks'
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(ks_dir + '/' + db_dir) if f.endswith('.db')])
        self.assertEqual(sstables, 0)

        # modify the commit log crc values
        cl_dir = path + '/commitlogs'
        self.assertTrue(len(os.listdir(cl_dir)) > 0)
        for cl in os.listdir(cl_dir):
            # locate the CRC location
            with open(cl_dir + '/' + cl, 'r') as f:
                f.seek(0)
                version = struct.unpack('>i', f.read(4))[0]
                crc_pos = 12
                if version >= 5:
                    f.seek(crc_pos)
                    psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                    crc_pos += 2 + psize

            # rewrite it with crap
            with open(cl_dir + '/' + cl, 'w') as f:
                f.seek(crc_pos)
                f.write(struct.pack('>i', 123456))

            # verify said crap
            with open(cl_dir + '/' + cl, 'r') as f:
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]
                self.assertEqual(crc, 123456)

        # try starting normally
        try:
            node.start(wait_for_binary_proto=True)
        except:
            # ok
            return
        # if not, our data should still be there (demonstrates CASSANDRA-9749)
        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        results = cursor.execute("SELECT * FROM ks.tbl")
        self.assertEqual(len(results), 10)

    def test_compression_error(self):
        """
        if the commit log header refers to an unknown compression class, and the commit_failure_policy is stop, C* shouldn't startup
        """
        self.cluster.populate(nodes=1)
        node = self.cluster.nodelist()[0]
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop',
                                        'commitlog_compression': [{'class_name': 'LZ4Compressor'}],
                                        'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        self.create_ks(cursor, 'ks1', 1)
        cursor.execute("CREATE TABLE ks1.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks1.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = cursor.execute("SELECT * FROM ks1.tbl")
        self.assertEqual(len(results), 10)

        time.sleep(1)

        node.stop(gently=False)

        # check that ks1.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = path + '/data/ks1'
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(ks_dir + '/' + db_dir) if f.endswith('.db')])
        self.assertEqual(sstables, 0)

        def get_header_crc(header):
            """
            When calculating the header crc, C* splits up the 8b id, first adding the 4 least significant
            bytes to the crc, then the 5 most significant bytes, so this splits them and calculates the same way
            """
            new_header = header[:4]
            # C* evaluates most and least significant 4 bytes out of order
            new_header += header[8:12]
            new_header += header[4:8]
            # C* evaluates the short parameter length as an int
            new_header += '\x00\x00' + header[12:14]  # the
            new_header += header[14:]
            return binascii.crc32(new_header)

        # modify the compression parameters to look for a compressor that isn't there
        # while this scenario is pretty unlikely, if a jar or lib got moved or something,
        # you'd have a similar situation, which would be fixable by the user
        cl_dir = path + '/commitlogs'
        self.assertTrue(len(os.listdir(cl_dir)) > 0)
        for cl in os.listdir(cl_dir):
            # read the header and find the crc location
            with open(cl_dir + '/' + cl, 'r') as f:
                f.seek(0)
                version = struct.unpack('>i', f.read(4))[0]
                crc_pos = 12
                # if version < 5:
                #     # not applicable
                #     return
                f.seek(crc_pos)
                psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                crc_pos += 2 + psize

                header_length = crc_pos
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]

                # check that we're going this right
                f.seek(0)
                header_bytes = f.read(header_length)
                self.assertEqual(get_header_crc(header_bytes), crc)

            # rewrite it with imaginary compressor
            self.assertIn('LZ4Compressor', header_bytes)
            header_bytes = header_bytes.replace('LZ4Compressor', 'LZ5Compressor')
            self.assertNotIn('LZ4Compressor', header_bytes)
            self.assertIn('LZ5Compressor', header_bytes)
            with open(cl_dir + '/' + cl, 'w') as f:
                f.seek(0)
                f.write(header_bytes)
                f.seek(crc_pos)
                f.write(struct.pack('>i', get_header_crc(header_bytes)))

            # verify we wrote everything correctly
            with open(cl_dir + '/' + cl, 'r') as f:
                f.seek(0)
                self.assertEqual(f.read(header_length), header_bytes)
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]
                self.assertEqual(crc, get_header_crc(header_bytes))

        # try starting normally
        try:
            node.start(wait_for_binary_proto=True)
        except:
            # ok
            return
        # if not, our data should still be there (demonstrates CASSANDRA-9749)
        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        results = cursor.execute("SELECT * FROM ks1.tbl")
        self.assertEqual(len(results), 10)
