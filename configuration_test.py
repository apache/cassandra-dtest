from dtest import Tester
from assertions import *

import re
from ccmlib.cluster import Cluster

class TestConfiguration(Tester):

    def compression_chunk_length_test(self):
        """ Verify the setting of compression chunk_length [#3558]"""
        cluster = self.cluster

        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        cli = node.cli()
        cli.do("create keyspace ks")
        cli.do("use ks")
        cli.do("create column family cf with compression_options={sstable_compression:SnappyCompressor, chunk_length_kb:32}")
        self._check_chunk_length(cli, 32)

        cli.do("update column family cf with compression_options={sstable_compression:SnappyCompressor, chunk_length_kb:64}")
        self._check_chunk_length(cli, 64)

        cli.close()

    def _check_chunk_length(self, cli, value):
        cli.do("describe cf")
        assert not cli.has_errors()

        output = cli.last_output()
        m = re.search(r'chunk_length_kb:\s*(\d+)', output)
        assert m is not None, 'cannot find chunk_length_kb in ' + output
        assert m.group(1) == str(value), 'expecting chunk length of ' + str(value) + ', got ' + m.group(1)
