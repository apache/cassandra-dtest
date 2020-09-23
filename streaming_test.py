import logging
import operator

import pytest
from cassandra import ConsistencyLevel
from pytest import mark

from dtest import Tester, create_ks, create_cf
from tools.data import insert_c1c2
from tools.misc import generate_ssl_stores
from itertools import product

since = pytest.mark.since
logger = logging.getLogger(__name__)

opmap = {
    operator.eq: "==",
    operator.gt: ">",
    operator.lt: "<",
    operator.ne: "!=",
    operator.ge: ">=",
    operator.le: "<="
}


class TestStreaming(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        )

    def setup_internode_ssl(self, cluster):
        logger.debug("***using internode ssl***")
        generate_ssl_stores(self.fixture_dtest_setup.test_path)
        cluster.enable_internode_ssl(self.fixture_dtest_setup.test_path)

    def _test_streaming(self, op_zerocopy, op_partial, num_partial, num_zerocopy,
                        compaction_strategy='LeveledCompactionStrategy', num_keys=1000, rf=3, num_nodes=3, ssl=False):
        keys = num_keys
        cluster = self.cluster

        if ssl:
            self.setup_internode_ssl(cluster)

        tokens = cluster.balanced_tokens(num_nodes)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(num_nodes)
        nodes = cluster.nodelist()

        for i in range(0, len(nodes)):
            nodes[i].set_configuration_options(values={'initial_token': tokens[i]})

        cluster.start()

        session = self.patient_cql_connection(nodes[0])

        create_ks(session, name='ks2', rf=rf)

        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                  compaction_strategy=compaction_strategy)
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        session_n2 = self.patient_exclusive_cql_connection(nodes[1])
        session_n2.execute("TRUNCATE system.available_ranges;")

        mark = nodes[1].mark_log()
        nodes[1].nodetool('rebuild -ks ks2')

        nodes[1].watch_log_for('Completed submission of build tasks', filename='debug.log', timeout=120)
        zerocopy_streamed_sstable = len(
            nodes[1].grep_log('.*CassandraEntireSSTableStreamReader.*?Finished receiving Data.*', filename='debug.log',
                              from_mark=mark))
        partial_streamed_sstable = len(
            nodes[1].grep_log('.*CassandraStreamReader.*?Finished receiving file.*', filename='debug.log',
                              from_mark=mark))

        assert op_zerocopy(zerocopy_streamed_sstable, num_zerocopy), "%s %s %s" % (num_zerocopy, opmap.get(op_zerocopy),
                                                                                   zerocopy_streamed_sstable)
        assert op_partial(partial_streamed_sstable, num_partial), "%s %s %s" % (num_partial, op_partial,
                                                                                partial_streamed_sstable)

    @since('4.0')
    @pytest.mark.parametrize('ssl,compaction_strategy', product(['SSL', 'NoSSL'], ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy']))
    def test_zerocopy_streaming(self, ssl, compaction_strategy):
        self._test_streaming(op_zerocopy=operator.gt, op_partial=operator.gt, num_zerocopy=1, num_partial=1, rf=2,
                             num_nodes=3, ssl=(ssl == 'SSL'), compaction_strategy=compaction_strategy)

    @since('4.0')
    def test_zerocopy_streaming(self):
        self._test_streaming(op_zerocopy=operator.gt, op_partial=operator.eq, num_zerocopy=1, num_partial=0,
                             num_nodes=2, rf=2)

    @since('4.0')
    def test_zerocopy_streaming_no_replication(self):
        self._test_streaming(op_zerocopy=operator.eq, op_partial=operator.eq, num_zerocopy=0, num_partial=0, rf=1,
                             num_nodes=3)
