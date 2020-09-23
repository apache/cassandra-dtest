import pytest
import logging

from dtest import Tester
from tools.data import rows_to_list

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('3.0')
class TestStressSparsenessRatio(Tester):
    """
    @jira_ticket CASSANDRA-9522

    Tests for the `row-population-ratio` parameter to `cassandra-stress`.
    """

    def test_uniform_ratio(self):
        """
        Tests that the ratio-specifying string 'uniform(5..15)/50' results in
        ~80% of the values written being non-null.
        """
        self.distribution_template(ratio_spec='uniform(5..15)/50',
                                   expected_ratio=.8,
                                   delta=.1)

    def test_fixed_ratio(self):
        """
        Tests that the string 'fixed(1)/3' results in ~1/3 of the values
        written being non-null.
        """
        self.distribution_template(ratio_spec='fixed(1)/3',
                                   expected_ratio=1 - 1 / 3,
                                   delta=.01)

    def distribution_template(self, ratio_spec, expected_ratio, delta):
        """
        @param ratio_spec the string passed to `row-population-ratio` in the call to `cassandra-stress`
        @param expected_ratio the expected ratio of null/non-null values in the values written
        @param delta the acceptable delta between the expected and actual ratios

        A parameterized test for the `row-population-ratio` parameter to
        `cassandra-stress`.
        """
        self.cluster.populate(1).start()
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'n=1000', 'no-warmup', '-rate', 'threads=50', '-col', 'n=FIXED(50)',
                     '-insert', 'row-population-ratio={ratio_spec}'.format(ratio_spec=ratio_spec)])
        session = self.patient_cql_connection(node)
        written = rows_to_list(session.execute('SELECT * FROM keyspace1.standard1;'))

        num_nones = sum(row.count(None) for row in written)
        num_results = sum(len(row) for row in written)

        assert abs(float(num_nones) / num_results - expected_ratio) <= delta


@since('3.0')
class TestStressWrite(Tester):

    @pytest.mark.timeout(3 * 60)
    def test_quick_write(self):
        """
        @jira_ticket CASSANDRA-14890
        A simple write stress test should be done very quickly
        """
        self.cluster.populate(1).start()
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'err<0.9', 'n>1', '-rate', 'threads=1'])
        out, err, _ = node.run_cqlsh('describe table keyspace1.standard1')
        assert 'standard1' in out
