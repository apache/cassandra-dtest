from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os

@since('2.2')
class TestValidation(Tester):

    __test__= False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def prepare(self):
        """
        Sets up cluster to test against. Currently 3 CCM Nodes
        """
        cluster = self.cluster

        cluster.populate(3).start()

        return cluster

    def stress_write(self, node):
        """
        Writes data via stress. Should write exact data expected by stress_read()
        """
        node.stress(['write', 'n=1M', '-mode', 'native', 'cql3', '-rate', 'threads=10',
            '-pop', 'dist=UNIFORM(1..1M)', '-schema', 'compaction(strategy=%s)' % self.strategy])

    def stress_read(self, node):
        """
        Reads previously written data via stress. Should check for exact data written
        by stress_write().
        """
        # Verify the data
        tmpfile = tempfile.mktemp()
        with open(tmpfile, 'w+') as tmp:
            node.stress(['read', 'n=1M', '-mode', 'native', 'cql3', '-rate', 'threads=10', '-pop', 'dist=UNIFORM(1..1M)'],
                stdout=tmp, stderr=subprocess.STDOUT)
        return tmpfile

    def validate_stress_output(self, outfile, expect_failure=False, expect_errors=False):
        """
        Validates if data was lost, or determines if stress encountered errors.
        Should be updated once stress has more sophisticated validation.

        outfile - an output file with stress output.
        expect_failure - if data loss should be expected
        expect_errors - if exceptions should be expected
        """
        with open(outfile, 'r') as tmp:
            output = tmp.read()

            debug(output)
            failure = output.find("Data returned was not validated")
            if expect_failure:
                assert failure >= 0, "No missing data detected, despite data loss"
            else:
                self.assertEqual(failure, -1, "Stress failed to validate all data")

            failure = output.find("Exception")
            if expect_errors:
                assert failure >= 0, "No errors detected, despite invalid cluster state"
            else:
                self.assertEqual(failure, -1, "Error while reading data")

    def lose_stress_data(self, node):
        """
        Simulate data loss.
        Currently works by deleting sstable files.
        """
        node.flush()
        path = os.path.join(node.get_path(), 'data', 'keyspace1')
        for folder in os.listdir(path):
            if folder.find('standard') >= 0:
                path = os.path.join(path, folder)
        for sstable_file in os.listdir(path):
            if sstable_file.find('Data') >= 0:
                with open(os.path.join(path, sstable_file), 'w') as data_file:
                    data_file.seek(0)
                    data_file.truncate()

    def simple_write_read_test(self):
        """
        A basic test that writes data at CL=ONE, RF=1
        Tests to ensure no data is lost or errors thrown.
        """
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)

        self.stress_read(node2)

    def down_node_test(self):
        """
        Removes a node and checks for data loss. Assumes RF=1.
        """
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)
        node1.stop()
        tmpfile = self.stress_read(node2)
        self.validate_stress_output(tmpfile, expect_failure=True, expect_errors=True)

    def delete_sstables_test(self):
        """
        Deletes sstables from a node and checks for data loss. Assumes RF=1.
        """
        self.ignore_log_patterns = ['CompactionExecutor',
            'java.lang.RuntimeException: Tried to hard link to file that does not exist']
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)

        self.lose_stress_data(node1)
        tmpfile = self.stress_read(node2)
        self.validate_stress_output(tmpfile, expect_failure=True, expect_errors=True)

strategies = {'LeveledCompactionStrategy' : 'LCS', 'SizeTieredCompactionStrategy' : 'STCS',
    'DateTieredCompactionStrategy' : 'DTCS'}

for strategy in strategies.keys():
    cls_name = ('TestValidation_with_' + strategies[strategy])
    vars()[cls_name] = type(cls_name, (TestValidation,), {'strategy': strategy, '__test__':True})
