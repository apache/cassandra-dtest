from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os, shutil

@since('3.0')
class TestValidation(Tester):

    def prepare(self):
        cluster = self.cluster

        cluster.populate(3).start()

        return cluster

    def stress_write(self, node):
        node.stress(['write', 'n=1M', '-mode', 'native', 'cql3', '-rate', 'threads=10', '-pop', 'dist=UNIFORM(1..1M)'])

    def stress_read(self, node):
        # Verify the data
        tmpfile = tempfile.mktemp()
        with open(tmpfile, 'w+') as tmp:
            node.stress(['read', 'n=1M', '-mode', 'native', 'cql3', '-rate', 'threads=10', '-pop', 'dist=UNIFORM(1..1M)'],
                stdout=tmp, stderr=subprocess.STDOUT)
        return tmpfile

    def validate_stress_output(self, tmpfile, expect_failure=False, expect_errors=False):
        with open(tmpfile, 'r') as tmp:
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
        node.flush()
        path = os.path.join(node.get_path(), 'data', 'keyspace1')
        for folder in os.listdir(path):
            if folder.find('standard') >= 0:
                path = os.path.join(path, folder)
        for folder in os.listdir(path):
            if folder.find('Data') >= 0:
                os.remove(os.path.join(path, folder))

    def base_test(self):
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)

        self.stress_read(node2)

    def reverse_test(self):
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)
        node1.stop()
        tmpfile = self.stress_read(node2)
        self.validate_stress_output(tmpfile, expect_failure=True, expect_errors=True)

    def delete_data_test(self):
        self.ignore_log_patterns = ['CompactionExecutor',
            'java.lang.RuntimeException: Tried to hard link to file that does not exist']
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()
        self.stress_write(node1)

        self.lose_stress_data(node1)
        tmpfile = self.stress_read(node2)
        self.validate_stress_output(tmpfile, expect_failure=True, expect_errors=True)
