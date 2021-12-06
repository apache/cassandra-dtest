import pytest
import logging
import os
import subprocess
import tempfile

from dtest import Tester
from shutil import rmtree

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('4.0')
class TestFQLTool(Tester):
    """
    Makes sure fqltool replay and fqltool compare work
    @jira_ticket CASSANDRA-14690
    """
    def test_replay(self):
        """
        Generates a full query log, wipes the nodes and replays the
        query log, then makes sure that the data is correct.
        @jira_ticket CASSANDRA-14690
        """
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()

        with tempfile.TemporaryDirectory() as temp_dir:
            tmpdir = tempfile.mkdtemp(dir=temp_dir)
            tmpdir2 = tempfile.mkdtemp(dir=temp_dir)
            node1.nodetool("enablefullquerylog --path={}".format(tmpdir))
            node2.nodetool("enablefullquerylog --path={}".format(tmpdir2))
            node1.stress(['write', 'n=1000'])
            node1.flush()
            node2.flush()
            node1.nodetool("disablefullquerylog")
            node2.nodetool("disablefullquerylog")
            node1.stop(wait_other_notice=True)
            node2.stop(wait_other_notice=True)
            node1.clear()
            node2.clear()

            node1.start(wait_for_binary_proto=True)
            node2.start(wait_for_binary_proto=True)
            # make sure the node is empty:
            got_exception = False
            try:
                node1.stress(['read', 'n=1000'])
            except Exception:
                got_exception = True
            assert got_exception
            # replay the log files
            self._run_fqltool_replay(node1, [tmpdir, tmpdir2], "127.0.0.1", None, None, True)
            # and verify the data is there
            node1.stress(['read', 'n=1000'])

    def test_compare(self):
        """
        uses fqltool replay to compare two runs of the same query log and makes
        sure that the results match
        @jira_ticket CASSANDRA-14690
        """
        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        with tempfile.TemporaryDirectory() as temp_dir:
            results1 = tempfile.mkdtemp(dir=temp_dir)
            queries1 = tempfile.mkdtemp(dir=temp_dir)
            results2 = tempfile.mkdtemp(dir=temp_dir)
            queries2 = tempfile.mkdtemp(dir=temp_dir)
            fqldir = tempfile.mkdtemp(dir=temp_dir)

            node1.stress(['write', 'n=1000'])
            node1.flush()
            node1.nodetool("enablefullquerylog --path={}".format(fqldir))
            node1.stress(['read', 'n=1000'])
            node1.nodetool("disablefullquerylog")
            self._run_fqltool_replay(node1, [fqldir], "127.0.0.1", queries1, results1)
            self._run_fqltool_replay(node1, [fqldir], "127.0.0.1", queries2, results2)
            output = self._run_fqltool_compare(node1, queries1, [results1, results2])
            assert b"MISMATCH" not in output  # running the same reads against the same data

    def test_compare_mismatch(self):
        """
        generates two fql log files with different data (seq is different when running stress)
        then asserts that the replays of each generates a mismatch
        @jira_ticket CASSANDRA-14690
        """
        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        with tempfile.TemporaryDirectory() as temp_dir:
            fqldir1 = tempfile.mkdtemp(dir=temp_dir)
            fqldir2 = tempfile.mkdtemp(dir=temp_dir)
            results1 = tempfile.mkdtemp(dir=temp_dir)
            queries1 = tempfile.mkdtemp(dir=temp_dir)
            results2 = tempfile.mkdtemp(dir=temp_dir)
            queries2 = tempfile.mkdtemp(dir=temp_dir)

            node1.nodetool("enablefullquerylog --path={}".format(fqldir1))
            node1.stress(['write', 'n=1000'])
            node1.flush()
            node1.stress(['read', 'n=1000'])
            node1.nodetool("disablefullquerylog")

            node1.stop()
            for d in node1.data_directories():
                rmtree(d)
                os.mkdir(d)
            node1.start(wait_for_binary_proto=True)

            node1.nodetool("enablefullquerylog --path={}".format(fqldir2))
            node1.stress(['write', 'n=1000', '-pop', 'seq=1000..2000'])
            node1.flush()
            node1.stress(['read', 'n=1000', '-pop', 'seq=1000..2000'])
            node1.nodetool("disablefullquerylog")
            node1.stop()
            for d in node1.data_directories():
                rmtree(d)
                os.mkdir(d)
            node1.start(wait_for_binary_proto=True)

            self._run_fqltool_replay(node1, [fqldir1], "127.0.0.1", queries1, results1)
            node1.stop()
            for d in node1.data_directories():
                rmtree(d)
                os.mkdir(d)
            node1.start(wait_for_binary_proto=True)
            self._run_fqltool_replay(node1, [fqldir2], "127.0.0.1", queries2, results2)

            output = self._run_fqltool_compare(node1, queries1, [results1, results2])
            assert b"MISMATCH" in output  # compares two different stress runs, should mismatch

    def test_jvmdtest(self):
        """ mimics the behavior of the in-jvm dtest, see CASSANDRA-16720 """
        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)

        with tempfile.TemporaryDirectory() as temp_dir:
            tmpdir = tempfile.mkdtemp(dir=temp_dir)
            session.execute("CREATE KEYSPACE fql_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
            node1.nodetool("enablefullquerylog --path={}".format(tmpdir))
            session.execute("CREATE TABLE fql_ks.fql_table (id int primary key);")
            session.execute("INSERT INTO fql_ks.fql_table (id) VALUES (1)")
            node1.nodetool("disablefullquerylog")
            session.execute("DROP TABLE fql_ks.fql_table;")
            self._run_fqltool_replay(node1, [tmpdir], "127.0.0.1", None, None, replay_ddl_statements=False)

            got_exception = False
            try:
                session.execute("SELECT * FROM fql_ks.fql_table;")
            except Exception:
                got_exception = True
            assert got_exception

            self._run_fqltool_replay(node1, [tmpdir], "127.0.0.1", None, None, replay_ddl_statements=True)
            rs = session.execute("SELECT * FROM fql_ks.fql_table;")
            assert(len(list(rs)) == 1)

    def test_unclean_enable(self):
        """
        test that fql can be enabled on a dirty directory
        @jira_ticket CASSANDRA-17136
        """
        self.cluster.populate(1).start()
        node1 = self.cluster.nodelist()[0]

        with tempfile.TemporaryDirectory() as temp_dir:
            fqldir = tempfile.mkdtemp(dir=temp_dir)
            baddir = os.path.join(fqldir, 'baddir')
            os.mkdir(baddir)
            badfile = os.path.join(baddir, 'badfile')
            with open(os.path.join(badfile), 'w')  as f:
                    f.write('bad')
            os.chmod(badfile, 0o000)
            os.chmod(baddir, 0o000)
            node1.nodetool("enablefullquerylog --path={}".format(fqldir))
            # so teardown doesn't fail
            os.chmod(baddir, 0o777)

    def _run_fqltool_replay(self, node, logdirs, target, queries, results, replay_ddl_statements=False):
        fqltool = self.fqltool(node)
        args = [fqltool, "replay", "--target {}".format(target)]
        if queries is not None:
            args.append("--store-queries {}".format(queries))
        if results is not None:
            args.append("--results {}".format(results))
        if replay_ddl_statements is True:
            args.append("--replay-ddl-statements")
        args.extend(logdirs)
        rc = subprocess.call(args)
        assert rc == 0

    def _run_fqltool_compare(self, node, queries, results):
        fqltool = self.fqltool(node)
        args = [fqltool, "compare", "--queries {}".format(queries)]
        args.extend([os.path.join(r, "127.0.0.1") for r in results])
        logger.info(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        logger.info(stdout)
        return stdout

    def fqltool(self, node):
        cdir = node.get_install_dir()
        fqltool = os.path.join(cdir, 'tools', 'bin', 'fqltool')
        return fqltool
