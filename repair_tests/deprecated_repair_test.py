import pytest
import logging
import os
import subprocess

from distutils.version import LooseVersion

from cassandra import ConsistencyLevel
from ccmlib.common import is_win
from ccmlib.node import handle_external_tool_process, ToolError
import ccmlib.repository

from dtest import Tester, create_ks, create_cf
from tools.assertions import assert_length_equal
from tools.data import insert_c1c2
from tools.jmxutils import (JolokiaAgent, make_mbean)

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since("2.2", max_version="4")
class TestDeprecatedRepairAPI(Tester):
    """
    @jira_ticket CASSANDRA-9570

    Test if deprecated repair JMX API runs with expected parameters
    """

    def test_force_repair_async_1(self):
        """
        test forceRepairAsync(String keyspace, boolean isSequential,
                              Collection<String> dataCenters,
                              Collection<String> hosts,
                              boolean primaryRange, boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,boolean,java.util.Collection,java.util.Collection,boolean,boolean,[Ljava.lang.String;)",
                                          ['ks', True, [], [], False, False, ["cf"]])
        assert opt["parallelism"], "parallel" if is_win() else "sequential" == opt
        assert opt["primary_range"], "false" == opt
        assert opt["incremental"], "true" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["column_families"], "[cf]" == opt

    def test_force_repair_async_2(self):
        """
        test forceRepairAsync(String keyspace, int parallelismDegree,
                              Collection<String> dataCenters,
                              Collection<String> hosts,
                              boolean primaryRange, boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,int,java.util.Collection,java.util.Collection,boolean,boolean,[Ljava.lang.String;)",
                                          ['ks', 1, [], [], True, True, []])
        assert opt["parallelism"], "parallel" == opt
        assert opt["primary_range"], "true" == opt
        assert opt["incremental"], "false" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["column_families"], "[]" == opt

    def test_force_repair_async_3(self):
        """
        test forceRepairAsync(String keyspace, boolean isSequential,
                              boolean isLocal, boolean primaryRange,
                              boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,boolean,boolean,boolean,boolean,[Ljava.lang.String;)",
                                          ['ks', False, False, False, False, ["cf"]])
        assert opt["parallelism"], "parallel" == opt
        assert opt["primary_range"], "false" == opt
        assert opt["incremental"], "true" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["column_families"], "[cf]" == opt

    def test_force_repair_range_async_1(self):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, boolean isSequential,
                                   Collection<String> dataCenters,
                                   Collection<String> hosts, boolean fullRepair,
                                   String... columnFamilies)
        """
        # when giving token ranges, there needs to be logic to make sure we have partitions for
        # those tokens... which self._deprecated_repair_jmx does not do... for this reason most
        # runs will not actually trigger repair and will abort (we check logging, which will happen
        # still).
        self.fixture_dtest_setup.ignore_log_patterns = [
            'Nothing to repair for'
        ]
        opt = self._deprecated_repair_jmx("forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,boolean,java.util.Collection,java.util.Collection,boolean,[Ljava.lang.String;)",
                                          ["0", "1000", "ks", True, ["dc1"], [], False, ["cf"]])
        assert opt["parallelism"], "parallel" if is_win() else "sequential" == opt
        assert opt["primary_range"], "false" == opt
        assert opt["incremental"], "true" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[dc1]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["ranges"], "1" == opt
        assert opt["column_families"], "[cf]" == opt

    def test_force_repair_range_async_2(self):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, int parallelismDegree,
                                   Collection<String> dataCenters,
                                   Collection<String> hosts,
                                   boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,int,java.util.Collection,java.util.Collection,boolean,[Ljava.lang.String;)",
                                          ["0", "1000", "ks", 2, [], [], True, ["cf"]])
        assert opt["parallelism"], "parallel" if is_win() else "dc_parallel" == opt
        assert opt["primary_range"], "false" == opt
        assert opt["incremental"], "false" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["ranges"], "1" == opt
        assert opt["column_families"], "[cf]" == opt

    def test_force_repair_range_async_3(self):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, boolean isSequential,
                                   boolean isLocal, boolean fullRepair,
                                   String... columnFamilies)
        """
        # when giving token ranges, there needs to be logic to make sure we have partitions for
        # those tokens... which self._deprecated_repair_jmx does not do... for this reason most
        # runs will not actually trigger repair and will abort (we check logging, which will happen
        # still).
        self.fixture_dtest_setup.ignore_log_patterns = [
            'Nothing to repair for'
        ]
        opt = self._deprecated_repair_jmx("forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,boolean,boolean,boolean,[Ljava.lang.String;)",
                                          ["0", "1000", "ks", True, True, True, ["cf"]])
        assert opt["parallelism"], "parallel" if is_win() else "sequential" == opt
        assert opt["primary_range"], "false" == opt
        assert opt["incremental"], "false" == opt
        assert opt["job_threads"], "1" == opt
        assert opt["data_centers"], "[dc1]" == opt
        assert opt["hosts"], "[]" == opt
        assert opt["ranges"], "1" == opt
        assert opt["column_families"], "[cf]" == opt

    def _deprecated_repair_jmx(self, method, arguments):
        """
        * Launch a two node, two DC cluster
        * Create a keyspace and table
        * Insert some data
        * Call the deprecated repair JMX API based on the arguments passed into this method
        * Check the node log to see if the correct repair was performed based on the jmx args
        """
        cluster = self.cluster

        logger.debug("Starting cluster..")
        cluster.populate([1, 1])
        node1, node2 = cluster.nodelist()
        cluster.start()
        supports_pull_repair = cluster.version() >= LooseVersion('3.10')

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)

        # Run repair
        mbean = make_mbean('db', 'StorageService')
        with JolokiaAgent(node1) as jmx:
            # assert repair runs and returns valid cmd number
            assert jmx.execute_method(mbean, method, arguments) == 1
        # wait for log to start
        node1.watch_log_for("Starting repair command")
        # get repair parameters from the log
        line = node1.grep_log((r"Starting repair command #1" + (r" \([^\)]+\)" if cluster.version() >= LooseVersion("3.10") else "") +
                               r", repairing keyspace ks with repair options \(parallelism: (?P<parallelism>\w+), primary range: (?P<pr>\w+), "
                               r"incremental: (?P<incremental>\w+), job threads: (?P<jobs>\d+), ColumnFamilies: (?P<cfs>.+), dataCenters: (?P<dc>.+), "
                               r"hosts: (?P<hosts>.+), # of ranges: (?P<ranges>\d+)(, pull repair: (?P<pullrepair>true|false))?(, ignore unreplicated keyspaces: (?P<ignoreunrepl>true|false))?\)"))

        assert_length_equal(line, 1)
        line, m = line[0]

        if supports_pull_repair:
            assert m.group("pullrepair"), "false" == "Pull repair cannot be enabled through the deprecated API so the pull repair option should always be false."

        return {"parallelism": m.group("parallelism"),
                "primary_range": m.group("pr"),
                "incremental": m.group("incremental"),
                "job_threads": m.group("jobs"),
                "column_families": m.group("cfs"),
                "data_centers": m.group("dc"),
                "hosts": m.group("hosts"),
                "ranges": m.group("ranges")}


@since("3.0.16", max_version="4")
class TestDeprecatedRepairNotifications(Tester):
    """
    * @jira_ticket CASSANDRA-13121
    * Test if legacy JMX detects failures in repair jobs launched with the deprecated API.
    * Affects cassandra-3.x clusters when users run JMX from cassandra-2.1 and older to submit repair jobs.
    """

    def get_legacy_environment(self, legacy_version, node_env=None):
        """
        * Set up an environment to run nodetool from cassandra-2.1.
        """
        env = {}
        if (node_env is not None):
            env = node_env
        legacy_dirpath = ccmlib.repository.directory_name(legacy_version)
        env["CASSANDRA_HOME"] = legacy_dirpath
        binpaths = [legacy_dirpath,
                    os.path.join(legacy_dirpath, "build", "classes", "main"),
                    os.path.join(legacy_dirpath, "build", "classes", "thrift")]
        env["cassandra_bin"] = ":".join(binpaths)
        env["CASSANDRA_CONF"] = os.path.join(legacy_dirpath, "conf")
        classpaths = [env["CASSANDRA_CONF"], env["cassandra_bin"]]
        for jar in os.listdir(os.path.join(legacy_dirpath, "lib")):
            if (jar.endswith(".jar")):
                classpaths.append(os.path.join(legacy_dirpath, "lib", jar))
        env['CLASSPATH'] = ":".join(classpaths)
        return env

    def test_deprecated_repair_error_notification(self):
        """
        * Check whether a legacy JMX nodetool understands the
        * notification for a failed repair job.
        """
        # This test intentionally provokes an error in a repair job
        self.fixture_dtest_setup.ignore_log_patterns = [r'Repair failed', r'The current host must be part of the repair']

        # start a 2-node cluster
        logger.debug("Starting cluster...")
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.start()

        # write some data that could be repaired
        logger.debug("Stressing node1...")
        node1.stress(stress_options=['write', 'n=5000', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=2)', '-rate', 'threads=5'])

        # set up a legacy repository
        logger.debug("Setting up legacy repository...")
        legacy_version = 'github:apache/cassandra-2.1'
        ccmlib.repository.setup(legacy_version)

        # Run repair with legacy nodetool.
        # The options specified will cause an error, and legacy nodetool should error out.
        logger.debug("Running repair on node1 using legacy nodetool (using options that will cause failure with error)")
        legacy_dirpath = ccmlib.repository.directory_name(legacy_version)
        legacy_nodetool_path = os.path.join(legacy_dirpath, "bin", "nodetool")
        repair_env = self.get_legacy_environment(legacy_version, node_env=node1.get_env())
        repair_args = [legacy_nodetool_path, "-h", "localhost", "-p", str(node1.jmx_port), "repair", "-hosts", "127.0.0.2"]
        p = subprocess.Popen(repair_args, env=repair_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        nodetool_stderr = None
        nodetool_returncode = None
        try:
            _, nodetool_stderr, _ = handle_external_tool_process(p, repair_args)
        except ToolError as tool_error:
            nodetool_stderr = tool_error.stderr

        # Check for repair failed message in node1 log
        repair_failed_logs = node1.grep_log(r"ERROR \[(Repair-Task|Thread)-\d+\] \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} RepairRunnable.java:\d+ - Repair failed")
        assert len(repair_failed_logs) > 0, "Node logs don't have an error message for the failed repair"
        # Check for error and stacktrace in nodetool output
        assert nodetool_stderr.find("error") > -1, "Legacy nodetool didn't print an error message for the failed repair"
        assert nodetool_stderr.find("-- StackTrace --") > -1, "Legacy nodetool didn't print a stack trace for the failed repair"
