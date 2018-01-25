import pytest
import logging

from distutils.version import LooseVersion

from cassandra import ConsistencyLevel
from ccmlib.common import is_win

from dtest import Tester, create_ks, create_cf
from tools.assertions import assert_length_equal
from tools.data import insert_c1c2
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)

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
        remove_perf_disable_shared_mem(node1)
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
        line = node1.grep_log(("Starting repair command #1" + (" \([^\)]+\)" if cluster.version() >= LooseVersion("3.10") else "") +
                               ", repairing keyspace ks with repair options \(parallelism: (?P<parallelism>\w+), primary range: (?P<pr>\w+), "
                               "incremental: (?P<incremental>\w+), job threads: (?P<jobs>\d+), ColumnFamilies: (?P<cfs>.+), dataCenters: (?P<dc>.+), "
                               "hosts: (?P<hosts>.+), # of ranges: (?P<ranges>\d+)(, pull repair: (?P<pullrepair>true|false))?\)"))

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
