from __future__ import with_statement
import os, tempfile, sys, shutil, subprocess, types, time, threading, ConfigParser, logging
import fnmatch
import re
import copy

from ccmlib.cluster import Cluster
from ccmlib.node import Node
from cql.thrifteries import ThriftCursor
from uuid import UUID
from nose.exc import SkipTest
from thrift.transport import TSocket
from unittest import TestCase
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import Cluster as PyCluster

LOG_SAVED_DIR="logs"
try:
    os.mkdir(LOG_SAVED_DIR)
except OSError:
    pass

LAST_LOG = os.path.join(LOG_SAVED_DIR, "last")

LAST_TEST_DIR='last_test_dir'

DEFAULT_DIR='./'
config = ConfigParser.RawConfigParser()
if len(config.read(os.path.expanduser('~/.cassandra-dtest'))) > 0:
    if config.has_option('main', 'default_dir'):
        DEFAULT_DIR=os.path.expanduser(config.get('main', 'default_dir'))

NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
DEBUG = os.environ.get('DEBUG', '').lower() in ('yes', 'true')
TRACE = os.environ.get('TRACE', '').lower() in ('yes', 'true')
KEEP_LOGS = os.environ.get('KEEP_LOGS', '').lower() in ('yes', 'true')
KEEP_TEST_DIR = os.environ.get('KEEP_TEST_DIR', '').lower() in ('yes', 'true')
PRINT_DEBUG = os.environ.get('PRINT_DEBUG', '').lower() in ('yes', 'true')
DISABLE_VNODES = os.environ.get('DISABLE_VNODES', '').lower() in ('yes', 'true')
OFFHEAP_MEMTABLES = os.environ.get('OFFHEAP_MEMTABLES', '').lower() in ('yes', 'true')
NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
RECORD_COVERAGE = os.environ.get('RECORD_COVERAGE', '').lower() in ('yes', 'true')


CURRENT_TEST = ""

logging.basicConfig(filename=os.path.join(LOG_SAVED_DIR,"dtest.log"),
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(current_test)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

LOG = logging.getLogger('dtest')

# copy the initial environment variables so we can reset them later:
initial_environment = copy.deepcopy(os.environ)
def reset_environment_vars():
    os.environ.clear()
    os.environ.update(initial_environment)

def debug(msg):
    LOG.debug(msg, extra={"current_test":CURRENT_TEST})
    if PRINT_DEBUG:
        print msg

def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)

def is_win():
    return True if sys.platform == "cygwin" or sys.platform == "win32" else False

class Tester(TestCase):

    def __init__(self, *argv, **kwargs):
        # if False, then scan the log of each node for errors after every test.
        self.allow_log_errors = False
        self.cluster_options = kwargs.pop('cluster_options', None)
        super(Tester, self).__init__(*argv, **kwargs)

    def _get_cluster(self, name='test'):
        self.test_path = tempfile.mkdtemp(prefix='dtest-')
        # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
        # regular Windows space on wmic calls which will otherwise break pathing
        if sys.platform == "cygwin":
            self.test_path = subprocess.Popen(["cygpath", "-m", self.test_path], stdout = subprocess.PIPE, stderr = subprocess.STDOUT).communicate()[0].rstrip()
        debug("cluster ccm directory: "+self.test_path)
        version = os.environ.get('CASSANDRA_VERSION')
        cdir = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)
        
        if version:
            cluster = Cluster(self.test_path, name, cassandra_version=version)
        else:
            cluster = Cluster(self.test_path, name, cassandra_dir=cdir)
        
        if cluster.version() >= "1.2":
            if DISABLE_VNODES:
                cluster.set_configuration_options(values={'num_tokens': None})
            else:
                cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})

        if cluster.version() >= "2.1":
            if OFFHEAP_MEMTABLES:
                cluster.set_configuration_options(values={'memtable_allocation_type': 'offheap_objects'})

        return cluster

    def _cleanup_cluster(self):
        if KEEP_TEST_DIR:
            self.cluster.stop(gently=RECORD_COVERAGE)
        else:
            # when recording coverage the jvm has to exit normally
            # or the coverage information is not written by the jacoco agent
            # otherwise we can just kill the process
            if RECORD_COVERAGE:
                self.cluster.stop(gently=True)

            # Cleanup everything:
            debug("removing ccm cluster " + self.cluster.name + " at: " + self.test_path)
            self.cluster.remove()
            os.rmdir(self.test_path)
        if os.path.exists(LAST_TEST_DIR):
            os.remove(LAST_TEST_DIR)

    def set_node_to_current_version(self, node):
        version = os.environ.get('CASSANDRA_VERSION')
        cdir = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)
        
        if version:
            node.set_cassandra_dir(cassandra_version=version)
        else:
            node.set_cassandra_dir(cassandra_dir=cdir)
        
    def setUp(self):
        global CURRENT_TEST
        CURRENT_TEST = self.id() + self._testMethodName
        # cleaning up if a previous execution didn't trigger tearDown (which
        # can happen if it is interrupted by KeyboardInterrupt)
        # TODO: move that part to a generic fixture
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                self.test_path = f.readline().strip('\n')
                name = f.readline()
                try:
                    self.cluster = Cluster.load(self.test_path, name)
                    # Avoid waiting too long for node to be marked down
                    self._cleanup_cluster()
                except IOError:
                    # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
                    pass

        self.cluster = self._get_cluster()
        if RECORD_COVERAGE:
            self.__setup_jacoco()
        # the failure detector can be quite slow in such tests with quick start/stop
        self.cluster.set_configuration_options(values={'phi_convict_threshold': 5})

        timeout = 10000
        if self.cluster_options is not None:
            self.cluster.set_configuration_options(values=self.cluster_options)
        elif self.cluster.version() < "1.2":
            self.cluster.set_configuration_options(values={'rpc_timeout_in_ms': timeout})
        else:
            self.cluster.set_configuration_options(values={
                'read_request_timeout_in_ms' : timeout,
                'range_request_timeout_in_ms' : timeout,
                'write_request_timeout_in_ms' : timeout,
                'truncate_request_timeout_in_ms' : timeout,
                'request_timeout_in_ms' : timeout
            })

        with open(LAST_TEST_DIR, 'w') as f:
            f.write(self.test_path + '\n')
            f.write(self.cluster.name)
        if DEBUG:
            self.cluster.set_log_level("DEBUG")
        if TRACE:
            self.cluster.set_log_level("TRACE")
        self.connections = []
        self.runners = []

    @classmethod
    def tearDownClass(cls):
        reset_environment_vars()

    def tearDown(self):
        reset_environment_vars()

        for con in self.connections:
            con.close()

        for runner in self.runners:
            try:
                runner.stop()
            except:
                pass

        failed = sys.exc_info() != (None, None, None)
        try:
            for node in self.cluster.nodelist():
                if self.allow_log_errors == False:
                    errors = list(self.__filter_errors([ msg for msg, i in node.grep_log("ERROR")]))
                    if len(errors) is not 0:
                        failed = True
                        raise AssertionError('Unexpected error in %s node log: %s' % (node.name, errors))
        finally:
            try:
                if failed or KEEP_LOGS:
                    # means the test failed. Save the logs for inspection.
                    self.copy_logs()
            except Exception as e:
                    print "Error saving log:", str(e)
            finally:
                self._cleanup_cluster()

    def copy_logs(self, directory=None, name=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        if directory is None:
            directory = LOG_SAVED_DIR
        if name is None:
            name = LAST_LOG
        else:
            name = os.path.join(directory, name)
        if not os.path.exists(directory):
            os.mkdir(directory)
        logs = [ (node.name, node.logfilename()) for node in self.cluster.nodes.values() ]
        if len(logs) is not 0:
            basedir = str(int(time.time() * 1000)) + '_' + self.id()
            logdir = os.path.join(directory, basedir)
            os.mkdir(logdir)
            for n, log in logs:
                shutil.copyfile(log, os.path.join(logdir, n + ".log"))
            if os.path.exists(name):
                os.unlink(name)
            os.symlink(basedir, name)

    def cql_connection(self, node, keyspace=None, version=None, user=None, password=None):
        import cql
        host, port = node.network_interfaces['thrift']
        if not version and self.cluster.version() >= "1.2":
            version = "3.0.0"
        elif not version and self.cluster.version() >= "1.1":
            version = "2.0.0"

        if version:
            con = cql.connect(host, port, keyspace=keyspace, cql_version=version, user=user, password=password)
        else:
            con = cql.connect(host, port, keyspace=keyspace, user=user, password=password)
        self.connections.append(con)
        return con

    def patient_cql_connection(self, node, keyspace=None, version=None, user=None, password=None, timeout=10):
        """
        Returns a connection after it stops throwing TTransportExceptions due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout = timeout * 5

        return retry_till_success(
            self.cql_connection,
            node,
            keyspace=keyspace,
            version=version,
            user=user,
            password=password,
            timeout=timeout,
            bypassed_exception=TSocket.TTransportException
        )

    def create_ks(self, cursor, name, rf):
        if self.cluster.version() >= "1.2" and cursor.cql_major_version >= 3:
            query = 'CREATE KEYSPACE %s WITH replication={%s}'
            if isinstance(rf, types.IntType):
                # we assume simpleStrategy
                cursor.execute(query % (name, "'class':'SimpleStrategy', 'replication_factor':%d" % rf))
            else:
                assert len(rf) != 0, "At least one datacenter/rf pair is needed"
                # we assume networkTopolyStrategy
                options = (', ').join([ '\'%s\':%d' % (d, r) for d, r in rf.iteritems() ])
                cursor.execute(query % (name, "'class':'NetworkTopologyStrategy', %s" % options))
        else:
            query = 'CREATE KEYSPACE %s WITH strategy_class=%s AND %s'
            if isinstance(rf, types.IntType):
                # we assume simpleStrategy
                cursor.execute(query % (name, 'SimpleStrategy', 'strategy_options:replication_factor=%d' % rf))
            else:
                assert len(rf) != 0, "At least one datacenter/rf pair is needed"
                # we assume networkTopolyStrategy
                options = (' AND ').join([ 'strategy_options:%s=%d' % (d, r) for d, r in rf.iteritems() ])
                cursor.execute(query % (name, 'NetworkTopologyStrategy', options))
        cursor.execute('USE %s' % name)

    # We default to UTF8Type because it's simpler to use in tests
    def create_cf(self, cursor, name, key_type="varchar", speculative_retry=None, read_repair=None, compression=None, gc_grace=None, columns=None, validation="UTF8Type"):
        additional_columns = ""
        if columns is not None:
            for k, v in columns.items():
                additional_columns = "%s, %s %s" % (additional_columns, k, v)

        if self.cluster.version() >= "1.2":
            if additional_columns == "":
                query = 'CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'' % (name, key_type)
            else:
                query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'' % (name, key_type, additional_columns)
            if compression is not None:
                query = '%s AND compression = { \'sstable_compression\': \'%sCompressor\' }' % (query, compression)
        else:
            query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comparator=UTF8Type AND default_validation=%s' % (name, key_type, additional_columns, validation)
            if compression is not None:
                query = '%s AND compression_parameters:sstable_compression=%sCompressor' % (query, compression)

        if read_repair is not None:
            query = '%s AND read_repair_chance=%f' % (query, read_repair)
        if gc_grace is not None:
            query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
        if self.cluster.version() >= "2.0":
            if speculative_retry is not None:
                query = '%s AND speculative_retry=\'%s\'' % (query, speculative_retry)

        cursor.execute(query)
        time.sleep(0.2)

    def go(self, func):
        runner = Runner(func)
        self.runners.append(runner)
        runner.start()
        return runner

    def skip(self, msg):
        if not NO_SKIP:
            raise SkipTest(msg)
    
    def __setup_jacoco(self, cluster_name='test'):
        """Setup JaCoCo code coverage support"""
        # use explicit agent and execfile locations
        # or look for a cassandra build if they are not specified
        cdir = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)

        agent_location = os.environ.get('JACOCO_AGENT_JAR', os.path.join(cdir, 'build/lib/jars/jacocoagent.jar'))
        jacoco_execfile = os.environ.get('JACOCO_EXECFILE', os.path.join(cdir, 'build/jacoco/jacoco.exec'))

        if os.path.isfile(agent_location):
            debug("Jacoco agent found at {}".format(agent_location))
            with open(os.path.join(
                    self.test_path, cluster_name, 'cassandra.in.sh'),'w') as f:

                f.write('JVM_OPTS="$JVM_OPTS -javaagent:{jar_path}=destfile={exec_file}"'\
                    .format(jar_path=agent_location, exec_file=jacoco_execfile))
                
                if os.path.isfile(jacoco_execfile):
                    debug("Jacoco execfile found at {}, execution data will be appended".format(jacoco_execfile))
                else:
                    debug("Jacoco execfile will be created at {}".format(jacoco_execfile))
        else:
            debug("Jacoco agent not found or is not file. Execution will not be recorded.")

    def __filter_errors(self, errors):
        """Filter errors, removing those that match self.ignore_log_patterns"""
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        for e in errors:
            for pattern in self.ignore_log_patterns:
                if re.search(pattern, e):
                    break
            else:
                yield e

    # Disable docstrings printing in nosetest output
    def shortDescription(self):
        return None

class Runner(threading.Thread):
    def __init__(self, func):
        threading.Thread.__init__(self)
        self.__func = func
        self.__error = None
        self.__stopped = False
        self.daemon = True

    def run(self):
        i = 0
        while True:
            if self.__stopped:
                return
            try:
                self.__func(i)
            except Exception as e:
                self.__error = e
                return
            i = i + 1

    def stop(self):
        self.__stopped = True
        self.join()
        if self.__error is not None:
            raise self.__error

    def check(self):
        if self.__error is not None:
            raise self.__error


class PyTester(Tester):

    def __init__(self, *argv, **kwargs):
        Tester.__init__(self, *argv, **kwargs)

    def cql_connection(self, node, keyspace=None, version=None, user=None, password=None, compression=True):

        node_ip =  node.network_interfaces['binary'][0]
        cluster = PyCluster([node_ip], compression=compression)
        session = cluster.connect()
        if keyspace is not None:
            session.execute('USE %s' % keyspace)

        self.connections.append(session)
        return session

    def patient_cql_connection(self, node, keyspace=None, version=None, user=None, password=None, timeout=10, compression=True):
        """
        Returns a connection after it stops throwing TTransportExceptions due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout = timeout * 5

        return retry_till_success(
            self.cql_connection,
            node,
            keyspace=keyspace,
            version=version,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            bypassed_exception=NoHostAvailable
        )

    def create_ks(self, session, name, rf):
        if self.cluster.version() >= "1.2":
            query = 'CREATE KEYSPACE %s WITH replication={%s}'
            if isinstance(rf, types.IntType):
                # we assume simpleStrategy
                session.execute(query % (name, "'class':'SimpleStrategy', 'replication_factor':%d" % rf))
            else:
                assert len(rf) != 0, "At least one datacenter/rf pair is needed"
                # we assume networkTopolyStrategy
                options = (', ').join([ '\'%s\':%d' % (d, r) for d, r in rf.iteritems() ])
                session.execute(query % (name, "'class':'NetworkTopologyStrategy', %s" % options))
        else:
            query = 'CREATE KEYSPACE %s WITH strategy_class=%s AND %s'
            if isinstance(rf, types.IntType):
                # we assume simpleStrategy
                session.execute(query % (name, 'SimpleStrategy', 'strategy_options:replication_factor=%d' % rf))
            else:
                assert len(rf) != 0, "At least one datacenter/rf pair is needed"
                # we assume networkTopolyStrategy
                options = (' AND ').join([ 'strategy_options:%s=%d' % (d, r) for d, r in rf.iteritems() ])
                session.execute(query % (name, 'NetworkTopologyStrategy', options))
        session.execute('USE %s' % name)

    # We default to UTF8Type because it's simpler to use in tests
    def create_cf(self, session, name, key_type="varchar", speculative_retry=None, read_repair=None, compression=None, gc_grace=None, columns=None, validation="UTF8Type"):
        additional_columns = ""
        if columns is not None:
            for k, v in columns.items():
                additional_columns = "%s, %s %s" % (additional_columns, k, v)

        if self.cluster.version() >= "1.2":
            if additional_columns == "":
                query = 'CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'' % (name, key_type)
            else:
                query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'' % (name, key_type, additional_columns)
            if compression is not None:
                query = '%s AND compression = { \'sstable_compression\': \'%sCompressor\' }' % (query, compression)
        else:
            query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comparator=UTF8Type AND default_validation=%s' % (name, key_type, additional_columns, validation)
            if compression is not None:
                query = '%s AND compression_parameters:sstable_compression=%sCompressor' % (query, compression)

        if read_repair is not None:
            query = '%s AND read_repair_chance=%f' % (query, read_repair)
        if gc_grace is not None:
            query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
        if self.cluster.version() >= "2.0":
            if speculative_retry is not None:
                query = '%s AND speculative_retry=\'%s\'' % (query, speculative_retry)

        session.execute(query)
        time.sleep(0.2)

    def tearDown(self):
        reset_environment_vars()

        for con in self.connections:
            con.shutdown()

        for runner in self.runners:
            try:
                runner.stop()
            except:
                pass

        failed = sys.exc_info() != (None, None, None)
        try:
            for node in self.cluster.nodelist():
                if self.allow_log_errors == False:
                    errors = list(self.__filter_errors([ msg for msg, i in node.grep_log("ERROR")]))
                    if len(errors) is not 0:
                        failed = True
                        raise AssertionError('Unexpected error in %s node log: %s' % (node.name, errors))
        finally:
            try:
                if failed or KEEP_LOGS:
                    # means the test failed. Save the logs for inspection.
                    self.copy_logs()
            except Exception as e:
                    print "Error saving log:", str(e)
            finally:
                self._cleanup_cluster()

    def __filter_errors(self, errors):
        """Filter errors, removing those that match self.ignore_log_patterns"""
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        for e in errors:
            for pattern in self.ignore_log_patterns:
                if re.search(pattern, e):
                    break
            else:
                yield e

class TracingCursor(ThriftCursor):
    """A CQL Cursor with query tracing ability"""
    def __init__(self, connection):
        ThriftCursor.__init__(self, connection)
        self.last_session_id = None
        self.connection = connection

    def execute(self, cql_query, params={}, decoder=None, 
                consistency_level=None, trace=True):
        if trace:
            self.last_session_id = UUID(bytes=self.connection.client.trace_next_query())
        ThriftCursor.execute(self, cql_query, params=params, decoder=decoder, 
                             consistency_level=consistency_level)

    def get_last_trace(self):
        if self.last_session_id:
            time.sleep(0.5) # Tracing is done async, so wait a little.
            self.execute("SELECT session_id, event_id, activity, source, "
                         "source_elapsed, thread FROM system_traces.events "
                         "WHERE session_id=%s" % self.last_session_id)
            return [event for event in self]
        else:
            raise AssertionError('No query to trace')
        
