import pytest
import glob
import os
import shutil
import time
import logging
import re
import tempfile
import subprocess
import sys
import errno
import pprint
from collections import OrderedDict

from cassandra.cluster import Cluster as PyCluster
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from ccmlib.common import get_version_from_build, is_win
from ccmlib.cluster import Cluster

from dtest import (get_ip_from_node, make_execution_profile, get_auth_provider, get_port_from_node,
                   get_eager_protocol_version)
from distutils.version import LooseVersion

from tools.context import log_filter
from tools.funcutils import merge_dicts

logger = logging.getLogger(__name__)


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


class DTestSetup:
    def __init__(self, dtest_config=None, setup_overrides=None, cluster_name="test"):
        self.dtest_config = dtest_config
        self.setup_overrides = setup_overrides
        self.cluster_name = cluster_name
        self.ignore_log_patterns = []
        self.cluster = None
        self.cluster_options = []
        self.replacement_node = None
        self.allow_log_errors = False
        self.connections = []

        self.log_saved_dir = "logs"
        try:
            os.mkdir(self.log_saved_dir)
        except OSError:
            pass

        self.last_log = os.path.join(self.log_saved_dir, "last")
        self.test_path = self.get_test_path()
        self.enable_for_jolokia = False
        self.subprocs = []
        self.log_watch_thread = None
        self.last_test_dir = "last_test_dir"
        self.jvm_args = []
        self.create_cluster_func = None
        self.iterations = 0

    def get_test_path(self):
        test_path = tempfile.mkdtemp(prefix='dtest-')

        # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
        # regular Windows space on wmic calls which will otherwise break pathing
        if sys.platform == "cygwin":
            process = subprocess.Popen(["cygpath", "-m", test_path], stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            test_path = process.communicate()[0].rstrip()

        return test_path

    def glob_data_dirs(self, path, ks="ks"):
        result = []
        for node in self.cluster.nodelist():
            for data_dir in node.data_directories():
                ks_dir = os.path.join(data_dir, ks, path)
                result.extend(glob.glob(ks_dir))
        return result

    def begin_active_log_watch(self):
        """
        Calls into ccm to start actively watching logs.

        In the event that errors are seen in logs, ccm will call back to _log_error_handler.

        When the cluster is no longer in use, stop_active_log_watch should be called to end log watching.
        (otherwise a 'daemon' thread will (needlessly) run until the process exits).
        """
        self.log_watch_thread = self.cluster.actively_watch_logs_for_error(self._log_error_handler, interval=0.25)

    def _log_error_handler(self, errordata):
        """
        Callback handler used in conjunction with begin_active_log_watch.
        When called, prepares exception instance, we will use pytest.fail
        to kill the current test being executed and mark it as failed

        @param errordata is a dictonary mapping node name to failure list.
        """
        # in some cases self.allow_log_errors may get set after proactive log checking has been enabled
        # so we need to double-check first thing before proceeding
        if self.allow_log_errors:
            return

        reportable_errordata = OrderedDict()

        for nodename, errors in list(errordata.items()):
            filtered_errors = list(self.__filter_errors(['\n'.join(msg) for msg in errors]))
            if len(filtered_errors) is not 0:
                reportable_errordata[nodename] = filtered_errors

        # no errors worthy of halting the test
        if not reportable_errordata:
            return

        message = "Errors seen in logs for: {nodes}".format(nodes=", ".join(list(reportable_errordata.keys())))
        for nodename, errors in list(reportable_errordata.items()):
            for error in errors:
                message += "\n{nodename}: {error}".format(nodename=nodename, error=error)

        logger.debug('Errors were just seen in logs, ending test (if not ending already)!')
        pytest.fail("Error details: \n{message}".format(message=message))

    def copy_logs(self, directory=None, name=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        if directory is None:
            directory = self.log_saved_dir
        if name is None:
            name = self.last_log
        else:
            name = os.path.join(directory, name)
        if not os.path.exists(directory):
            os.mkdir(directory)
        logs = [(node.name, node.logfilename(), node.debuglogfilename(), node.gclogfilename(),
                 node.compactionlogfilename())
                for node in self.cluster.nodelist()]
        if len(logs) is not 0:
            basedir = str(int(time.time() * 1000)) + '_' + str(id(self))
            logdir = os.path.join(directory, basedir)
            os.mkdir(logdir)
            for n, log, debuglog, gclog, compactionlog in logs:
                if os.path.exists(log):
                    assert os.path.getsize(log) >= 0
                    shutil.copyfile(log, os.path.join(logdir, n + ".log"))
                if os.path.exists(debuglog):
                    assert os.path.getsize(debuglog) >= 0
                    shutil.copyfile(debuglog, os.path.join(logdir, n + "_debug.log"))
                if os.path.exists(gclog):
                    assert os.path.getsize(gclog) >= 0
                    shutil.copyfile(gclog, os.path.join(logdir, n + "_gc.log"))
                if os.path.exists(compactionlog):
                    assert os.path.getsize(compactionlog) >= 0
                    shutil.copyfile(compactionlog, os.path.join(logdir, n + "_compaction.log"))
            if os.path.exists(name):
                os.unlink(name)
            if not is_win():
                os.symlink(basedir, name)

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None, port=None, ssl_opts=None, **kwargs):

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def exclusive_cql_connection(self, node, keyspace=None, user=None,
                                 password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                                 **kwargs):

        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr,
                                    **kwargs)

    def _create_session(self, node, keyspace, user, password, compression, protocol_version,
                        port=None, ssl_opts=None, execution_profiles=None, **kwargs):
        node_ip = get_ip_from_node(node)
        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = get_eager_protocol_version(node.cluster.version())

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(**kwargs)
                    } if not execution_profiles else execution_profiles

        cluster = PyCluster([node_ip],
                            auth_provider=auth_provider,
                            compression=compression,
                            protocol_version=protocol_version,
                            port=port,
                            ssl_options=ssl_opts,
                            connect_timeout=15,
                            allow_beta_protocol_version=True,
                            execution_profiles=profiles)
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        self.connections.append(session)
        return session

    def patient_cql_connection(self, node, keyspace=None,
                               user=None, password=None, timeout=30, compression=True,
                               protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        expected_log_lines = ('Control connection failed to connect, shutting down Cluster:',
                              '[control connection] Error connecting to ')
        with log_filter('cassandra.cluster', expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs
            )

        return session

    def patient_exclusive_cql_connection(self, node, keyspace=None,
                                         user=None, password=None, timeout=30, compression=True,
                                         protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def check_logs_for_errors(self):
        for node in self.cluster.nodelist():
            errors = list(self.__filter_errors(
                ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
            if len(errors) is not 0:
                for error in errors:
                    print("Unexpected error in {node_name} log, error: \n{error}".format(node_name=node.name, error=error))
                return True

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

    def get_jfr_jvm_args(self):
        """
        @return The JVM arguments required for attaching flight recorder to a Java process.
        """
        return ["-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder"]

    def start_jfr_recording(self, nodes):
        """
        Start Java flight recorder provided the cluster was started with the correct jvm arguments.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.start'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def dump_jfr_recording(self, nodes):
        """
        Save Java flight recorder results to file for analyzing with mission control.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.dump',
                                  'recording=1', 'filename=recording_{}.jfr'.format(node.address())],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def supports_v5_protocol(self, cluster_version):
        return cluster_version >= LooseVersion('4.0')

    def cleanup_last_test_dir(self):
        if os.path.exists(self.last_test_dir):
            os.remove(self.last_test_dir)

    def stop_active_log_watch(self):
        """
        Joins the log watching thread, which will then exit.
        Should be called after each test, ideally after nodes are stopped but before cluster files are removed.

        Can be called multiple times without error.
        If not called, log watching thread will remain running until the parent process exits.
        """
        self.log_watch_thread.join(timeout=60)

    def cleanup_cluster(self):
        with log_filter('cassandra'):  # quiet noise from driver when nodes start going down
            if self.dtest_config.keep_test_dir:
                self.cluster.stop(gently=self.dtest_config.enable_jacoco_code_coverage)
            else:
                # when recording coverage the jvm has to exit normally
                # or the coverage information is not written by the jacoco agent
                # otherwise we can just kill the process
                if self.dtest_config.enable_jacoco_code_coverage:
                    self.cluster.stop(gently=True)

                # Cleanup everything:
                try:
                    if self.log_watch_thread:
                        self.stop_active_log_watch()
                finally:
                    logger.debug("removing ccm cluster {name} at: {path}".format(name=self.cluster.name,
                                                                          path=self.test_path))
                    self.cluster.remove()

                    logger.debug("clearing ssl stores from [{0}] directory".format(self.test_path))
                    for filename in ('keystore.jks', 'truststore.jks', 'ccm_node.cer'):
                        try:
                            os.remove(os.path.join(self.test_path, filename))
                        except OSError as e:
                            # ENOENT = no such file or directory
                            assert e.errno == errno.ENOENT

                    os.rmdir(self.test_path)
                    self.cleanup_last_test_dir()

    def cleanup_and_replace_cluster(self):
        for con in self.connections:
            con.cluster.shutdown()
        self.connections = []

        self.cleanup_cluster()
        self.test_path = self.get_test_path()
        self.initialize_cluster(self.create_cluster_func)

    def init_default_config(self):
        # the failure detector can be quite slow in such tests with quick start/stop
        phi_values = {'phi_convict_threshold': 5}

        # enable read time tracking of repaired data between replicas by default
        if self.cluster.version() >= '4':
            repaired_data_tracking_values = {'repaired_data_tracking_for_partition_reads_enabled': 'true',
                                             'repaired_data_tracking_for_range_reads_enabled': 'true',
                                             'report_unconfirmed_repaired_data_mismatches': 'true'}
        else:
            repaired_data_tracking_values = {}

        timeout = 15000
        if self.cluster_options is not None and len(self.cluster_options) > 0:
            values = merge_dicts(self.cluster_options, phi_values, repaired_data_tracking_values)
        else:
            values = merge_dicts(phi_values, repaired_data_tracking_values, {
                'read_request_timeout_in_ms': timeout,
                'range_request_timeout_in_ms': timeout,
                'write_request_timeout_in_ms': timeout,
                'truncate_request_timeout_in_ms': timeout,
                'request_timeout_in_ms': timeout
            })

        if self.setup_overrides is not None and len(self.setup_overrides.cluster_options) > 0:
            values = merge_dicts(values, self.setup_overrides.cluster_options)

        # No more thrift in 4.0, and start_rpc doesn't exists anymore
        if self.cluster.version() >= '4' and 'start_rpc' in values:
            del values['start_rpc']

        self.cluster.set_configuration_options(values)
        logger.debug("Done setting configuration options:\n" + pprint.pformat(self.cluster._config_options, indent=4))

    def maybe_setup_jacoco(self, cluster_name='test'):
        """Setup JaCoCo code coverage support"""

        if not self.dtest_config.enable_jacoco_code_coverage:
            return

        # use explicit agent and execfile locations
        # or look for a cassandra build if they are not specified
        agent_location = os.environ.get('JACOCO_AGENT_JAR',
                                        os.path.join(self.dtest_config.cassandra_dir, 'build/lib/jars/jacocoagent.jar'))
        jacoco_execfile = os.environ.get('JACOCO_EXECFILE',
                                         os.path.join(self.dtest_config.cassandra_dir, 'build/jacoco/jacoco.exec'))

        if os.path.isfile(agent_location):
            logger.debug("Jacoco agent found at {}".format(agent_location))
            with open(os.path.join(
                    self.test_path, cluster_name, 'cassandra.in.sh'), 'w') as f:

                f.write('JVM_OPTS="$JVM_OPTS -javaagent:{jar_path}=destfile={exec_file}"'
                        .format(jar_path=agent_location, exec_file=jacoco_execfile))

                if os.path.isfile(jacoco_execfile):
                    logger.debug("Jacoco execfile found at {}, execution data will be appended".format(jacoco_execfile))
                else:
                    logger.debug("Jacoco execfile will be created at {}".format(jacoco_execfile))
        else:
            logger.debug("Jacoco agent not found or is not file. Execution will not be recorded.")


    @staticmethod
    def create_ccm_cluster(dtest_setup):
        logger.debug("cluster ccm directory: " + dtest_setup.test_path)
        version = dtest_setup.dtest_config.cassandra_version

        if version:
            cluster = Cluster(dtest_setup.test_path, dtest_setup.cluster_name, cassandra_version=version)
        else:
            cluster = Cluster(dtest_setup.test_path, dtest_setup.cluster_name, cassandra_dir=dtest_setup.dtest_config.cassandra_dir)

        if dtest_setup.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': dtest_setup.dtest_config.num_tokens})
        else:
            cluster.set_configuration_options(values={'num_tokens': None})

        if dtest_setup.dtest_config.use_off_heap_memtables:
            cluster.set_configuration_options(values={'memtable_allocation_type': 'offheap_objects'})

        cluster.set_datadir_count(dtest_setup.dtest_config.data_dir_count)
        cluster.set_environment_variable('CASSANDRA_LIBJEMALLOC', dtest_setup.dtest_config.jemalloc_path)

        return cluster

    def set_cluster_log_levels(self):
        """
        The root logger gets configured in the fixture named fixture_logging_setup.
        Based on the logging configuration options the user invoked pytest with,
        that fixture sets the root logger to that configuration. We then ensure all
        Cluster objects we work with "inherit" these logging settings (which we can
        lookup off the root logger)
        """
        if logging.root.level != 'NOTSET':
            log_level = logging.getLevelName(logging.INFO)
        else:
            log_level = logging.root.level
        self.cluster.set_log_level(log_level)

    def initialize_cluster(self, create_cluster_func):
        """
        This method is responsible for initializing and configuring a ccm
        cluster for the next set of tests.  This can be called for two
        different reasons:
         * A class of tests is starting
         * A test method failed/errored, so the cluster has been wiped

        Subclasses that require custom initialization should generally
        do so by overriding post_initialize_cluster().
        """
        # connections = []
        # cluster_options = []
        self.iterations += 1
        self.create_cluster_func = create_cluster_func
        self.cluster = self.create_cluster_func(self)
        self.init_default_config()
        self.maybe_setup_jacoco()
        self.set_cluster_log_levels()

        # cls.init_config()
        # write_last_test_file(cls.test_path, cls.cluster)

        # cls.post_initialize_cluster()
