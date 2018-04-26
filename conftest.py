import pytest
import logging
import os
import shutil
import time
import re
import platform
import copy
import inspect
import subprocess
from itertools import zip_longest

from dtest import running_in_docker, cleanup_docker_environment_before_test_execution

from datetime import datetime
from distutils.version import LooseVersion
from netifaces import AF_INET
from psutil import virtual_memory

import netifaces as ni
import ccmlib.repository
from ccmlib.common import validate_install_dir, get_version_from_build, is_win

from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides

logger = logging.getLogger(__name__)


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.use_off_heap_memtables = False
        self.num_tokens = -1
        self.data_dir_count = -1
        self.force_execution_of_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.cassandra_dir = None
        self.cassandra_version = None
        self.cassandra_version_from_build = None
        self.delete_logs = False
        self.execute_upgrade_tests = False
        self.disable_active_log_watching = False
        self.keep_test_dir = False
        self.enable_jacoco_code_coverage = False
        self.jemalloc_path = find_libjemalloc()

    def setup(self, request):
        self.use_vnodes = request.config.getoption("--use-vnodes")
        self.use_off_heap_memtables = request.config.getoption("--use-off-heap-memtables")
        self.num_tokens = request.config.getoption("--num-tokens")
        self.data_dir_count = request.config.getoption("--data-dir-count-per-instance")
        self.force_execution_of_resource_intensive_tests = request.config.getoption("--force-resource-intensive-tests")
        self.skip_resource_intensive_tests = request.config.getoption("--skip-resource-intensive-tests")
        if request.config.getoption("--cassandra-dir") is not None:
            self.cassandra_dir = os.path.expanduser(request.config.getoption("--cassandra-dir"))
        self.cassandra_version = request.config.getoption("--cassandra-version")

        # There are times when we want to know the C* version we're testing against
        # before we do any cluster. In the general case, we can't know that -- the
        # test method could use any version it wants for self.cluster. However, we can
        # get the version from build.xml in the C* repository specified by
        # CASSANDRA_VERSION or CASSANDRA_DIR.
        if self.cassandra_version is not None:
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(self.cassandra_version)
            self.cassandra_version_from_build = get_version_from_build(ccm_repo_cache_dir)
        elif self.cassandra_dir is not None:
            self.cassandra_version_from_build = get_version_from_build(self.cassandra_dir)

        self.delete_logs = request.config.getoption("--delete-logs")
        self.execute_upgrade_tests = request.config.getoption("--execute-upgrade-tests")
        self.disable_active_log_watching = request.config.getoption("--disable-active-log-watching")
        self.keep_test_dir = request.config.getoption("--keep-test-dir")
        self.enable_jacoco_code_coverage = request.config.getoption("--enable-jacoco-code-coverage")


def check_required_loopback_interfaces_available():
    """
    We need at least 3 loopback interfaces configured to run almost all dtests. On Linux, loopback
    interfaces are automatically created as they are used, but on Mac they need to be explicitly
    created. Check if we're running on Mac (Darwin), and if so check we have at least 3 loopback
    interfaces available, otherwise bail out so we don't run the tests in a known bad config and
    give the user some helpful advice on how to get their machine into a good known config
    """
    if platform.system() == "Darwin":
        if len(ni.ifaddresses('lo0')[AF_INET]) < 9:
            pytest.exit("At least 9 loopback interfaces are required to run dtests. "
                            "On Mac you can create the required loopback interfaces by running "
                            "'for i in {1..9}; do sudo ifconfig lo0 alias 127.0.0.$i up; done;'")


def pytest_addoption(parser):
    parser.addoption("--use-vnodes", action="store_true", default=False,
                     help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--use-off-heap-memtables", action="store_true", default=False,
                     help="Enable Off Heap Memtables when creating test clusters for tests")
    parser.addoption("--num-tokens", action="store", default=256,
                     help="Number of tokens to set num_tokens yaml setting to when creating instances "
                          "with vnodes enabled")
    parser.addoption("--data-dir-count-per-instance", action="store", default=3,
                     help="Control the number of data directories to create per instance")
    parser.addoption("--force-resource-intensive-tests", action="store_true", default=False,
                     help="Forces the execution of tests marked as resource_intensive")
    parser.addoption("--skip-resource-intensive-tests", action="store_true", default=False,
                     help="Skip all tests marked as resource_intensive")
    parser.addoption("--cassandra-dir", action="store", default=None,
                     help="The directory containing the built C* artifacts to run the tests against. "
                          "(e.g. the path to the root of a cloned C* git directory. Before executing dtests using "
                          "this directory you must build C* with 'ant clean jar'). If you're doing C* development and "
                          "want to run the tests this is almost always going to be the correct option.")
    parser.addoption("--cassandra-version", action="store", default=None,
                     help="A specific C* version to run the dtests against. The dtest framework will "
                          "pull the required artifacts for this version.")
    parser.addoption("--delete-logs", action="store_true", default=False,
                     help="Delete all generated logs created by a test after the completion of a test.")
    parser.addoption("--execute-upgrade-tests", action="store_true", default=False,
                     help="Execute Cassandra Upgrade Tests (e.g. tests annotated with the upgrade_test mark)")
    parser.addoption("--disable-active-log-watching", action="store_true", default=False,
                     help="Disable ccm active log watching, which will cause dtests to check for errors in the "
                          "logs in a single operation instead of semi-realtime processing by consuming "
                          "ccm _log_error_handler callbacks")
    parser.addoption("--keep-test-dir", action="store_true", default=False,
                     help="Do not remove/cleanup the test ccm cluster directory and it's artifacts "
                          "after the test completes")
    parser.addoption("--enable-jacoco-code-coverage", action="store_true", default=False,
                     help="Enable JaCoCo Code Coverage Support")


def sufficient_system_resources_for_resource_intensive_tests():
    mem = virtual_memory()
    total_mem_gb = mem.total/1024/1024/1024
    logger.info("total available system memory is %dGB" % total_mem_gb)
    # todo kjkj: do not hard code our bound.. for now just do 9 instances at 3gb a piece
    return total_mem_gb >= 9*3


@pytest.fixture(scope='function', autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    """
    no-op default implementation of fixture_dtest_setup_overrides.
    we run this when a test class hasn't implemented their own
    fixture_dtest_setup_overrides
    """
    return DTestSetupOverrides()


"""
Not exactly sure why :\ but, this fixture needs to be scoped to function level and not
session or class. If you invoke pytest with tests across multiple test classes, when scopped
at session, the root logger appears to get reset between each test class invocation.
this means that the first test to run not from the first test class (and all subsequent 
tests), will have the root logger reset and see a level of NOTSET. Scoping it at the
class level seems to work, and I guess it's not that much extra overhead to setup the
logger once per test class vs. once per session in the grand scheme of things.
"""
@pytest.fixture(scope="function", autouse=True)
def fixture_logging_setup(request):
    # set the root logger level to whatever the user asked for
    # all new loggers created will use the root logger as a template
    # essentially making this the "default" active log level
    log_level = logging.INFO
    try:
        # first see if logging level overridden by user as command line argument
        log_level_from_option = pytest.config.getoption("--log-level")
        if log_level_from_option is not None:
            log_level = logging.getLevelName(log_level_from_option)
        else:
            raise ValueError
    except ValueError:
        # nope, user didn't specify it as a command line argument to pytest, check if
        # we have a default in the loaded pytest.ini. Note: words are seperated in variables
        # in .ini land with a "_" while the command line arguments use "-"
        if pytest.config.inicfg.get("log_level") is not None:
            log_level = logging.getLevelName(pytest.config.inicfg.get("log_level"))

    logging.root.setLevel(log_level)

    logging_format = None
    try:
        # first see if logging level overridden by user as command line argument
        log_format_from_option = pytest.config.getoption("--log-format")
        if log_format_from_option is not None:
            logging_format = log_format_from_option
        else:
            raise ValueError
    except ValueError:
        if pytest.config.inicfg.get("log_format") is not None:
            logging_format = pytest.config.inicfg.get("log_format")

    logging.basicConfig(level=log_level,
                        format=logging_format)

    # next, regardless of the level we set above (and requested by the user),
    # reconfigure the "cassandra" logger to minimum INFO level to override the
    # logging level that the "cassandra.*" imports should use; DEBUG is just
    # insanely noisy and verbose, with the extra logging of very limited help
    # in the context of dtest execution
    if log_level == logging.DEBUG:
        cassandra_module_log_level = logging.INFO
    else:
        cassandra_module_log_level = log_level
    logging.getLogger("cassandra").setLevel(cassandra_module_log_level)


@pytest.fixture(scope="session")
def log_global_env_facts(fixture_dtest_config, fixture_logging_setup):
    if pytest.config.pluginmanager.hasplugin('junitxml'):
        my_junit = getattr(pytest.config, '_xml', None)
        my_junit.add_global_property('USE_VNODES', fixture_dtest_config.use_vnodes)


@pytest.fixture(scope='function', autouse=True)
def fixture_maybe_skip_tests_requiring_novnodes(request):
    """
    Fixture run before the start of every test function that checks if the test is marked with
    the no_vnodes annotation but the tests were started with a configuration that
    has vnodes enabled. This should always be a no-op as we explicitly deselect tests
    in pytest_collection_modifyitems that match this configuration -- but this is explicit :)
    """
    if request.node.get_marker('no_vnodes'):
        if request.config.getoption("--use-vnodes"):
            pytest.skip("Skipping test marked with no_vnodes as tests executed with vnodes enabled via the "
                        "--use-vnodes command line argument")


@pytest.fixture(scope='function', autouse=True)
def fixture_log_test_name_and_date(request, fixture_logging_setup):
    logger.info("Starting execution of %s at %s" % (request.node.name, str(datetime.now())))


def _filter_errors(dtest_setup, errors):
    """Filter errors, removing those that match ignore_log_patterns in the current DTestSetup"""
    for e in errors:
        for pattern in dtest_setup.ignore_log_patterns:
            if re.search(pattern, repr(e)):
                break
        else:
            yield e


def check_logs_for_errors(dtest_setup):
    errors = []
    for node in dtest_setup.cluster.nodelist():
        errors = list(_filter_errors(dtest_setup, ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
        if len(errors) is not 0:
            for error in errors:
                if isinstance(error, (bytes, bytearray)):
                    error_str = error.decode("utf-8").strip()
                else:
                    error_str = error.strip()

                if error_str:
                    logger.error("Unexpected error in {node_name} log, error: \n{error}"
                                 .format(node_name=node.name, error=error_str))
                    errors.append(error_str)
                    break
    return errors


def copy_logs(request, cluster, directory=None, name=None):
    """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
    log_saved_dir = "logs"
    try:
        os.mkdir(log_saved_dir)
    except OSError:
        pass

    if directory is None:
        directory = log_saved_dir
    if name is None:
        name = os.path.join(log_saved_dir, "last")
    else:
        name = os.path.join(directory, name)
    if not os.path.exists(directory):
        os.mkdir(directory)
    logs = [(node.name, node.logfilename(), node.debuglogfilename(), node.gclogfilename(), node.compactionlogfilename())
            for node in list(cluster.nodes.values())]
    if len(logs) is not 0:
        basedir = str(int(time.time() * 1000)) + '_' + request.node.name
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


def reset_environment_vars(initial_environment):
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    os.environ.clear()
    os.environ.update(initial_environment)
    os.environ['PYTEST_CURRENT_TEST'] = pytest_current_test


@pytest.fixture(scope='function', autouse=False)
def fixture_dtest_setup(request, dtest_config, fixture_dtest_setup_overrides, fixture_logging_setup):
    if running_in_docker():
        cleanup_docker_environment_before_test_execution()

    # do all of our setup operations to get the enviornment ready for the actual test
    # to run (e.g. bring up a cluster with the necessary config, populate variables, etc)
    initial_environment = copy.deepcopy(os.environ)
    dtest_setup = DTestSetup(dtest_config=dtest_config, setup_overrides=fixture_dtest_setup_overrides)
    dtest_setup.initialize_cluster()

    if not dtest_config.disable_active_log_watching:
        dtest_setup.log_watch_thread = dtest_setup.begin_active_log_watch()

    # at this point we're done with our setup operations in this fixture
    # yield to allow the actual test to run
    yield dtest_setup

    # phew! we're back after executing the test, now we need to do
    # all of our teardown and cleanup operations

    reset_environment_vars(initial_environment)
    dtest_setup.jvm_args = []

    for con in dtest_setup.connections:
        con.cluster.shutdown()
    dtest_setup.connections = []

    failed = False
    try:
        if not dtest_setup.allow_log_errors:
            errors = check_logs_for_errors(dtest_setup)
            if len(errors) > 0:
                failed = True
                pytest.fail(msg='Unexpected error found in node logs (see stdout for full details). Errors: [{errors}]'
                                     .format(errors=str.join(", ", errors)), pytrace=False)
    finally:
        try:
            # save the logs for inspection
            if failed or not dtest_config.delete_logs:
                copy_logs(request, dtest_setup.cluster)
        except Exception as e:
            logger.error("Error saving log:", str(e))
        finally:
            dtest_setup.cleanup_cluster()


#Based on https://bugs.python.org/file25808/14894.patch
def loose_version_compare(a, b):
    for i, j in zip_longest(a.version, b.version, fillvalue=''):
        if type(i) != type(j):
            i = str(i)
            j = str(j)
        if i == j:
            continue
        elif i < j:
            return -1
        else:  # i > j
            return 1

    #Longer version strings with equal prefixes are equal, but if one version string is longer than it is greater
    aLen = len(a.version)
    bLen = len(b.version)
    if aLen == bLen:
        return 0
    elif aLen < bLen:
        return -1
    else:
        return 1


def _skip_msg(current_running_version, since_version, max_version):
    if loose_version_compare(current_running_version, since_version) < 0:
        return "%s < %s" % (current_running_version, since_version)
    if max_version and loose_version_compare(current_running_version, max_version) > 0:
        return "%s > %s" % (current_running_version, max_version)


@pytest.fixture(autouse=True)
def fixture_since(request, fixture_dtest_setup):
    if request.node.get_marker('since'):
        max_version_str = request.node.get_marker('since').kwargs.get('max_version', None)
        max_version = None
        if max_version_str:
            max_version = LooseVersion(max_version_str)

        since_str = request.node.get_marker('since').args[0]
        since = LooseVersion(since_str)
        # use cassandra_version_from_build as it's guaranteed to be a LooseVersion
        # whereas cassandra_version may be a string if set in the cli options
        current_running_version = fixture_dtest_setup.dtest_config.cassandra_version_from_build
        skip_msg = _skip_msg(current_running_version, since, max_version)
        if skip_msg:
            pytest.skip(skip_msg)


@pytest.fixture(autouse=True)
def fixture_skip_version(request, fixture_dtest_setup):
    marker = request.node.get_marker('skip_version')
    if marker is not None:
        for info in marker:
            version_to_skip = LooseVersion(info.args[0])
            if version_to_skip == fixture_dtest_setup.dtest_config.cassandra_version_from_build:
                pytest.skip("Test marked not to run on version %s" % version_to_skip)


@pytest.fixture(scope='session', autouse=True)
def install_debugging_signal_handler():
    import faulthandler
    faulthandler.enable()


@pytest.fixture(scope='session')
def dtest_config(request):
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    # if we're on mac, check that we have the required loopback interfaces before doing anything!
    check_required_loopback_interfaces_available()

    try:
        if dtest_config.cassandra_dir is not None:
            validate_install_dir(dtest_config.cassandra_dir)
    except Exception as e:
        pytest.exit("{}. Did you remember to build C*? ('ant clean jar')".format(e))

    yield dtest_config


def pytest_collection_modifyitems(items, config):
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
    if not config.getoption("--collect-only") and config.getoption("--cassandra-dir") is None:
        if config.getoption("--cassandra-version") is None:
            raise Exception("Required dtest arguments were missing! You must provide either --cassandra-dir "
                            "or --cassandra-version. Refer to the documentation or invoke the help with --help.")

    selected_items = []
    deselected_items = []

    sufficient_system_resources_resource_intensive = sufficient_system_resources_for_resource_intensive_tests()
    logger.debug("has sufficient resources? %s" % sufficient_system_resources_resource_intensive)

    for item in items:
        #  set a timeout for all tests, it may be overwritten at the test level with an additional marker
        if not item.get_marker("timeout"):
            item.add_marker(pytest.mark.timeout(60*15))

        deselect_test = False

        if item.get_marker("resource_intensive"):
            if config.getoption("--force-resource-intensive-tests"):
                pass
            if config.getoption("--skip-resource-intensive-tests"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as test marked resource_intensive. To force execution of "
                      "this test re-run with the --force-resource-intensive-tests command line argument" % item.name)
            if not sufficient_system_resources_resource_intensive:
                deselect_test = True
                logger.info("SKIP: Deselecting resource_intensive test %s due to insufficient system resources" % item.name)

        if item.get_marker("no_vnodes"):
            if config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be disabled. To run this test, "
                      "re-run without the --use-vnodes command line argument" % item.name)

        if item.get_marker("vnodes"):
            if not config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be enabled. To run this test, "
                            "re-run with the --use-vnodes command line argument" % item.name)

        for test_item_class in inspect.getmembers(item.module, inspect.isclass):
            if not hasattr(test_item_class[1], "pytestmark"):
                continue

            for module_pytest_mark in test_item_class[1].pytestmark:
                if module_pytest_mark.name == "upgrade_test":
                    if not config.getoption("--execute-upgrade-tests"):
                        deselect_test = True

        if item.get_marker("upgrade_test"):
            if not config.getoption("--execute-upgrade-tests"):
                deselect_test = True

        # todo kjkj: deal with no_offheap_memtables mark

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items


# Determine the location of the libjemalloc jar so that we can specify it
# through environment variables when start Cassandra.  This reduces startup
# time, making the dtests run faster.
def find_libjemalloc():
    if is_win():
        # let the normal bat script handle finding libjemalloc
        return ""

    this_dir = os.path.dirname(os.path.realpath(__file__))
    script = os.path.join(this_dir, "findlibjemalloc.sh")
    try:
        p = subprocess.Popen([script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stderr or not stdout:
            return "-"  # tells C* not to look for libjemalloc
        else:
            return stdout
    except Exception as exc:
        print("Failed to run script to prelocate libjemalloc ({}): {}".format(script, exc))
        return ""
