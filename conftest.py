import copy
import collections
import inspect
import logging
import os
import platform
import re
import shutil
import time
from datetime import datetime
from distutils.version import LooseVersion
# Python 3 imports
from itertools import zip_longest

import ccmlib.repository
import netifaces as ni
import pytest
from ccmlib.common import validate_install_dir, is_win, get_version_from_build
from netifaces import AF_INET
from psutil import virtual_memory

from dtest import running_in_docker, cleanup_docker_environment_before_test_execution
from dtest_config import DTestConfig
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides

logger = logging.getLogger(__name__)

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
    parser.addoption("--only-resource-intensive-tests", action="store_true", default=False,
                     help="Only run tests marked as resource_intensive")
    parser.addoption("--skip-resource-intensive-tests", action="store_true", default=False,
                     help="Skip all tests marked as resource_intensive")
    parser.addoption("--cassandra-dir", action="store", default=None,
                     help="The directory containing the built C* artifacts to run the tests against. "
                          "(e.g. the path to the root of a cloned C* git directory. Before executing dtests using "
                          "this directory you must build C* with 'ant clean jar'). If you're doing C* development and "
                          "want to run the tests this is almost always going to be the correct option.")
    parser.addini("cassandra_dir", default=None,
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
    parser.addoption("--execute-upgrade-tests-only", action="store_true", default=False,
                     help="Execute Cassandra Upgrade Tests without running any other tests")
    parser.addoption("--disable-active-log-watching", action="store_true", default=False,
                     help="Disable ccm active log watching, which will cause dtests to check for errors in the "
                          "logs in a single operation instead of semi-realtime processing by consuming "
                          "ccm _log_error_handler callbacks")
    parser.addoption("--keep-test-dir", action="store_true", default=False,
                     help="Do not remove/cleanup the test ccm cluster directory and it's artifacts "
                          "after the test completes")
    parser.addoption("--keep-failed-test-dir", action="store_true", default=False,
                     help="Do not remove/cleanup the test ccm cluster directory and it's artifacts "
                          "after the test fails")
    parser.addoption("--enable-jacoco-code-coverage", action="store_true", default=False,
                     help="Enable JaCoCo Code Coverage Support")
    parser.addoption("--upgrade-version-selection", action="store", default="indev",
                     help="Specify whether to run indev, releases, or both")


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

@pytest.fixture(scope='function')
def fixture_dtest_cluster_name():
    """
    :return: The name to use for the running test's cluster
    """
    return "test"

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
    if request.node.get_closest_marker('no_vnodes'):
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
        if not os.path.exists(node.logfilename()):
            continue
        errors = list(_filter_errors(dtest_setup, ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
        if len(errors) != 0:
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
        name = os.path.join(directory, "last")
    else:
        name = os.path.join(directory, name)
    if not os.path.exists(directory):
        os.mkdir(directory)

    basedir = str(int(time.time() * 1000)) + '_' + request.node.name
    logdir = os.path.join(directory, basedir)

    any_file = False
    for node in cluster.nodes.values():
        nodelogdir = node.log_directory()
        for f in os.listdir(nodelogdir):
            file = os.path.join(nodelogdir, f)
            if os.path.isfile(file):
                if not any_file:
                    os.mkdir(logdir)
                    any_file = True

                if f == 'system.log':
                    target_name = node.name + '.log'
                elif f == 'gc.log.0.current':
                    target_name = node.name + '_gc.log'
                else:
                    target_name = node.name + '_' + f
                shutil.copyfile(file, os.path.join(logdir, target_name))

    if any_file:
        if os.path.exists(name):
            os.unlink(name)
        if not is_win():
            os.symlink(basedir, name)


def reset_environment_vars(initial_environment):
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    os.environ.clear()
    os.environ.update(initial_environment)
    os.environ['PYTEST_CURRENT_TEST'] = pytest_current_test

@pytest.fixture(scope='function')
def fixture_dtest_create_cluster_func():
    """
    :return: A function whose sole argument is a DTestSetup instance and returns an
             object that operates with the same interface as ccmlib.Cluster.
    """
    return DTestSetup.create_ccm_cluster

@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)
    return rep

@pytest.fixture(scope='function', autouse=False)
def fixture_dtest_setup(request,
                        dtest_config,
                        fixture_dtest_setup_overrides,
                        fixture_logging_setup,
                        fixture_dtest_cluster_name,
                        fixture_dtest_create_cluster_func):
    if running_in_docker():
        cleanup_docker_environment_before_test_execution()

    # do all of our setup operations to get the enviornment ready for the actual test
    # to run (e.g. bring up a cluster with the necessary config, populate variables, etc)
    initial_environment = copy.deepcopy(os.environ)
    dtest_setup = DTestSetup(dtest_config=dtest_config,
                             setup_overrides=fixture_dtest_setup_overrides,
                             cluster_name=fixture_dtest_cluster_name)
    dtest_setup.initialize_cluster(fixture_dtest_create_cluster_func)

    if not dtest_config.disable_active_log_watching:
        dtest_setup.begin_active_log_watch()

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
            dtest_setup.cleanup_cluster(request)


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
    if isinstance(since_version, collections.Sequence):
        previous = None
        since_version.sort()

        for i in range(1, len(since_version) + 1):
            sv = since_version[-i]
            if loose_version_compare(current_running_version, sv) >= 0:
                if not previous:
                    if max_version and loose_version_compare(current_running_version, max_version) > 0:
                        return "%s > %s" % (current_running_version, max_version)
                    return None

                if loose_version_compare(current_running_version, previous) < 0:
                    return None

            previous = LooseVersion('.'.join([str(s) for s in sv.version[:-1]]))

        # no matches found, so fail
        return "%s < %s" % (current_running_version, since_version)
    else:
        if loose_version_compare(current_running_version, since_version) < 0:
            return "%s < %s" % (current_running_version, since_version)
        if max_version and loose_version_compare(current_running_version, max_version) > 0:
            return "%s > %s" % (current_running_version, max_version)


@pytest.fixture(autouse=True)
def fixture_since(request, fixture_dtest_setup):
    if request.node.get_closest_marker('since'):
        max_version_str = request.node.get_closest_marker('since').kwargs.get('max_version', None)
        max_version = None
        if max_version_str:
            max_version = LooseVersion(max_version_str)

        since_str_or_list = request.node.get_closest_marker('since').args[0]
        if not isinstance(since_str_or_list, str) and isinstance(since_str_or_list, collections.Sequence):
            since = [LooseVersion(since_str) for since_str in since_str_or_list]
        else:
            since = LooseVersion(since_str_or_list)
        # For upgrade tests don't run the test if any of the involved versions
        # are excluded by the annotation
        if hasattr(request.cls, "UPGRADE_PATH"):
            upgrade_path = request.cls.UPGRADE_PATH
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(upgrade_path.starting_meta.version)
            starting_version = get_version_from_build(ccm_repo_cache_dir)
            skip_msg = _skip_msg(starting_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(upgrade_path.upgrade_meta.version)
            ending_version = get_version_from_build(ccm_repo_cache_dir)
            skip_msg = _skip_msg(ending_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)
        else:
            # For regular tests the value in the current cluster actually means something so we should
            # use that to check.
            # Use cassandra_version_from_build as it's guaranteed to be a LooseVersion
            # whereas cassandra_version may be a string if set in the cli options
            current_running_version = fixture_dtest_setup.dtest_config.cassandra_version_from_build
            skip_msg = _skip_msg(current_running_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)


@pytest.fixture(autouse=True)
def fixture_skip_version(request, fixture_dtest_setup):
    marker = request.node.get_closest_marker('skip_version')
    if marker is not None:
        version_to_skip = LooseVersion(marker.args[0])
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


def cassandra_dir_and_version(config):
    cassandra_dir = config.getoption("--cassandra-dir") or config.getini("cassandra_dir")
    cassandra_version = config.getoption("--cassandra-version")
    return cassandra_dir, cassandra_version


def pytest_collection_modifyitems(items, config):
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
    collect_only = config.getoption("--collect-only")
    cassandra_dir, cassandra_version = cassandra_dir_and_version(config)
    if not collect_only and cassandra_dir is None:
        if  cassandra_version is None:
            raise Exception("Required dtest arguments were missing! You must provide either --cassandra-dir "
                            "or --cassandra-version. You can also set 'cassandra_dir' in pytest.ini. "
                            "Refer to the documentation or invoke the help with --help.")

    # Either cassandra_version or cassandra_dir is defined, so figure out the version
    CASSANDRA_VERSION = cassandra_version or get_version_from_build(cassandra_dir)

    # Check that use_off_heap_memtables is supported in this c* version
    if config.getoption("--use-off-heap-memtables") and ("3.0" <= CASSANDRA_VERSION < "3.4"):
        raise Exception("The selected Cassandra version %s doesn't support the provided option "
                        "--use-off-heap-memtables, see https://issues.apache.org/jira/browse/CASSANDRA-9472 "
                        "for details" % CASSANDRA_VERSION)


    selected_items = []
    deselected_items = []

    sufficient_system_resources_resource_intensive = sufficient_system_resources_for_resource_intensive_tests()
    logger.debug("has sufficient resources? %s" % sufficient_system_resources_resource_intensive)

    for item in items:
        deselect_test = False

        if config.getoption("--execute-upgrade-tests-only"):
            deselect_test = not item.get_closest_marker("upgrade_test")
            if deselect_test:
                logger.info("SKIP: Deselecting non-upgrade test %s because of --execute-upgrade-tests-only" % item.name)

        if item.get_closest_marker("resource_intensive") and not collect_only:
            force_resource_intensive = config.getoption("--force-resource-intensive-tests")
            skip_resource_intensive = config.getoption("--skip-resource-intensive-tests")
            if not force_resource_intensive:
                if skip_resource_intensive:
                    deselect_test = True
                    logger.info("SKIP: Deselecting test %s as test marked resource_intensive. To force execution of "
                          "this test re-run with the --force-resource-intensive-tests command line argument" % item.name)
                if not sufficient_system_resources_resource_intensive:
                    deselect_test = True
                    logger.info("SKIP: Deselecting resource_intensive test %s due to insufficient system resources" % item.name)

        if not item.get_closest_marker("resource_intensive") and not collect_only:
            only_resource_intensive = config.getoption("--only-resource-intensive-tests")
            if only_resource_intensive:
                deselect_test = True
                logger.info("SKIP: Deselecting non resource_intensive test %s as --only-resource-intensive-tests specified" % item.name)

        if item.get_closest_marker("no_vnodes"):
            if config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be disabled. To run this test, "
                      "re-run without the --use-vnodes command line argument" % item.name)

        # include ported_to_in_jvm when doing the vnode check as tests ported to jvm-dtest
        # do not support vnode, so these tests still need to run, only on vnode though
        if item.get_closest_marker("vnodes") or item.get_closest_marker("ported_to_in_jvm"):
            if not config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be enabled. To run this test, "
                            "re-run with the --use-vnodes command line argument" % item.name)

        for test_item_class in inspect.getmembers(item.module, inspect.isclass):
            if not hasattr(test_item_class[1], "pytestmark"):
                continue

            for module_pytest_mark in test_item_class[1].pytestmark:
                if module_pytest_mark.name == "upgrade_test":
                    deselect_test = not _upgrade_testing_enabled(config)

        if item.get_closest_marker("upgrade_test"):
            deselect_test = not _upgrade_testing_enabled(config)

        if item.get_closest_marker("no_offheap_memtables"):
            if config.getoption("use_off_heap_memtables"):
                deselect_test = True

        # deselect cqlsh tests that depend on fixing a driver behavior
        if item.get_closest_marker("depends_driver"):
            deselect_test = True

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items


def _upgrade_testing_enabled(config):
    return config.getoption("--execute-upgrade-tests") or config.getoption("--execute-upgrade-tests-only")
