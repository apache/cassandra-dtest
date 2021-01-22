import subprocess
import os
import ccmlib.repository
import logging

from ccmlib.common import is_win, get_version_from_build
from pytest import UsageError

logger = logging.getLogger(__name__)


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.use_off_heap_memtables = False
        self.num_tokens = -1
        self.data_dir_count = -1
        self.force_execution_of_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.only_resource_intensive_tests = False
        self.cassandra_dir = None
        self.cassandra_version = None
        self.cassandra_version_from_build = None
        self.delete_logs = False
        self.execute_upgrade_tests = False
        self.execute_upgrade_tests_only = False
        self.disable_active_log_watching = False
        self.keep_test_dir = False
        self.keep_failed_test_dir = False
        self.enable_jacoco_code_coverage = False
        self.jemalloc_path = find_libjemalloc()
        self.metatests = False

    def setup(self, config):
        """
        Reads and validates configuration. Throws UsageError if configuration is invalid.
        """
        self.metatests = config.getoption("--metatests")
        if self.metatests:
            self.cassandra_dir = os.path.join(os.getcwd(), "meta_tests/cassandra-dir-4.0-beta")
            self.cassandra_version_from_build = self.get_version_from_build()
            return

        self.use_vnodes = config.getoption("--use-vnodes")
        self.use_off_heap_memtables = config.getoption("--use-off-heap-memtables")
        self.num_tokens = config.getoption("--num-tokens")
        self.data_dir_count = config.getoption("--data-dir-count-per-instance")
        self.force_execution_of_resource_intensive_tests = config.getoption("--force-resource-intensive-tests")
        self.skip_resource_intensive_tests = config.getoption("--skip-resource-intensive-tests")
        self.only_resource_intensive_tests = config.getoption("--only-resource-intensive-tests")
        cassandra_dir = config.getoption("--cassandra-dir") or config.getini("cassandra_dir")
        if cassandra_dir is not None and cassandra_dir.strip() == "":
            cassandra_dir = None
        if cassandra_dir is not None:
            self.cassandra_dir = os.path.expanduser(cassandra_dir)
        self.cassandra_version = config.getoption("--cassandra-version")

        if self.cassandra_version is not None and self.cassandra_dir is not None:
            raise UsageError("Please remove --cassandra-version because Cassandra build directory is already "
                             "defined (by --cassandra-dir or in ini file)")

        try:
            self.cassandra_version_from_build = self.get_version_from_build()
        except FileNotFoundError as fnfe:
            raise UsageError("The Cassandra directory %s does not seem to be valid: %s" % (self.cassandra_dir, fnfe))

        self.delete_logs = config.getoption("--delete-logs")
        self.execute_upgrade_tests = config.getoption("--execute-upgrade-tests")
        self.execute_upgrade_tests_only = config.getoption("--execute-upgrade-tests-only")
        self.disable_active_log_watching = config.getoption("--disable-active-log-watching")
        self.keep_test_dir = config.getoption("--keep-test-dir")
        self.keep_failed_test_dir = config.getoption("--keep-failed-test-dir")
        self.enable_jacoco_code_coverage = config.getoption("--enable-jacoco-code-coverage")

        if self.cassandra_version is None and self.cassandra_version_from_build is None:
            raise UsageError("Required dtest arguments were missing! You must provide either --cassandra-dir "
                             "or --cassandra-version. You can also set 'cassandra_dir' in pytest.ini. "
                             "Refer to the documentation or invoke the help with --help.")

        version = self.cassandra_version or self.cassandra_version_from_build

        if self.skip_resource_intensive_tests and \
                (self.only_resource_intensive_tests or self.force_execution_of_resource_intensive_tests):
            raise UsageError("--skip-resource-intensive-tests does not make any sense with either "
                             "--only-resource-intensive-tests or --force-resource-intensive-tests.")

        # Check that use_off_heap_memtables is supported in this c* version
        if self.use_off_heap_memtables and ("3.0" <= version < "3.4"):
            raise UsageError("The selected Cassandra version %s doesn't support the provided option "
                             "--use-off-heap-memtables, see https://issues.apache.org/jira/browse/CASSANDRA-9472 "
                             "for details" % version)

    def get_version_from_build(self):
        # There are times when we want to know the C* version we're testing against
        # before we do any cluster. In the general case, we can't know that -- the
        # test method could use any version it wants for self.cluster. However, we can
        # get the version from build.xml in the C* repository specified by
        # CASSANDRA_VERSION or CASSANDRA_DIR.
        if self.cassandra_version is not None:
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(self.cassandra_version)
            return get_version_from_build(ccm_repo_cache_dir)
        elif self.cassandra_dir is not None:
            return get_version_from_build(self.cassandra_dir)


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
