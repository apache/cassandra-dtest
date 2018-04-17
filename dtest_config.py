import subprocess
import os
import ccmlib.repository

from ccmlib.common import is_win, get_version_from_build

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

        self.cassandra_version_from_build = self.get_version_from_build()

        self.delete_logs = request.config.getoption("--delete-logs")
        self.execute_upgrade_tests = request.config.getoption("--execute-upgrade-tests")
        self.disable_active_log_watching = request.config.getoption("--disable-active-log-watching")
        self.keep_test_dir = request.config.getoption("--keep-test-dir")
        self.enable_jacoco_code_coverage = request.config.getoption("--enable-jacoco-code-coverage")

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
