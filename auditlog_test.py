import logging
import os
import os.path
import pytest
import shlex
import stat
import subprocess
import tempfile
import time

from ccmlib.node import handle_external_tool_process
from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)

@since('4.0')
class TestAuditlog(Tester):
    def test_archiving(self):
        cluster = self.cluster
        log_dir = tempfile.mkdtemp('logs')
        moved_log_dir, move_script = self._create_script()
        cluster.set_configuration_options(values={'audit_logging_options': {'enabled': True,
                                                                            'audit_logs_dir': log_dir,
                                                                            'roll_cycle': 'TEST_SECONDLY',
                                                                            'archive_command':'%s %%path'%(move_script)}})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.stress(['write', 'n=100k', "no-warmup", "cl=ONE", "-rate", "threads=300"])
        node.nodetool("disableauditlog")
        assert len(os.listdir(moved_log_dir)) > 0
        for f in os.listdir(log_dir):
            assert not f.endswith(".cq4")

    def test_fql_nodetool_options(self):
        cluster = self.cluster
        log_dir = tempfile.mkdtemp('logs')
        moved_log_dir, move_script = self._create_script()
        cluster.set_configuration_options(values={'full_query_logging_options': {'log_dir': log_dir,
                                                                                 'archive_command': 'conf should not be used'}})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.nodetool("enablefullquerylog --archive-command \"%s %%path\" --roll-cycle=TEST_SECONDLY"%move_script)
        node.stress(['write', 'n=100k', "no-warmup", "cl=ONE", "-rate", "threads=300"])
        # make sure at least one file has been rolled and archived:
        assert node.grep_log("Executing archive command", filename="debug.log")
        assert len(os.listdir(moved_log_dir)) > 0
        node.nodetool("disablefullquerylog")
        for f in os.listdir(log_dir):
            assert not f.endswith(".cq4")
        # make sure there are not non-rolled files
        assert not node.grep_log("Archiving existing file", filename="debug.log")

    def test_archiving_fql(self):
        cluster = self.cluster
        log_dir = tempfile.mkdtemp('logs')
        moved_log_dir, move_script = self._create_script()
        cluster.set_configuration_options(values={'full_query_logging_options': {'log_dir': log_dir,
                                                                                 'roll_cycle': 'TEST_SECONDLY',
                                                                                 'archive_command':'%s %%path'%(move_script)}})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.nodetool("enablefullquerylog")
        node.stress(['write', 'n=100k', "no-warmup", "cl=ONE", "-rate", "threads=300"])
        # make sure at least one file has been rolled and archived:
        assert node.grep_log("Executing archive command", filename="debug.log")
        assert len(os.listdir(moved_log_dir)) > 0
        node.nodetool("disablefullquerylog")
        for f in os.listdir(log_dir):
            assert not f.endswith(".cq4")
        # make sure there are not non-rolled files
        assert not node.grep_log("Archiving existing file", filename="debug.log")

    def test_archive_on_startup(self):
        cluster = self.cluster
        log_dir = tempfile.mkdtemp('logs')
        moved_log_dir, move_script = self._create_script()
        files = []
        for i in range(10):
            (fd, fakelogfile) = tempfile.mkstemp(dir=log_dir, suffix='.cq4')
            # empty cq4 files are deleted as part of CASSANDRA-17933 so we write some fake content into them
            # for the sake of having them archived on startup and not deleted
            try:
                os.write(fd, str.encode("somecontent"))
            finally:
                os.close(fd)
            files.append(fakelogfile)
        for f in files:
            assert os.path.isfile(f)

        assert len(files) == 10

        cluster.set_configuration_options(values={'full_query_logging_options': {'log_dir': log_dir,
                                                                                 'roll_cycle': 'TEST_SECONDLY',
                                                                                 'archive_command':'%s %%path'%(move_script)}})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.nodetool("enablefullquerylog")

        for f in files:
            assert not os.path.isfile(f)
            filename = os.path.basename(f)
            assert os.path.isfile(os.path.join(moved_log_dir, filename))

    def test_archive_on_shutdown(self):
        cluster = self.cluster
        log_dir = tempfile.mkdtemp('logs')
        moved_log_dir, move_script = self._create_script()

        cluster.set_configuration_options(values={'full_query_logging_options': {'log_dir': log_dir,
                                                                                 'roll_cycle': 'TEST_SECONDLY',
                                                                                 'archive_command':'%s %%path'%(move_script)}})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.nodetool("enablefullquerylog")

        # adding a bunch of files after fql is enabled - these will get archived when we disable
        files = []
        for i in range(10):
            (_, fakelogfile) = tempfile.mkstemp(dir=log_dir, suffix='.cq4')
            files.append(fakelogfile)
        for f in files:
            assert os.path.isfile(f)

        assert len(files) == 10

        node.nodetool("disablefullquerylog")

        for f in files:
            assert not os.path.isfile(f)
            filename = os.path.basename(f)
            assert os.path.isfile(os.path.join(moved_log_dir, filename))


    def _create_script(self):
        moved_log_dir = tempfile.mkdtemp('movedlogs')
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("""
#!/bin/sh
mv $1 %s
                """%moved_log_dir)
            move_script = f.name
        st = os.stat(move_script)
        os.chmod(move_script, st.st_mode | stat.S_IEXEC)
        return (moved_log_dir, move_script)
