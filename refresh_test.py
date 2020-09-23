import time

from dtest import Tester
from ccmlib.node import ToolError
import pytest

since = pytest.mark.since

@since('3.0')
class TestRefresh(Tester):
    def test_refresh_deadlock_startup(self):
        """ Test refresh deadlock during startup (CASSANDRA-14310) """
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.byteman_port = '8100'
        node.import_config_files()
        self.cluster.start()
        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.a (id int primary key, d text)")
        session.execute("CREATE TABLE ks.b (id int primary key, d text)")
        node.nodetool("disableautocompaction") # make sure we have more than 1 sstable
        for x in range(0, 10):
            session.execute("INSERT INTO ks.a (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            session.execute("INSERT INTO ks.b (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            node.flush()
        node.stop()
        node.update_startup_byteman_script('byteman/sstable_open_delay.btm')
        node.start()
        node.watch_log_for("opening keyspace ks", filename="debug.log")
        time.sleep(5)
        for x in range(0, 20):
            try:
                node.nodetool("refresh ks a")
                node.nodetool("refresh ks b")
            except ToolError:
                pass # this is OK post-14310 - we just don't want to hang forever
            time.sleep(1)
