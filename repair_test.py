import time, re
from dtest import Tester

class TestRepair(Tester):

    def check_rows_on_node(self, node_to_check, rows, columns=[], not_found=[], restart=True):
        stopped_nodes = []
        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        cli = node_to_check.cli()
        cli.do("use test").do("consistencylevel as ONE")
        cli.do("list testCF")
        assert re.search('%d Rows Returned' % rows, cli.last_output()), cli.last_output()
        for k, c in columns:
            cli.do("get testCF[%s][%s]" % (k, c))
            assert re.search('=> \(column=%s' % c, cli.last_output()), cli.last_output()
        for k, c in not_found:
            cli.do("get testCF[%s][%s]" % (k, c))
            assert re.search('not found', cli.last_output()), cli.last_output()
        cli.close()

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def simple_repair_test(self):
        cluster = self.cluster

        cluster.populate(3)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.update_configuration(hh=False, cl_batch=True)

        cluster.start()
        node1, node2, node3 = (cluster.nodes["node1"], cluster.nodes["node2"], cluster.nodes["node3"])
        time.sleep(.5)

        cli = node1.cli()
        if cluster.version() > "1":
            cli.do("create keyspace test with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:3}")
        elif cluster.version() < "0.8":
            cli.do("create keyspace test with replication_factor=3")
        else:
            cli.do("create keyspace test with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:3}]")

        cli.do("use test").do("consistencylevel as ONE")
        cli.do("create column family testCF with key_validation_class=UTF8Type and comparator=UTF8Type and default_validation_class=UTF8Type and read_repair_chance=0.0")


        # Insert 10 keys, kill node 3, insert 1 key, restart node 3, insert 10 more keys
        for i in xrange(0, 10):
            cli.do("set testCF[key%d][col] = 'value'" % i)
        time.sleep(.5)
        node3.nodetool("flush");
        node3.stop()
        cli.do("set testCF[key%d][col] = 'value'" % 10)
        time.sleep(.5)
        node3.start(wait_other_notice=True)
        for i in xrange(11, 21):
            cli.do("set testCF[key%d][col] = 'value'" % i)
        time.sleep(.5)
        assert not cli.has_errors(), "errors: " + str(cli.errors())

        cli.close()

        node1.nodetool("flush");
        node2.nodetool("flush");
        node3.nodetool("flush");

        # Verify that node1 has 21 keys
        self.check_rows_on_node(node1, 21, [("key10", "col")])

        # Verify that node2 has 21 keys
        self.check_rows_on_node(node2, 21, [("key10", "col")])

        # Verify that node3 has only 20 keys
        self.check_rows_on_node(node3, 20, not_found=[("key10", "col")])

        # Run repair
        node1.nodetool("repair")

        # Validate that only one range was transfered
        l = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        if cluster.version() > "1":
            assert len(l) == 2, "Lines matching: " + str([elt[0] for elt in l])
        else:
            # In pre-1.0, we should have only one line
            assert len(l) == 1, "Lines matching: " + str([elt[0] for elt in l])
        valid = [(node1.address(), node3.address()), (node3.address(), node1.address()),
                 (node2.address(), node3.address()), (node3.address(), node2.address())]
        for line, m in l:
            assert int(m.group(3)) == 1, "Expecting 1 range out of sync, got " + int(m.group(1))
            assert (m.group(1), m.group(2)) in valid, str((m.group(1), m.group(2)))
            valid.remove((m.group(1), m.group(2)))
            valid.remove((m.group(2), m.group(1)))

        # Check node3 now has the key
        self.check_rows_on_node(node3, 21, [("key10", "col")], restart=False)
