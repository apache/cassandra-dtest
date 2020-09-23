import os
import subprocess
import time
import pytest
import parse
import logging

from cassandra.util import sortedset
from ccmlib import common

from dtest import Tester
from tools.data import rows_to_list

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.2', max_version='3.0.0')
class TestTokenGenerator(Tester):
    """
    Basic tools/bin/token-generator test.
    Token-generator was removed in CASSANDRA-5261
    @jira_ticket CASSANDRA-5261
    @jira_ticket CASSANDRA-9300
    """

    def call_token_generator(self, install_dir, randomPart, nodes):
        executable = os.path.join(install_dir, 'tools', 'bin', 'token-generator')
        if common.is_win():
            executable += ".bat"

        args = [executable]
        if randomPart is not None:
            if randomPart:
                args.append("--random")
            else:
                args.append("--murmur3")

        for n in nodes:
            args.append(str(n))

        logger.debug('Invoking {}'.format(args))
        token_gen_output = subprocess.check_output(args).decode()
        lines = token_gen_output.split("\n")
        dc_tokens = None
        generated_tokens = []
        for line in lines:
            if line.startswith("DC #"):
                if dc_tokens is not None:
                    assert dc_tokens.__len__(), 0 > "dc_tokens is empty from token-generator {}".format(args)
                    generated_tokens.append(dc_tokens)
                dc_tokens = []
            else:
                if line:
                    m = parse.search('Node #{node_num:d}:{:s}{node_token:d}', line)
                    assert m, "Line \"{}\" does not match pattern from token-generator {}".format(line is not None, args)
                    node_num = int(m.named['node_num'])
                    node_token = int(m.named['node_token'])
                    dc_tokens.append(node_token)
                    assert node_num, dc_tokens.__len__() == "invalid token count from token-generator {}".format(args)
        assert dc_tokens is not None, "No tokens from token-generator {}".format(args)
        assert dc_tokens.__len__(), 0 > "No tokens from token-generator {}".format(args)
        generated_tokens.append(dc_tokens)

        return generated_tokens

    def prepare(self, randomPart=None, nodes=1):
        cluster = self.cluster

        install_dir = cluster.get_install_dir()
        generated_tokens = self.call_token_generator(install_dir, randomPart, [nodes])

        if not randomPart:
            cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
        else:
            if randomPart:
                cluster.set_partitioner("org.apache.cassandra.dht.RandomPartitioner")
            else:
                cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")

        # remove these from cluster options - otherwise node's config would be overridden with cluster._config_options_
        cluster._config_options.__delitem__('num_tokens')
        if self.dtest_config.use_vnodes:
            cluster._config_options.__delitem__('initial_token')

        assert not cluster.nodelist(), "nodelist() already initialized"
        cluster.populate(nodes, use_vnodes=False, tokens=generated_tokens[0]).start()
        time.sleep(0.2)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return generated_tokens, session

    def _token_gen_test(self, nodes, randomPart=None):
        generated_tokens, session = self.prepare(randomPart, nodes=nodes)
        dc_tokens = generated_tokens[0]

        tokens = []
        local_tokens = rows_to_list(session.execute("SELECT tokens FROM system.local"))[0]
        assert local_tokens.__len__(), 1 == "too many tokens for peer"
        for tok in local_tokens:
            tokens += tok

        rows = rows_to_list(session.execute("SELECT tokens FROM system.peers"))
        assert rows.__len__() == nodes - 1
        for row in rows:
            peer_tokens = row[0]
            assert peer_tokens.__len__(), 1 == "too many tokens for peer"
            for tok in peer_tokens:
                tokens.append(tok)

        assert tokens.__len__() == dc_tokens.__len__()
        for cluster_token in tokens:
            tok = int(cluster_token)
            assert dc_tokens.index(tok), 0 >= "token in cluster does not match generated tokens"

    def token_gen_def_test(self, nodes=3):
        """ Validate token-generator with Murmur3Partitioner with default token-generator behavior """

        self._token_gen_test(nodes)

    def token_gen_murmur3_test(self, nodes=3):
        """ Validate token-generator with Murmur3Partitioner with explicit murmur3 """

        self._token_gen_test(nodes, False)

    def token_gen_random_test(self, nodes=3):
        """ Validate token-generator with Murmur3Partitioner with explicit random """

        self._token_gen_test(nodes, True)

    dc_nodes_combinations = [
        [3, 5],
        [3, 5, 5],
        [12, 5, 7],
        [50, 100, 250],
        [100, 100, 100],
        [250, 250, 250],
        [1000, 1000, 1000],
        [2500, 2500, 2500, 2500]
    ]

    def _multi_dc_tokens(self, random=None):
        t_min = 0
        t_max = 1 << 127
        if random is None or not random:
            t_min = -1 << 63
            t_max = 1 << 63
        for dc_nodes in self.dc_nodes_combinations:
            all_tokens = sortedset()
            node_count = 0
            generated_tokens = self.call_token_generator(self.cluster.get_install_dir(), random, dc_nodes)
            assert dc_nodes.__len__() == generated_tokens.__len__()
            for n in range(0, dc_nodes.__len__()):
                nodes = dc_nodes[n]
                node_count += nodes
                tokens = generated_tokens[n]
                assert nodes == tokens.__len__()
                for tok in tokens:
                    assert t_min <= tok < t_max, "Generated token %r out of Murmur3Partitioner range %r..%r" % (tok, t_min, t_max - 1)
                    assert not all_tokens.__contains__(tok), "Duplicate token %r for nodes-counts %r" % (tok, dc_nodes)
                    all_tokens.add(tok)
            assert all_tokens.__len__() == node_count, "Number of tokens %r and number of nodes %r does not match for %r" % (all_tokens.__len__(), node_count, dc_nodes)

    def test_multi_dc_tokens_default(self):
        self._multi_dc_tokens()

    def test_multi_dc_tokens_murmur3(self):
        self._multi_dc_tokens(False)

    def test_multi_dc_tokens_random(self):
        self._multi_dc_tokens(True)
