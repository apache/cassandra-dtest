# coding: utf-8

import os
import re
import subprocess
import time

from cassandra.util import sortedset
from ccmlib import common

from dtest import Tester, debug
from tools import rows_to_list, since


@since('2.0.16', max_version='3.0.0')
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

        debug('Invoking {}'.format(args))
        token_gen_output = subprocess.check_output(args)
        lines = token_gen_output.split("\n")
        dc_tokens = None
        generated_tokens = []
        for line in lines:
            if line.startswith("DC #"):
                if dc_tokens is not None:
                    self.assertGreater(dc_tokens.__len__(), 0, "dc_tokens is empty from token-generator %r" % args)
                    generated_tokens.append(dc_tokens)
                dc_tokens = []
            else:
                if line.__len__() > 0:
                    m = re.search("^  Node #(\d+): [ ]*([-]?\d+)$", line)
                    self.assertIsNotNone(m, "Line \"%r\" does not match pattern from token-generator %r" % (line, args))
                    node_num = int(m.group(1))
                    node_token = int(m.group(2))
                    dc_tokens.append(node_token)
                    self.assertEqual(node_num, dc_tokens.__len__(), "invalid token count from token-generator %r" % args)
        self.assertIsNotNone(dc_tokens, "No tokens from token-generator %r" % args)
        self.assertGreater(dc_tokens.__len__(), 0, "No tokens from token-generator %r" % args)
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
        cluster._config_options.__delitem__('initial_token')

        self.assertTrue(not cluster.nodelist(), "nodelist() already initialized")
        cluster.populate(nodes, use_vnodes=False, tokens=generated_tokens[0]).start(wait_for_binary_proto=True)
        time.sleep(0.2)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return generated_tokens, session

    def _token_gen_test(self, nodes, randomPart=None):
        generated_tokens, session = self.prepare(randomPart, nodes=nodes)
        dc_tokens = generated_tokens[0]

        tokens = []
        local_tokens = rows_to_list(session.execute("SELECT tokens FROM system.local"))[0]
        self.assertEqual(local_tokens.__len__(), 1, "too many tokens for peer")
        for tok in local_tokens:
            tokens += tok

        rows = rows_to_list(session.execute("SELECT tokens FROM system.peers"))
        self.assertEqual(rows.__len__(), nodes - 1)
        for row in rows:
            peer_tokens = row[0]
            self.assertEqual(peer_tokens.__len__(), 1, "too many tokens for peer")
            for tok in peer_tokens:
                tokens.append(tok)

        self.assertEqual(tokens.__len__(), dc_tokens.__len__())
        for cluster_token in tokens:
            tok = int(cluster_token)
            self.assertGreaterEqual(dc_tokens.index(tok), 0, "token in cluster does not match generated tokens")

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
            self.assertEqual(dc_nodes.__len__(), generated_tokens.__len__())
            for n in range(0, dc_nodes.__len__()):
                nodes = dc_nodes[n]
                node_count += nodes
                tokens = generated_tokens[n]
                self.assertEqual(nodes, tokens.__len__())
                for tok in tokens:
                    self.assertTrue(t_min <= tok < t_max, "Generated token %r out of Murmur3Partitioner range %r..%r" % (tok, t_min, t_max - 1))
                    self.assertTrue(not all_tokens.__contains__(tok), "Duplicate token %r for nodes-counts %r" % (tok, dc_nodes))
                    all_tokens.add(tok)
            self.assertEqual(all_tokens.__len__(), node_count, "Number of tokens %r and number of nodes %r does not match for %r" % (all_tokens.__len__(), node_count, dc_nodes))

    def multi_dc_tokens_default_test(self):
        self._multi_dc_tokens()

    def multi_dc_tokens_murmur3_test(self):
        self._multi_dc_tokens(False)

    def multi_dc_tokens_random_test(self):
        self._multi_dc_tokens(True)
