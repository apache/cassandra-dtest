import re
import time
import pytest
import logging

from ccmlib.node import TimeoutError
from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestSchemaAgreementUpgrade(Tester):
    """
    Verifies that upgrades do not produce migration storms.

    For upgrades from 3.0 to 3.x we have encountered that the beloved 'cdc' column
    caused rolling upgrades to produce migration storms, because the schema digest calculated
    by 3.11 differs from that in 3.0, because 3.11 included the 'cdc' column in the digest
    calculation.

    This test verifies that (after some grace time, i.e. nodes in status NORMAL) neither the
    upgraded node nor the other nodes perform schema pulls (migration tasks) resulting in a
    "migration storm".

    A few schema migrations however happen before all nodes are in status NORMAL. This is not ideal,
    but legit. In an ideal world, those should not happen - but we would have to do the schema
    changes atomically - and currently there is just nothing like a concurrent schema change.

    The tests run for a quite some time, about 5 minutes per test.
    But there's some debug output for your entertainment - enjoy.

    The node.start/stop calls pass wait_other_notice=False intentionally, because there seem to
    be issues in 3.0 to thoroughly detect and _log_ the status change - i.e. the wait_other_notice
    check doesn't seem to be reliable - at least not in this test.
    """

    # The number of seconds we wait for schema migration log entries to verify
    migration_check_time = 30

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        )

    def _prepare(self, version, num_nodes=3):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version=version)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        cluster.populate(num_nodes).start()

        return cluster

    def _set_verify_log_mark(self, nodes):
        for node in nodes:
            node.verify_log_mark = node.mark_log(filename='debug.log')

    def _expect_no_schema_migrations(self, nodes):
        """
        Inspects the debug.log files from the given nodes whether any log about schema migration is present.
        """

        # Verify that there are _no_ log messages like:
        expressions = [" - [pP]ulling schema from endpoint",
                       " - [Ss]ubmitting migration task",
                       " - [Pp]ulled schema from endpoint"]
        logger.debug("Inspecting log files of {}...".format([n.name for n in nodes]))
        all_matchings = ""
        for node in nodes:
            try:
                matchings = node.watch_log_for(expressions, from_mark=node.verify_log_mark, timeout=0, filename='debug.log')
                all_matchings = all_matchings + "\n{}: {}".format(node.name, matchings)
            except TimeoutError:
                # good
                logger.debug("  {}: log files don't show schema migration messages (good)".format(node.name))
        if all_matchings != "":
            msg = "Expected no schema migration log entries, but got:{}".format(all_matchings)
            logger.debug(msg)  # debug message for the validation test case (3.0 vs 3.11.1)
            pytest.fail(msg)

    def _wait_for_status_normal(self, node, mark):
        # Wait until the node is in state NORMAL (otherwise we can expect
        # schema migrations for the first upgraded node).
        node.watch_log_for("Node /{} state jump to NORMAL".format(node.address()),
                           from_mark=mark, timeout=300, filename='debug.log')

    def _bounce_node(self, node):
        logger.debug("Bouncing {}...".format(node.name))
        logger.debug("  Stopping...")
        node.stop(wait_other_notice=False)  # intentionally set to wait_other_notice=False
        mark = node.mark_log(filename='debug.log')
        logger.debug("  Starting...")
        node.start(wait_other_notice=False)  # intentionally set to wait_other_notice=False
        logger.debug("  Waiting for status NORMAL...")
        self._wait_for_status_normal(node, mark)

    def _min_version(self, nodes):
        """
        Retrieve the minimum major version (x.y) the given nodes run.
        """

        min_version = 99.9
        for node in nodes:
            short_version = node.get_base_cassandra_version()
            logger.debug("{} is on {} ({})".format(node.name, short_version, node.get_cassandra_version()))
            if short_version < min_version:
                min_version = short_version
        return min_version

    def _upgrade_schema_agreement_test(self, upgrade_path):
        """
        Test the upgrade though the specified versions and verify that there is schema agreement.
        In theory, upgrading a 2-node cluster and verifying that might be sufficient, but the intention
        of this test is also to catch potential edge cases. Therefore it uses a 3-node cluster and
        bounces the upgraded nodes for an additional verification.

        The most effective way to check for schema migration is to inspect the debug log file for
        related messages. However, since schema migrations are normal during upgrades and may happen
        in a lazy fashion (e.g. potentially delayed by migration-interval), it does not work reliably -
        i.e. the test would depend heavily on the test environment (hardware).

        Therefore, at most points, we can only efficiently check the schema versions as shown via
        'nodetool describecluster'.

        If these tests become flaky, tuning the (initial) migration-delay should help.

        :param upgrade_path: two-dimensional array containing the versions and whether each version
                             logs a '"Gossiping my * schema version' instead of the usual 'Gossiping my
                             schema version' message, which is only true for 3.11.2 (but not 4.0).
        """

        # prepare the cluster with initial version from the upgrade path
        logger.debug('Starting upgrade test with {}'.format(upgrade_path[0][1]))
        cluster = self._prepare(version=upgrade_path[0][1])

        nodes = self.cluster.nodelist()

        # perform _rolling_ upgrades from one version to another
        for (gossip_log_with_product_version, version) in upgrade_path[1:]:
            logger.debug("")
            logger.debug("Upgrading cluster to {}".format(version))
            cluster.set_install_dir(version=version)
            self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

            for node in nodes:
                other_nodes = [n for n in nodes if n != node]

                logger.debug("")
                logger.debug("Stopping {} for upgrade...".format(node.name))
                # needed to "patch" the config file (especially since 4.0) and get the correct version number
                node.set_install_dir(version=version)
                node.stop(wait_other_notice=False)  # intentionally set to wait_other_notice=False

                # remember the logfile-mark when the node was upgraded
                upgrade_log_mark = node.mark_log(filename='debug.log')
                logger.debug("Starting upgraded {}...".format(node.name))
                node.start(wait_other_notice=False)  # intentionally set to wait_other_notice=False

                # wait until the upgraded node is in status NORMAL
                self._wait_for_status_normal(node, upgrade_log_mark)

                # If it's a 3.11.2 node, check that the correct schema version is announced
                min_version = self._min_version(nodes)
                logger.debug("Minimum version: {}".format(min_version))
                if gossip_log_with_product_version:
                    # 3.11.2 nodes (and only 3.11.2) indicate whether they announce
                    # a "3.0 compatible" or "real" "3.11" schema version.
                    watch_part = "Gossiping my {} schema version".format("3.0 compatible" if min_version == 3.0 else "3.11")
                    logger.debug("Inspecting log for '{}'...".format(watch_part))
                    matchings = node.watch_log_for(watch_part, from_mark=upgrade_log_mark, timeout=120, filename='debug.log')
                    logger.debug("  Found: {}".format(matchings))

                # Only log the schema information for debug purposes here. Primarily want to catch the
                # schema migration race.
                for n in nodes:
                    out, _, _ = n.nodetool("describecluster")
                    logger.debug("nodetool describecluster of {}:".format(n.name))
                    logger.debug(out)

                # We expect no schema migrations at this point.
                self._set_verify_log_mark(other_nodes)
                logger.debug("  Sleep for {} seconds...".format(self.migration_check_time))
                time.sleep(self.migration_check_time)
                self._expect_no_schema_migrations(other_nodes)

                # Try to trigger the schema migration race by bouncing the upgraded node.
                # Bouncing a nodes causes a new gossip-digest-'generation', which in turn causes
                # the whole endpoint state to propagate - including the schema version, which, in theory,
                # should trigger the race.
                # It is expected, that the _other_ nodes do not try to pull the schema.
                logger.debug("")
                logger.debug("Try to trigger schema migration race by bouncing the upgraded node")
                self._bounce_node(node)
                self._set_verify_log_mark(other_nodes)
                logger.debug("  Sleep for {} seconds...".format(self.migration_check_time))
                time.sleep(self.migration_check_time)
                self._expect_no_schema_migrations(other_nodes)

                # Even if it was impossible to trigger a schema version race, compare the schema versions.
                # Kind of a last resort. Sometimes it's difficult to trigger the race. But we know that we
                # only want to have one schema version.
                for n in nodes:
                    out, _, _ = n.nodetool("describecluster")
                    logger.debug("nodetool describecluster of {}:".format(n.name))
                    logger.debug(out)
                    versions = out.split('Schema versions:')[1].strip()
                    num_schemas = len(re.findall(r'\[.*?\]', versions))
                    self.assertEqual(num_schemas, 1, "Multiple schema versions detected on {}: {}".format(n.name, out))

    def upgrade_schema_agreement_30_3112_test(self):
        """
        Test the upgrade from 3.0.latest to 3.11.2 and
        verify schema agreement and no further migrations.
        """
        self._upgrade_schema_agreement_test(upgrade_path=[[False, 'alias:apache/cassandra-3.0'],
                                                          [True, 'alias:apache/cassandra-3.11']])

    def upgrade_schema_agreement_3111_3112_test(self):
        """
        Test the upgrade from 3.11.1 to 3.11.2 and
        verify schema agreement and no further migrations.
        """
        self._upgrade_schema_agreement_test(upgrade_path=[[False, 'alias:apache/cassandra-3.11.1'],
                                                          [True, 'alias:apache/cassandra-3.11']])

    def upgrade_schema_agreement_30latest_3111_test(self):
        """
        Cross-check that the dtest still works (it expects the assertion on the log files).

        If this test becomes flaky, check whether the time to verify (self.migration_check_time) the log files is
        still good enough. However, as long as the vast majority of runs of this tests succeeds, the functionality
        itself works.
        """
        with self.assertRaises(AssertionError, msg="Expected no schema migration log entries for the last {} seconds".format(self.migration_check_time)):
            self._upgrade_schema_agreement_test(upgrade_path=[[False, 'alias:apache/cassandra-3.0'],
                                                              [False, 'alias:apache/cassandra-3.11.1']])

    # REVIEWER NOTICE:
    # The following tests should not make it into the dtest repo. The above tests using cassandra-3.11 cannot work
    # until the corresponding is committed.

    def upgrade_schema_agreement_ok30_test(self):
        """
        Test the upgrade from 3.0 to 3.11.2 and verify that there is no migration storm.

        TO BE CLEAR: THIS TEST IS INTENDED TO FAIL AND NOT INTENDED TO BE COMMITTED!
        """
        self._upgrade_schema_agreement_test(upgrade_path=[[False, 'alias:apache/cassandra-3.0'],
                                                          [True, 'alias:snazy/schema-migration-upgrade-bug-trunk']])

    def upgrade_schema_agreement_ok3112_test(self):
        """
        Test the upgrade from 3.0 to 3.11.2 and verify that there is no migration storm.

        TO BE CLEAR: THIS TEST IS INTENDED TO FAIL AND NOT INTENDED TO BE COMMITTED!
        """
        self._upgrade_schema_agreement_test(upgrade_path=[[False, 'alias:apache/cassandra-3.11'],
                                                          [True, 'alias:snazy/schema-migration-upgrade-bug-3.11']])
