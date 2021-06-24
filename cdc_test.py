from __future__ import division

import errno
import os
import re
import shutil
import time
import uuid
from collections import namedtuple
from itertools import repeat
from pprint import pformat

import pytest
from cassandra import WriteFailure
from cassandra.concurrent import (execute_concurrent,
                                  execute_concurrent_with_args)
from ccmlib.node import Node

from cqlsh_tests.cqlsh_tools import assert_resultset_contains
from dtest import Tester, create_ks, logger
from tools.assertions import assert_length_equal
from tools.data import rows_to_list
from tools.files import size_of_files_in_dir
from tools.funcutils import get_rate_limited_function
from tools.hacks import advance_to_next_cl_segment

since = pytest.mark.since

_16_uuid_column_spec = (
    'a uuid PRIMARY KEY, b uuid, c uuid, d uuid, e uuid, f uuid, g uuid, '
    'h uuid, i uuid, j uuid, k uuid, l uuid, m uuid, n uuid, o uuid, '
    'p uuid'
)


def _insert_rows(session, table_name, insert_stmt, values):
    prepared_insert = session.prepare(insert_stmt)
    values = list(values)  # in case values is a generator
    execute_concurrent(session, ((prepared_insert, x) for x in values),
                       concurrency=500, raise_on_first_error=True)

    data_loaded = rows_to_list(session.execute('SELECT * FROM ' + table_name))
    logger.debug('{n} rows inserted into {table_name}'.format(n=len(data_loaded), table_name=table_name))
    # use assert_equal over assert_length_equal to avoid printing out
    # potentially large lists
    assert len(values) == len(data_loaded)
    return data_loaded


def _move_commitlog_segments(source_dir, dest_dir, verbose=True):
    for source_filename in [name for name in os.listdir(source_dir) if not name.endswith('_cdc.idx')]:
        source_path, dest_path = (os.path.join(source_dir, source_filename),
                                  os.path.join(dest_dir, source_filename))
        if verbose:
            logger.debug('moving {} to {}'.format(source_path, dest_path))
        shutil.move(source_path, dest_path)


def _get_16_uuid_insert_stmt(ks_name, table_name):
    return (
        'INSERT INTO {ks_name}.{table_name} '
        '(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) '
        'VALUES (uuid(), uuid(), uuid(), uuid(), uuid(), '
        'uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), '
        'uuid(), uuid(), uuid(), uuid(), uuid())'
    ).format(ks_name=ks_name, table_name=table_name)


def _get_create_table_statement(ks_name, table_name, column_spec, options=None):
    if options:
        options_pairs = ('{k}={v}'.format(k=k, v=v) for (k, v) in options.items())
        options_string = 'WITH ' + ' AND '.join(options_pairs)
    else:
        options_string = ''

    return (
        'CREATE TABLE ' + ks_name + '.' + table_name + ' '
                                                       '(' + column_spec + ') ' + options_string
    )


def _write_to_cdc_write_failure(session, insert_stmt):
    prepared = session.prepare(insert_stmt)
    start, rows_loaded, error_found = time.time(), 0, False
    rate_limited_debug = get_rate_limited_function(logger.debug, 5)
    while not error_found:
        # We want to fail if inserting data takes too long. Locally this
        # takes about 10s, but let's be generous.
        assert ((time.time() - start) <= 600), (
                "It's taken more than 10 minutes to reach a WriteFailure trying "
                'to overrun the space designated for CDC commitlogs. This could '
                "be because data isn't being written quickly enough in this "
                'environment, or because C* is failing to reject writes when '
                'it should.')

        # If we haven't logged from here in the last 5s, do so.
        rate_limited_debug(
            '  data load step has lasted {s:.2f}s, '
            'loaded {r} rows'.format(s=(time.time() - start), r=rows_loaded))

        batch_results = list(execute_concurrent(
            session,
            ((prepared, ()) for _ in range(1000)),
            concurrency=500,
            # Don't propagate errors to the main thread. We expect at least
            # one WriteFailure, so we handle it below as part of the
            # results recieved from this method.
            raise_on_first_error=False
        ))

        # Here, we track the number of inserted values by getting the
        # number of successfully completed statements...
        rows_loaded += len([br for br in batch_results if br[0]])
        # then, we make sure that the only failures are the expected
        # WriteFailure.
        assert ([] == [result for (success, result) in batch_results
                       if not success and not isinstance(result, WriteFailure)])
        # Finally, if we find a WriteFailure, that means we've inserted all
        # the CDC data we can and so we flip error_found to exit the loop.
        if any(isinstance(result, WriteFailure) for (_, result) in batch_results):
            logger.debug("write failed (presumably because we've overrun "
                         'designated CDC commitlog space) after '
                         'loading {r} rows in {s:.2f}s'.format(
                             r=rows_loaded,
                             s=time.time() - start))
            error_found = True
    return rows_loaded


_TableInfoNamedtuple = namedtuple('TableInfoNamedtuple', [
    # required
    'ks_name', 'table_name', 'column_spec',
    # optional
    'options', 'insert_stmt',
    # derived
    'name', 'create_stmt'
])


class TableInfo(_TableInfoNamedtuple):
    __slots__ = ()

    def __new__(cls, ks_name, table_name, column_spec, options=None, insert_stmt=None):
        name = ks_name + '.' + table_name
        create_stmt = _get_create_table_statement(ks_name, table_name, column_spec, options)
        self = super(TableInfo, cls).__new__(
            cls,
            # required
            ks_name=ks_name, table_name=table_name, column_spec=column_spec,
            # optional
            options=options, insert_stmt=insert_stmt,
            # derived
            name=name, create_stmt=create_stmt
        )
        return self


def _set_cdc_on_table(session, table_name, value, ks_name=None):
    """
    Uses <session> to set CDC to <value> on <ks_name>.<table_name>.
    """
    table_string = ks_name + '.' + table_name if ks_name else table_name
    value_string = 'true' if value else 'false'
    stmt = 'ALTER TABLE ' + table_string + ' WITH CDC = ' + value_string

    logger.debug(stmt)
    session.execute(stmt)


def _get_set_cdc_func(session, ks_name, table_name):
    """
    Close over a session, keyspace name, and table name and return a function
    that takes enables CDC on that keyspace if its argument is truthy and
    otherwise disables it.
    """

    def set_cdc(value):
        return _set_cdc_on_table(
            session=session,
            ks_name=ks_name, table_name=table_name,
            value=value
        )

    return set_cdc


def _get_commitlog_files(node_path):
    commitlog_dir = os.path.join(node_path, 'commitlogs')
    return {
        os.path.join(commitlog_dir, name)
        for name in os.listdir(commitlog_dir)
    }


def _get_cdc_raw_files(node_path, cdc_raw_dir_name='cdc_raw'):
    commitlog_dir = os.path.join(node_path, cdc_raw_dir_name)
    return {
        os.path.join(commitlog_dir, name)
        for name in os.listdir(commitlog_dir)
    }


@since('3.8')
class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844
    @jira_ticket CASSANDRA-12148

    Test the correctness of some features of CDC, Change Data Capture, which
    provides a view of the commitlog on tables for which it is enabled.
    """

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns = (
            # We expect to see this error in the logs when we reach CDC limit
            r'Failed to apply mutation locally'
        )

    def _create_temp_dir(self, dir_name, verbose=True):
        """
        Create a directory that will be deleted when this test class is torn
        down.
        """
        if verbose:
            logger.debug('creating ' + dir_name)
        try:
            os.mkdir(dir_name)
        except OSError as e:
            if e.errno != errno.EEXIST:
                logger.debug(dir_name + ' already exists. removing and recreating.')
                shutil.rmtree(dir_name)
                os.mkdir(dir_name)
            else:
                raise e

        def debug_and_rmtree():
            shutil.rmtree(dir_name)
            logger.debug(dir_name + ' removed')

        self.addCleanup(debug_and_rmtree)

    def prepare(self, ks_name,
                table_name=None, cdc_enabled_table=None,
                gc_grace_seconds=None,
                column_spec=None,
                configuration_overrides=None,
                table_id=None):
        """
        Create a 1-node cluster, start it, create a keyspace, and if
        <table_name>, create a table in that keyspace. If <cdc_enabled_table>,
        that table is created with CDC enabled. If <column_spec>, use that
        string to specify the schema of the table -- for example, a valid value
        is 'a int PRIMARY KEY, b int'. The <configuration_overrides> is
        treated as a dict-like object and passed to
        self.cluster.set_configuration_options.
        """
        config_defaults = {
            'cdc_enabled': True,
            # we want to be able to generate new segments quickly
            'commitlog_segment_size_in_mb': 2,
        }
        if configuration_overrides is None:
            configuration_overrides = {}
        self.cluster.populate(1)
        self.cluster.set_configuration_options(dict(config_defaults, **configuration_overrides))
        self.cluster.start()
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        create_ks(session, ks_name, rf=1)

        if table_name is not None:
            assert cdc_enabled_table is not None, 'if creating a table in prepare, must specify whether or not CDC is enabled on it'
            assert column_spec is not None, 'if creating a table in prepare, must specify its schema'
            options = {}
            if gc_grace_seconds is not None:
                options['gc_grace_seconds'] = gc_grace_seconds
            if table_id is not None:
                options['id'] = table_id
            if cdc_enabled_table:
                options['cdc'] = 'true'
            stmt = _get_create_table_statement(
                ks_name, table_name, column_spec,
                options=options
            )
            logger.debug(stmt)
            session.execute(stmt)

        return node, session

    def _assert_cdc_data_readable_on_round_trip(self, start_with_cdc_enabled):
        """
        Parameterized test asserting that data written to a table is still
        readable after flipping the CDC flag on that table, then flipping it
        again. Starts with CDC enabled if start_with_cdc_enabled, otherwise
        starts with it disabled.
        """
        ks_name, table_name = 'ks', 'tab'
        sequence = [True, False, True] if start_with_cdc_enabled else [False, True, False]
        start_enabled, alter_path = sequence[0], list(sequence[1:])

        node, session = self.prepare(ks_name=ks_name, table_name=table_name,
                                     cdc_enabled_table=start_enabled,
                                     column_spec='a int PRIMARY KEY, b int')
        set_cdc = _get_set_cdc_func(session=session, ks_name=ks_name, table_name=table_name)

        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')
        # data = zip(list(range(1000)), list(range(1000)))
        start = 0
        stop = 1000
        step = 1
        data = [(n, min(n+step, stop)) for n in range(start, stop, step)]

        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        assert [] == list(node.get_sstables(ks_name, table_name))

        for enable in alter_path:
            set_cdc(enable)
            assert_resultset_contains(session.execute('SELECT * FROM ' + table_name), data)

    def test_cdc_enabled_data_readable_on_round_trip(self):
        """
        Test that data is readable after an enabled->disabled->enabled round
        trip.
        """
        self._assert_cdc_data_readable_on_round_trip(start_with_cdc_enabled=True)

    def test_cdc_disabled_data_readable_on_round_trip(self):
        """
        Test that data is readable after an disabled->enabled->disabled round
        trip.
        """
        self._assert_cdc_data_readable_on_round_trip(start_with_cdc_enabled=False)

    def test_non_cdc_segments_deleted_after_replay(self):
        """
        Test that non-cdc segment files generated in previous runs are deleted
        after replay.
        """
        ks_name, table_name = 'ks', 'tab'
        node, session = self.prepare(ks_name=ks_name, table_name=table_name,
                                     cdc_enabled_table=True,
                                     column_spec='a int PRIMARY KEY, b int')
        old_files = _get_cdc_raw_files(node.get_path())
        node.drain()
        session.cluster.shutdown()
        node.stop()
        node.start(wait_for_binary_proto=True)
        new_files = _get_cdc_raw_files(node.get_path())
        assert len(old_files.intersection(new_files)) == 0

    def test_insertion_and_commitlog_behavior_after_reaching_cdc_total_space(self):
        """
        Test that C* behaves correctly when CDC tables have consumed all the
        space available to them. In particular: after writing
        cdc_total_space_in_mb MB into CDC commitlogs:
        - CDC writes are rejected
        - non-CDC writes are accepted
        - on flush, CDC commitlogs are copied to cdc_raw
        - on flush, non-CDC commitlogs are not copied to cdc_raw
        This is a lot of behavior to validate in one test, but we do so to
        avoid running multiple tests that each write 1MB of data to fill
        cdc_total_space_in_mb.
        """
        ks_name = 'ks'
        full_cdc_table_info = TableInfo(
            ks_name=ks_name, table_name='full_cdc_tab',
            column_spec=_16_uuid_column_spec,
            insert_stmt=_get_16_uuid_insert_stmt(ks_name, 'full_cdc_tab'),
            options={'cdc': 'true'}
        )

        configuration_overrides = {
            # Make CDC space as small as possible so we can fill it quickly.
            'cdc_total_space_in_mb': 4,
        }
        node, session = self.prepare(
            ks_name=ks_name,
            configuration_overrides=configuration_overrides
        )
        session.execute(full_cdc_table_info.create_stmt)

        # Later, we'll also make assertions about the behavior of non-CDC
        # tables, so we create one here.
        non_cdc_table_info = TableInfo(
            ks_name=ks_name, table_name='non_cdc_tab',
            column_spec=_16_uuid_column_spec,
            insert_stmt=_get_16_uuid_insert_stmt(ks_name, 'non_cdc_tab')
        )
        session.execute(non_cdc_table_info.create_stmt)
        # We'll also make assertions about the behavior of CDC tables when
        # other CDC tables have already filled the designated space for CDC
        # commitlogs, so we create the second CDC table here.
        empty_cdc_table_info = TableInfo(
            ks_name=ks_name, table_name='empty_cdc_tab',
            column_spec=_16_uuid_column_spec,
            insert_stmt=_get_16_uuid_insert_stmt(ks_name, 'empty_cdc_tab'),
            options={'cdc': 'true'}
        )
        session.execute(empty_cdc_table_info.create_stmt)

        # Here, we insert values into the first CDC table until we get a
        # WriteFailure. This should happen when the CDC commitlogs take up 1MB
        # or more.
        logger.debug('flushing non-CDC commitlogs')
        node.flush()
        # Then, we insert rows into the CDC table until we can't anymore.
        logger.debug('beginning data insert to fill CDC commitlogs')
        rows_loaded = _write_to_cdc_write_failure(session, full_cdc_table_info.insert_stmt)

        assert 0 < rows_loaded, ('No CDC rows inserted. This may happen when '
                                 'cdc_total_space_in_mb > commitlog_segment_size_in_mb')

        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        commitlogs_size = size_of_files_in_dir(commitlog_dir)
        logger.debug('Commitlog dir ({d}) is {b}B'.format(d=commitlog_dir, b=commitlogs_size))

        # We should get a WriteFailure when trying to write to the CDC table
        # that's filled the designated CDC space...
        try:
            session.execute(full_cdc_table_info.insert_stmt)
            raise Exception("WriteFailure expected")
        except WriteFailure:
            pass
        # or any CDC table.
        try:
            session.execute(empty_cdc_table_info.insert_stmt)
            raise Exception("WriteFailure expected")
        except WriteFailure:
            pass

        # Now we test for behaviors of non-CDC tables when we've exceeded
        # cdc_total_space_in_mb.
        #
        # First, we drain and save the names of all the new discarded CDC
        # segments
        node.drain()
        session.cluster.shutdown()
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        pre_non_cdc_write_cdc_raw_segments = _get_cdc_raw_files(node.get_path())

        # Snapshot the _cdc.idx file if > 4.0 for comparison at end
        before_cdc_state = []  # init empty here to quiet PEP
        if self.cluster.version() >= '4.0':
            # Create ReplayData objects for each index file found in loading cluster
            node1_path = os.path.join(node.get_path(), 'cdc_raw')
            before_cdc_state = [ReplayData.load(node1_path, name)
                                for name in os.listdir(node1_path) if name.endswith('_cdc.idx')]

        # save the names of all the commitlog segments written up to this
        # point:
        pre_non_cdc_write_segments = _get_commitlog_files(node.get_path())

        # Check that writing to non-CDC tables succeeds even when writes to CDC
        # tables are rejected:
        non_cdc_prepared_insert = session.prepare(non_cdc_table_info.insert_stmt)
        session.execute(non_cdc_prepared_insert, ())  # should not raise an exception

        # Check the following property: any new commitlog segments written to
        # after cdc_raw has reached its maximum configured size should not be
        # moved to cdc_raw, on commitlog discard, because any such commitlog
        # segments are written to non-CDC tables.
        #
        # First, write to non-cdc tables.
        start, time_limit = time.time(), 600
        rate_limited_debug = get_rate_limited_function(logger.debug, 5)
        logger.debug('writing to non-cdc table')
        # We write until we get a new commitlog segment.
        while _get_commitlog_files(node.get_path()) <= pre_non_cdc_write_segments:
            elapsed = time.time() - start
            rate_limited_debug('  non-cdc load step has lasted {s:.2f}s'.format(s=elapsed))
            assert elapsed <= time_limit, "It's been over a {s}s and we haven't written a new " \
                                           "commitlog segment. Something is wrong.".format(s=time_limit)
            execute_concurrent(
                session,
                ((non_cdc_prepared_insert, ()) for _ in range(1000)),
                concurrency=500,
                raise_on_first_error=True,
            )

        # Finally, we check that draining doesn't move any new segments to cdc_raw:
        node.drain()
        session.cluster.shutdown()

        if self.cluster.version() < '4.0':
            assert pre_non_cdc_write_cdc_raw_segments == _get_cdc_raw_files(node.get_path())
        else:
            # Create ReplayData objects for each index file found in loading cluster
            node2_path = os.path.join(node.get_path(), 'cdc_raw')
            after_cdc_state = [ReplayData.load(node2_path, name)
                               for name in os.listdir(node2_path) if name.endswith('_cdc.idx')]

            # Confirm all indexes in 1st are accounted for and match corresponding entry in 2nd.
            found = True
            for idx in before_cdc_state:
                idx_found = False
                for idx_two in after_cdc_state:
                    if compare_replay_data(idx, idx_two):
                        idx_found = True
                if not idx_found:
                    found = False
                    break
            if not found:
                self._fail_and_print_sets(before_cdc_state, after_cdc_state,
                                          'Found CDC index in before not matched in after (non-CDC write test)')

            # Now we confirm we don't have anything that showed up in 2nd not accounted for in 1st
            orphan_found = False
            for idx_two in after_cdc_state:
                index_found = False
                for idx in before_cdc_state:
                    if compare_replay_data(idx_two, idx):
                        index_found = True
                if not index_found:
                    orphan_found = True
                    break
            if orphan_found:
                self._fail_and_print_sets(before_cdc_state, after_cdc_state,
                                          'Found orphaned index file in after CDC state not in former.')

    def _fail_and_print_sets(self, rd_one, rd_two, msg):
        print('Set One:')
        for idx in rd_one:
            print('   {},{},{},{}'.format(idx.name, idx.completed, idx.offset, idx.log_name))
        print('Set Two:')
        for idx_two in rd_two:
            print('   {},{},{},{}'.format(idx_two.name, idx_two.completed, idx_two.offset, idx_two.log_name))
        pytest.fail(msg)

    def _init_new_loading_node(self, ks_name, create_stmt, use_thrift=False):
        loading_node = Node(
            name='node2',
            cluster=self.cluster,
            auto_bootstrap=False,
            thrift_interface=('127.0.0.2', 9160) if use_thrift else None,
            storage_interface=('127.0.0.2', 7000),
            jmx_port='7400',
            remote_debug_port='0',
            initial_token=None,
            binary_interface=('127.0.0.2', 9042)
        )
        logger.debug('adding node')
        self.cluster.add(loading_node, is_seed=True, data_center="dc1")
        logger.debug('starting new node')
        loading_node.start(wait_for_binary_proto=120)
        logger.debug('recreating ks and table')
        loading_session = self.patient_exclusive_cql_connection(loading_node)
        create_ks(loading_session, ks_name, rf=1)
        logger.debug('creating new table')
        loading_session.execute(create_stmt)
        logger.debug('stopping new node')
        loading_session.cluster.shutdown()
        loading_node.stop()
        return loading_node

    def test_cdc_data_available_in_cdc_raw(self):
        ks_name = 'ks'
        # First, create a new node just for data generation.
        generation_node, generation_session = self.prepare(ks_name=ks_name)

        cdc_table_info = TableInfo(
            ks_name=ks_name, table_name='cdc_tab',
            column_spec=_16_uuid_column_spec,
            insert_stmt=_get_16_uuid_insert_stmt(ks_name, 'cdc_tab'),
            options={
                'cdc': 'true',
                # give table an explicit id so when we create it again it's the
                # same table and we can replay into it
                'id': uuid.uuid4()
            }
        )

        # Write until we get a new CL segment to avoid replaying initialization
        # mutations from this node's startup into system tables in the other
        # node. See CASSANDRA-11811.
        advance_to_next_cl_segment(
            session=generation_session,
            commitlog_dir=os.path.join(generation_node.get_path(), 'commitlogs')
        )

        generation_session.execute(cdc_table_info.create_stmt)

        # insert 10000 rows
        inserted_rows = _insert_rows(generation_session, cdc_table_info.name, cdc_table_info.insert_stmt,
                                     repeat((), 10000))

        # drain the node to guarantee all cl segments will be recycled
        logger.debug('draining')
        generation_node.drain()
        logger.debug('stopping')
        # stop the node and clean up all sessions attached to it
        generation_session.cluster.shutdown()
        generation_node.stop()

        # We can rely on the existing _cdc.idx files to determine which .log files contain cdc data.
        source_path = os.path.join(generation_node.get_path(), 'cdc_raw')
        source_cdc_indexes = {ReplayData.load(source_path, name)
                              for name in source_path if name.endswith('_cdc.idx')}
        # assertNotEqual(source_cdc_indexes, {})
        assert source_cdc_indexes != {}

        # create a new node to use for cdc_raw cl segment replay
        loading_node = self._init_new_loading_node(ks_name, cdc_table_info.create_stmt, self.cluster.version() < '4')

        # move cdc_raw contents to commitlog directories, then start the
        # node again to trigger commitlog replay, which should replay the
        # cdc_raw files we moved to commitlogs into memtables.
        logger.debug('moving cdc_raw and restarting node')
        _move_commitlog_segments(
            os.path.join(generation_node.get_path(), 'cdc_raw'),
            os.path.join(loading_node.get_path(), 'commitlogs')
        )
        loading_node.start(wait_for_binary_proto=120)
        logger.debug('node successfully started; waiting on log replay')
        loading_node.grep_log('Log replay complete')
        logger.debug('log replay complete')

        # final assertions
        validation_session = self.patient_exclusive_cql_connection(loading_node)
        data_in_cdc_table_after_restart = rows_to_list(
            validation_session.execute('SELECT * FROM ' + cdc_table_info.name)
        )
        logger.debug('found {cdc} values in CDC table'.format(
            cdc=len(data_in_cdc_table_after_restart)
        ))

        # Then we assert that the CDC data that we expect to be there is there.
        # All data that was in CDC tables should have been copied to cdc_raw,
        # then used in commitlog replay, so it should be back in the cluster.
        assert (inserted_rows == data_in_cdc_table_after_restart), 'not all expected data selected'

        if self.cluster.version() >= '4.0':
            # Create ReplayData objects for each index file found in loading cluster
            loading_path = os.path.join(loading_node.get_path(), 'cdc_raw')
            dest_cdc_indexes = [ReplayData.load(loading_path, name)
                                for name in os.listdir(loading_path) if name.endswith('_cdc.idx')]

            # Compare source replay data to dest to ensure replay process created both hard links and index files.
            for srd in source_cdc_indexes:
                # Confirm both log and index are in dest
                assert os.path.isfile(os.path.join(loading_path, srd.idx_name))
                assert os.path.isfile(os.path.join(loading_path, srd.log_name))

                # Find dest ReplayData that corresponds to the source (should be exactly 1)
                corresponding_dest_replay_datae = [x for x in dest_cdc_indexes
                                                   if srd.idx_name == x.idx_name]
                assert_length_equal(corresponding_dest_replay_datae, 1)
                drd = corresponding_dest_replay_datae[0]

                # We can't compare equality on offsets since replay uses the raw file length as the written
                # cdc offset. We *can*, however, confirm that the offset in the replayed file is >=
                # the source file, ensuring clients are signaled to replay at least all the data in the
                # log.
                assert drd.offset >= srd.offset

                # Confirm completed flag is the same in both
                assert srd.completed == drd.completed

            # Confirm that the relationship between index files on the source
            # and destination looks like we expect.
            # First, grab the mapping between the two, make sure it's a 1-1
            # mapping, and transform the dict to reflect that:
            src_to_dest_idx_map = {
                src_rd: [dest_rd for dest_rd in dest_cdc_indexes
                         if dest_rd.idx_name == src_rd.idx_name]
                for src_rd in source_cdc_indexes
            }
            for src_rd, dest_rds in src_to_dest_idx_map.items():
                assert_length_equal(dest_rds, 1)
                src_to_dest_idx_map[src_rd] = dest_rds[0]
            # All offsets in idx files that were copied should be >0 on the
            # destination node.
            assert (
                0 not in {i.offset for i in src_to_dest_idx_map.values()}),\
                ('Found index offsets == 0 in an index file on the '
                 'destination node that corresponds to an index file on the '
                 'source node:\n'
                 '{}').format(pformat(src_to_dest_idx_map))
            # Offsets of all shared indexes should be >= on the destination
            # than on the source.
            for src_rd, dest_rd in src_to_dest_idx_map.items():
                assert dest_rd.offset >= src_rd.offset

            src_to_dest_idx_map = {
                src_rd: [dest_rd for dest_rd in dest_cdc_indexes
                         if dest_rd.idx_name == src_rd.idx_name]
                for src_rd in source_cdc_indexes
            }
            for k, v in src_to_dest_idx_map.items():
                assert_length_equal(v, 1)
                assert k.offset >= v.offset


def compare_replay_data(rd_one, rd_two):
    return rd_one.idx_name == rd_two.idx_name and \
           rd_one.completed == rd_two.completed and \
           rd_one.offset == rd_two.offset and \
           rd_one.log_name == rd_two.log_name


class ReplayData(namedtuple('ReplayData', ['idx_name', 'completed', 'offset', 'log_name'])):
    """
    Replay data class containing data from a _cdc.idx file. Build one with the load method.
    """

    @classmethod
    def load(cls, path, name):
        assert '_cdc' in name, 'expected to find _cdc in passed in index name. Did not: ' + name
        with open(os.path.join(path, name), 'r') as f:
            offset, completed = [line.strip() for line in f.readlines()]

        return cls(
            idx_name=name,
            completed=completed,
            offset=int(offset),
            log_name=re.sub('_cdc.idx', '.log', name)
        )
