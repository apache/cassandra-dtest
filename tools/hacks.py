"""
This one's called hacks because it provides shared utilities to hack around
weirdnesses in Cassandra.
"""
import os
import time
import logging

from cassandra.concurrent import execute_concurrent

from tools.funcutils import get_rate_limited_function

logger = logging.getLogger(__name__)


def _files_in(directory):
    return {
        os.path.join(directory, name) for name in os.listdir(directory)
    }


def advance_to_next_cl_segment(session, commitlog_dir,
                               keyspace_name='ks', table_name='junk_table',
                               timeout=180):
    """
    This is a hack to work around problems like CASSANDRA-11811.

    The problem happens in commitlog-replaying tests, like the snapshot and CDC
    tests. If we replay the first commitlog that's created, we wind up
    replaying some mutations that initialize system tables, so this function
    advances the node to the next CL by filling up the first one.
    """
    session.execute(
        'CREATE TABLE {ks}.{tab} ('
        'a uuid PRIMARY KEY, b uuid, c uuid, d uuid, '
        'e uuid, f uuid, g uuid, h uuid'
        ')'.format(ks=keyspace_name, tab=table_name)
    )
    prepared_insert = session.prepare(
        'INSERT INTO {ks}.{tab} '
        '(a, b, c, d, e, f, g, h) '
        'VALUES ('
        'uuid(), uuid(), uuid(), uuid(), '
        'uuid(), uuid(), uuid(), uuid()'
        ')'.format(ks=keyspace_name, tab=table_name)
    )

    # record segments that we want to advance past
    initial_cl_files = _files_in(commitlog_dir)

    start = time.time()
    stop_time = start + timeout
    rate_limited_debug_logger = get_rate_limited_function(logger.debug, 5)
    logger.debug('attempting to write until we start writing to new CL segments: {}'.format(initial_cl_files))

    while _files_in(commitlog_dir) <= initial_cl_files:
        elapsed = time.time() - start
        rate_limited_debug_logger('  commitlog-advancing load step has lasted {s:.2f}s'.format(s=elapsed))
        assert (
            time.time() <= stop_time), ("It's been over {s}s and we haven't written a new " +
                                        "commitlog segment. Something is wrong.").format(s=timeout)
        execute_concurrent(
            session,
            ((prepared_insert, ()) for _ in range(1000)),
            concurrency=500,
            raise_on_first_error=True,
        )

    logger.debug('present commitlog segments: {}'.format(_files_in(commitlog_dir)))
