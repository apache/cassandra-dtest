import fileinput
import functools
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from distutils.version import LooseVersion
from threading import Thread

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from nose.plugins.attrib import attr

from ccmlib.node import Node
from dtest import CASSANDRA_DIR, DISABLE_VNODES, IGNORE_REQUIRE, debug


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def create_c1c2_table(tester, session, read_repair=None):
    tester.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'}, read_repair=read_repair)


def insert_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))
    if n:
        keys = list(range(n))

    statement = session.prepare("INSERT INTO cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in keys])


def query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False):
    query = SimpleStatement('SELECT c1, c2 FROM cf WHERE key=\'k%d\'' % key, consistency_level=consistency)
    rows = list(session.execute(query))
    if not tolerate_missing:
        assert len(rows) == 1
        res = rows[0]
        assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2', res
    if must_be_missing:
        assert len(rows) == 0


# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None):
    i = len(cluster.nodes) + 1
    node = Node('node%s' % i,
                cluster,
                bootstrap,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                remote_debug_port,
                token,
                binary_interface=('127.0.0.%s' % i, 9042))
    cluster.add(node, not bootstrap, data_center=data_center)
    return node


def insert_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    upds = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%06d\'" % (i, key, i) for i in xrange(offset * columns_count, columns_count * (offset + 1))]
    query = 'BEGIN BATCH %s; APPLY BATCH' % '; '.join(upds)
    simple_query = SimpleStatement(query, consistency_level=consistency)
    session.execute(simple_query)


def query_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k%s\' AND c >= \'c%06d\' AND c <= \'c%06d\'' % (key, offset, columns_count + offset - 1), consistency_level=consistency)
    res = list(session.execute(query))
    assert len(res) == columns_count, "%s != %s (%s-%s)" % (len(res), columns_count, offset, columns_count + offset - 1)
    for i in xrange(0, columns_count):
        assert res[i][1] == 'value%d' % (i + offset)


def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)


# Simple puts and get (on one row), testing both reads by names and by slice,
# with overwrites and flushes between inserts to make sure we hit multiple
# sstables on reads
def putget(cluster, session, cl=ConsistencyLevel.QUORUM):

    _put_with_overwrite(cluster, session, 1, cl)

    # reads by name
    ks = ["\'c%02d\'" % i for i in xrange(0, 100)]
    # We do not support proper IN queries yet
    # if cluster.version() >= "1.2":
    #    session.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    # else:
    #    session.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    # _validate_row(cluster, session)
    # slice reads
    query = SimpleStatement('SELECT * FROM cf WHERE key=\'k0\'', consistency_level=cl)
    rows = list(session.execute(query))
    _validate_row(cluster, rows)


def _put_with_overwrite(cluster, session, nb_keys, cl=ConsistencyLevel.QUORUM):
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i, k, i) for i in xrange(0, 100)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i * 4, k, i * 2) for i in xrange(0, 50)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i * 20, k, i * 5) for i in xrange(0, 20)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()


def _validate_row(cluster, res):
    assert len(res) == 100, len(res)
    for i in xrange(0, 100):
        if i % 5 == 0:
            assert res[i][2] == 'value%d' % (i * 4), 'for %d, expecting value%d, got %s' % (i, i * 4, res[i][2])
        elif i % 2 == 0:
            assert res[i][2] == 'value%d' % (i * 2), 'for %d, expecting value%d, got %s' % (i, i * 2, res[i][2])
        else:
            assert res[i][2] == 'value%d' % i, 'for %d, expecting value%d, got %s' % (i, i, res[i][2])


# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, session, cl=ConsistencyLevel.QUORUM):
    keys = 100

    _put_with_overwrite(cluster, session, keys, cl)

    paged_results = session.execute('SELECT * FROM cf LIMIT 10000000')
    rows = [result for result in paged_results]

    assert len(rows) == keys * 100, len(rows)
    for k in xrange(0, keys):
        res = rows[:100]
        del rows[:100]
        _validate_row(cluster, res)


def replace_in_file(filepath, search_replacements):
    """In-place file search and replace.

    filepath - The path of the file to edit
    search_replacements - a list of tuples (regex, replacement) that
    represent however many search and replace operations you wish to
    perform.

    Note: This does not work with multi-line regexes.
    """
    for line in fileinput.input(filepath, inplace=True):
        for regex, replacement in search_replacements:
            line = re.sub(regex, replacement, line)
        sys.stdout.write(line)


def generate_ssl_stores(base_dir, passphrase='cassandra'):
    """
    Util for generating ssl stores using java keytool -- nondestructive method if stores already exist this method is
    a no-op.

    @param base_dir (str) directory where keystore.jks, truststore.jks and ccm_node.cer will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can be
            overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool fails during any step
    """

    if os.path.exists(os.path.join(base_dir, 'keystore.jks')):
        debug("keystores already exists - skipping generation of ssl keystores")
        return

    debug("generating keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-genkeypair', '-alias', 'ccm_node', '-keyalg', 'RSA', '-validity', '365',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'), '-storepass', passphrase,
                           '-dname', 'cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US', '-keypass', passphrase])
    debug("exporting cert from keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-export', '-rfc', '-alias', 'ccm_node',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'),
                           '-file', os.path.join(base_dir, 'ccm_node.cer'), '-storepass', passphrase])
    debug("importing cert into truststore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-import', '-file', os.path.join(base_dir, 'ccm_node.cer'),
                           '-alias', 'ccm_node', '-keystore', os.path.join(base_dir, 'truststore.jks'),
                           '-storepass', passphrase, '-noprompt'])


class since(object):

    def __init__(self, cass_version, max_version=None):
        self.cass_version = LooseVersion(cass_version)
        self.max_version = max_version
        if self.max_version is not None:
            self.max_version = LooseVersion(self.max_version)

    def _skip_msg(self, version):
        if version < self.cass_version:
            return "%s < %s" % (version, self.cass_version)
        if self.max_version and version > self.max_version:
            return "%s > %s" % (version, self.max_version)

    def _wrap_setUp(self, cls):
        orig_setUp = cls.setUp

        @functools.wraps(cls.setUp)
        def wrapped_setUp(obj, *args, **kwargs):
            orig_setUp(obj, *args, **kwargs)
            version = LooseVersion(obj.cluster.version())
            msg = self._skip_msg(version)
            if msg:
                obj.skip(msg)

        cls.setUp = wrapped_setUp
        return cls

    def _wrap_function(self, f):
        @functools.wraps(f)
        def wrapped(obj):
            version = LooseVersion(obj.cluster.version())
            msg = self._skip_msg(version)
            if msg:
                obj.skip(msg)
            f(obj)
        return wrapped

    def __call__(self, skippable):
        if isinstance(skippable, type):
            return self._wrap_setUp(skippable)
        return self._wrap_function(skippable)


def no_vnodes():
    """Skips the decorated test or test class if using vnodes."""
    return unittest.skipIf(not DISABLE_VNODES, 'Test disabled for vnodes')


def require(require_pattern, broken_in=None):
    """Skips the decorated class or method, unless the argument
    'require_pattern' is a case-insensitive regex match for the name of the git
    branch in the directory from which Cassandra is running. For example, the
    method defined here:

        @require('compaction-fixes')
        def compaction_test(self):
            ...

    will run if Cassandra is running from a directory whose current git branch
    is named 'compaction-fixes'. If 'require_pattern' were
    '.*compaction-fixes.*', it would run only when Cassandra is being run from a
    branch whose name contains 'compaction-fixes'.

    To accommodate current branch-naming conventions, it also will run if the
    current Cassandra branch matches 'CASSANDRA-{require_pattern}'. This allows
    users to run tests like:

        @require(4200)
        class TestNewFeature(self):
            ...

    on branches named 'CASSANDRA-4200'.

    If neither 'require_pattern' nor 'CASSANDRA-{require_pattern}' is a
    case-insensitive match for the name of Cassandra's current git branch, the
    test function or class will be skipped with unittest.skip.

    To run decorated methods as if they were not decorated with @require, set
    the environment variable IGNORE_REQUIRE to 'yes' or 'true'. To only run
    methods decorated with require, set IGNORE_REQUIRE to 'yes' or 'true' and
    run `nosetests` with `-a required`. (This uses the built-in `attrib`
    plugin.)
    """
    tagging_decorator = attr('required')
    if IGNORE_REQUIRE:
        return tagging_decorator
    require_pattern = str(require_pattern)
    git_branch = ''
    git_branch = cassandra_git_branch()

    if git_branch:
        git_branch = git_branch.lower()
        run_on_branch_patterns = (require_pattern, 'cassandra-{b}'.format(b=require_pattern))
        # always run the test if the git branch name matches
        if any(re.match(p, git_branch, re.IGNORECASE) for p in run_on_branch_patterns):
            return tagging_decorator
        # if skipping a buggy/flapping test, use since
        elif broken_in:
            def tag_and_skip_after_version(decorated):
                return since('0', broken_in)(tagging_decorator(decorated))
            return tag_and_skip_after_version
        # otherwise, skip with a message
        else:
            def tag_and_skip(decorated):
                return unittest.skip('require ' + str(require_pattern))(tagging_decorator(decorated))
            return tag_and_skip
    else:
        return tagging_decorator


def known_failure(failure_source, jira_url, flaky=False):
    """
    Tag a test as a known failure. Associate it with the URL for a JIRA
    ticket and tag it as flaky or not.

    Valid values for failure_source include: 'cassandra', 'test', and
    'systemic'.

    To run all known failures, use the functionality provided by the nosetests
    attrib plugin, using the known_failure and known_flaky attributes:

        # only run tests that are known to fail
        $ nosetests -a known_failure
        # only run tests that are not known to fail
        $ nosetests -a !known_failure
        # only run tests that fail because of cassandra bugs
        $ nosetests -a known_failure=cassandra
        # only run tests that fail because of cassandra bugs, but are not flaky
        $ nosetests -a known_failure=cassandra -a !known_flaky

    Known limitations: a given test may only be tagged once and still work as
    expected with the attrib plugin machinery; if you decorate a test with
    known_failure multiple times, the known_failure attribute of that test
    will have the value applied by the outermost instance of the decorator.
    """
    valid_failure_sources = ('cassandra', 'test', 'systemic')
    assert failure_source in valid_failure_sources
    assert isinstance(flaky, bool)

    def wrapper(f):
        tagged_func = attr(known_failure=failure_source,
                           jira_url=jira_url)(f)
        if flaky:
            tagged_func = attr('known_flaky')(tagged_func)
        return tagged_func
    return wrapper


def cassandra_git_branch(cdir=None):
    '''Get the name of the git branch at CASSANDRA_DIR.
    '''
    cdir = CASSANDRA_DIR if cdir is None else cdir
    try:
        p = subprocess.Popen(['git', 'branch'], cwd=cdir,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:  # e.g. if git isn't available, just give up and return None
        debug('shelling out to git failed: {}'.format(e))
        return

    out, err = p.communicate()
    # fail if git failed
    if p.returncode != 0:
        raise RuntimeError('Git printed error: {err}'.format(err=err))
    [current_branch_line] = [line for line in out.splitlines() if line.startswith('*')]
    return current_branch_line[1:].strip()


def safe_mkdtemp():
    tmpdir = tempfile.mkdtemp()
    # \ on Windows is interpreted as an escape character and doesn't do anyone any favors
    return tmpdir.replace('\\', '/')


class InterruptBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("Prepare completed")
        self.node.stop(gently=False)


class InterruptCompaction(Thread):
    """
    Interrupt compaction by killing a node as soon as
    the "Compacting" string is found in the log file
    for the table specified. This requires debug level
    logging in 2.1+ and expects debug information to be
    available in a file called "debug.log" unless a
    different name is passed in as a paramter.
    """
    def __init__(self, node, tablename, filename='debug.log'):
        Thread.__init__(self)
        self.node = node
        self.tablename = tablename
        self.filename = filename
        self.mark = node.mark_log(filename=self.filename)

    def run(self):
        self.node.watch_log_for("Compacting(.*)%s" % (self.tablename,), from_mark=self.mark, filename=self.filename)
        self.node.stop(gently=False)


class KillOnBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("JOINING: Starting to bootstrap")
        self.node.stop(gently=False)
