import functools
import re
import unittest
from distutils.version import LooseVersion

from nose.plugins.attrib import attr
from nose.tools import assert_in, assert_is_instance

from dtest import DISABLE_VNODES, IGNORE_REQUIRE
from tools.git import cassandra_git_branch


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
    """
    Skips the decorated test or test class if using vnodes.
    """
    return unittest.skipIf(not DISABLE_VNODES, 'Test disabled for vnodes')


def require(require_pattern, broken_in=None):
    """
    Skips the decorated class or method, unless the argument
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


def known_failure(failure_source, jira_url, flaky=False, notes=''):
    """
    Tag a test as a known failure. Associate it with the URL for a JIRA
    ticket and tag it as flaky or not.

    Valid values for failure_source include: 'cassandra', 'test', 'driver', and
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
    valid_failure_sources = ('cassandra', 'test', 'systemic', 'driver')

    def wrapper(f):
        assert_in(failure_source, valid_failure_sources)
        assert_is_instance(flaky, bool)

        tagged_func = attr(known_failure=failure_source,
                           jira_url=jira_url)(f)
        if flaky:
            tagged_func = attr('known_flaky')(tagged_func)

        tagged_func = attr(failure_notes=notes)(tagged_func)
        return tagged_func
    return wrapper
