import functools
import unittest
from distutils.version import LooseVersion

from nose.plugins.attrib import attr
from nose.tools import assert_in, assert_is_instance

from dtest import DISABLE_VNODES


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
            version = obj.cluster.version()
            msg = self._skip_msg(version)
            if msg:
                obj.skip(msg)

        cls.setUp = wrapped_setUp
        return cls

    def _wrap_function(self, f):
        @functools.wraps(f)
        def wrapped(obj):
            version = obj.cluster.version()
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
