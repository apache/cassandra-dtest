import os
from re import search
from unittest import TestCase

from dtest_config import DTestConfig
from mock import Mock, patch
from pytest import UsageError, raises
import ccmlib.repository
import ccmlib.common


def _mock_responses(responses, default_response=None):
    return lambda input: responses[input] if input in responses else \
        "%s/meta_tests/cassandra-dir-4.0-beta" % os.getcwd() if input == "--cassandra-dir" else default_response


def _check_with_params(params):
    config = Mock()
    config.getoption.side_effect = _mock_responses(params)
    config.getini.side_effect = _mock_responses({})

    dTestConfig = DTestConfig()
    dTestConfig.setup(config)
    return dTestConfig


def _check_with_params_expect(params, pattern):
    with raises(UsageError, match=pattern):
        _check_with_params(params)


class DTestConfigTest(TestCase):

    def test_invalid_cass_dir_no_version(self):
        _check_with_params_expect({
            '--cassandra-dir': 'blah'
        }, "The Cassandra directory blah does not seem to be valid")

    def test_cass_dir_and_version(self):
        _check_with_params_expect({
            '--cassandra-version': '3.11'
        }, "Cassandra build directory is already defined")

    def test_no_cass_dir(self):
        with patch.object(ccmlib.repository, "setup") as mocked_setup:
            mocked_setup.side_effect = _mock_responses({'3.2': ("%s/meta_tests/cassandra-dir-3.2" % os.getcwd(), '3.2.0')})
            c = _check_with_params({
                '--cassandra-dir': None,
                '--cassandra-version': '3.2'
            })
            assert c.cassandra_version == '3.2'
            assert search("^3.2", str(c.cassandra_version_from_build))

    def test_valid_cass_dir_no_version(self):
        c = _check_with_params({
        })
        assert c.cassandra_version is None
        assert c.cassandra_version_from_build == '4.0-beta'

    def test_no_cass_dir_no_version(self):
        _check_with_params_expect({
            '--cassandra-dir': None
        }, "You must provide either --cassandra-dir or --cassandra-version")

    def test_illegal_args_combinations_for_resource_intensive_tests(self):
        _check_with_params_expect({
            '--only-resource-intensive-tests': True,
            '--skip-resource-intensive-tests': True
        }, 'does not make any sense')

        _check_with_params_expect({
            '--force-resource-intensive-tests': True,
            '--skip-resource-intensive-tests': True
        }, 'does not make any sense')

        _check_with_params_expect({
            '--only-resource-intensive-tests': True,
            '--force-resource-intensive-tests': True,
            '--skip-resource-intensive-tests': True
        }, 'does not make any sense')

    def test_legal_args_combinations_for_resource_intensive_tests(self):
        c = _check_with_params({
            '--only-resource-intensive-tests': True
        })
        assert c.only_resource_intensive_tests
        assert not c.skip_resource_intensive_tests
        assert not c.force_execution_of_resource_intensive_tests

        c = _check_with_params({
            '--only-resource-intensive-tests': True,
            '--force-resource-intensive-tests': True
        })
        assert c.only_resource_intensive_tests
        assert not c.skip_resource_intensive_tests
        assert c.force_execution_of_resource_intensive_tests

        c = _check_with_params({
            '--skip-resource-intensive-tests': True
        })
        assert not c.only_resource_intensive_tests
        assert c.skip_resource_intensive_tests
        assert not c.force_execution_of_resource_intensive_tests

        c = _check_with_params({
        })
        assert not c.only_resource_intensive_tests
        assert not c.skip_resource_intensive_tests
        assert not c.force_execution_of_resource_intensive_tests

    def off_heap_memtables_not_supported(self):
        _check_with_params_expect({
            '--cassandra-dir': "%s/meta_tests/cassandra-dir-3.2" % os.getcwd(),
            '--use-off-heap-memtables': True
        }, "The selected Cassandra version 3.2 doesn't support the provided option")

    def off_heap_memtables_supported(self):
        c = _check_with_params({
            '--use-off-heap-memtables': True
        })
        assert c.use_off_heap_memtables
