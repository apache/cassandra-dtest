from unittest import TestCase

from conftest import is_skippable
from mock import Mock


def _mock_responses(responses, default_response=None):
    return lambda arg: responses[arg] if arg in responses else default_response


def _is_skippable(item,
                  include_upgrade_tests=True,
                  include_non_upgrade_tests=True,
                  include_resource_intensive_tests=True,
                  include_non_resource_intensive_tests=True,
                  include_vnodes_tests=True,
                  include_no_vnodes_tests=True,
                  include_no_offheap_memtables_tests=True):
    return is_skippable(item,
                        include_upgrade_tests,
                        include_non_upgrade_tests,
                        include_resource_intensive_tests,
                        include_non_resource_intensive_tests,
                        include_vnodes_tests,
                        include_no_vnodes_tests,
                        include_no_offheap_memtables_tests)


class ConfTestTest(TestCase):
    regular_test = Mock(name="regular_test_mock")
    upgrade_test = Mock(name="upgrade_test_mock")
    resource_intensive_test = Mock(name="resource_intensive_test_mock")
    vnodes_test = Mock(name="vnodes_test_mock")
    no_vnodes_test = Mock(name="no_vnodes_test_mock")
    no_offheap_memtables_test = Mock(name="no_offheap_memtables_test_mock")
    depends_driver_test = Mock(name="depends_driver_test_mock")

    def setup_method(self, method):
        self.regular_test.get_closest_marker.side_effect = _mock_responses({})
        self.upgrade_test.get_closest_marker.side_effect = _mock_responses({"upgrade_test": True})
        self.resource_intensive_test.get_closest_marker.side_effect = _mock_responses({"resource_intensive": True})
        self.vnodes_test.get_closest_marker.side_effect = _mock_responses({"vnodes": True})
        self.no_vnodes_test.get_closest_marker.side_effect = _mock_responses({"no_vnodes": True})
        self.no_offheap_memtables_test.get_closest_marker.side_effect = _mock_responses({"no_offheap_memtables": True})
        self.depends_driver_test.get_closest_marker.side_effect = _mock_responses({"depends_driver": True})

    def test_regular_test(self):
        assert not _is_skippable(item=self.regular_test)
        assert _is_skippable(item=self.regular_test, include_non_upgrade_tests=False)
        assert _is_skippable(item=self.regular_test, include_non_resource_intensive_tests=False)

    def test_upgrade_test(self):
        assert not _is_skippable(item=self.upgrade_test)
        assert _is_skippable(item=self.upgrade_test, include_upgrade_tests=False)

    def test_resource_intensive_test(self):
        assert not _is_skippable(item=self.resource_intensive_test)
        assert _is_skippable(item=self.resource_intensive_test, include_resource_intensive_tests=False)

    def test_vnodes_test(self):
        assert not _is_skippable(item=self.vnodes_test)
        assert _is_skippable(item=self.vnodes_test, include_vnodes_tests=False)

    def test_no_vnodes_test(self):
        assert not _is_skippable(item=self.no_vnodes_test)
        assert _is_skippable(item=self.no_vnodes_test, include_no_vnodes_tests=False)

    def test_no_offheap_memtables_test(self):
        assert not _is_skippable(item=self.no_offheap_memtables_test)
        assert _is_skippable(item=self.no_offheap_memtables_test, include_no_offheap_memtables_tests=False)

    def test_depends_driver_test(self):
        assert _is_skippable(item=self.depends_driver_test)
