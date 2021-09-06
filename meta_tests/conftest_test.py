import pytest

from conftest import SkipConditions
from mock import Mock


class DTestConfigMock():
    def __init__(self):
        self.execute_upgrade_tests = False
        self.execute_upgrade_tests_only = False
        self.force_execution_of_resource_intensive_tests = False
        self.only_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.use_vnodes = False
        self.use_off_heap_memtables = False

    def set(self, config):
        if config != "":
            setattr(self, config, True)


def _mock_responses(responses, default_response=None):
    return lambda arg: responses[arg] if arg in responses else default_response


class TestConfTest(object):
    regular_test = Mock(name="regular_test_mock")
    upgrade_test = Mock(name="upgrade_test_mock")
    resource_intensive_test = Mock(name="resource_intensive_test_mock")
    vnodes_test = Mock(name="vnodes_test_mock")
    no_vnodes_test = Mock(name="no_vnodes_test_mock")
    no_offheap_memtables_test = Mock(name="no_offheap_memtables_test_mock")
    depends_driver_test = Mock(name="depends_driver_test_mock")

    def setup_method(self):
        self.regular_test.get_closest_marker.side_effect = _mock_responses({})
        self.upgrade_test.get_closest_marker.side_effect = _mock_responses(
            {"upgrade_test": True})
        self.resource_intensive_test.get_closest_marker.side_effect = _mock_responses(
            {"resource_intensive": True})
        self.vnodes_test.get_closest_marker.side_effect = _mock_responses(
            {"vnodes": True})
        self.no_vnodes_test.get_closest_marker.side_effect = _mock_responses(
            {"no_vnodes": True})
        self.no_offheap_memtables_test.get_closest_marker.side_effect = _mock_responses(
            {"no_offheap_memtables": True})
        self.depends_driver_test.get_closest_marker.side_effect = _mock_responses(
            {"depends_driver": True})

    @pytest.mark.parametrize("item", [upgrade_test, resource_intensive_test, vnodes_test,
                                      depends_driver_test])
    def test_skip_if_no_config(self, item):
        dtest_config = DTestConfigMock()
        assert SkipConditions(dtest_config, False).is_skippable(item)

    @pytest.mark.parametrize("item", [regular_test, resource_intensive_test, no_vnodes_test,
                                      no_offheap_memtables_test])
    def test_include_if_no_config(self, item):
        dtest_config = DTestConfigMock()
        assert not SkipConditions(dtest_config, True).is_skippable(item)

    @pytest.mark.parametrize("item,config",
                             [(upgrade_test, "execute_upgrade_tests_only"),
                              (resource_intensive_test, "only_resource_intensive_tests")])
    def test_include_if_config_only(self, item, config):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert not SkipConditions(dtest_config, True).is_skippable(item)

    @pytest.mark.parametrize("item",
                             [regular_test, upgrade_test, resource_intensive_test, vnodes_test,
                              no_vnodes_test, no_offheap_memtables_test])
    @pytest.mark.parametrize("only_item,config",
                             [(upgrade_test, "execute_upgrade_tests_only"),
                              (resource_intensive_test, "only_resource_intensive_tests")])
    def test_config_only(self, item, only_item, config):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        skip_conditions = SkipConditions(dtest_config, True)
        if item != only_item:
            assert skip_conditions.is_skippable(item)
        else:
            assert not skip_conditions.is_skippable(item)

    @pytest.mark.parametrize("item",
                             [regular_test, upgrade_test, resource_intensive_test,
                              no_vnodes_test, no_offheap_memtables_test])
    def test_include_if_execute_upgrade(self, item):
        dtest_config = DTestConfigMock()
        dtest_config.set("execute_upgrade_tests")
        assert not SkipConditions(dtest_config, True).is_skippable(item)

    @pytest.mark.parametrize("config, sufficient_resources",
                             [("", False),
                              ("skip_resource_intensive_tests", True),
                              ("skip_resource_intensive_tests", False)])
    def test_skip_resource_intensive(self, config, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert SkipConditions(dtest_config, sufficient_resources).is_skippable(self.resource_intensive_test)

    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_include_resource_intensive_if_any_resources(self, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set("force_execution_of_resource_intensive_tests")
        assert not SkipConditions(dtest_config, sufficient_resources).is_skippable(self.resource_intensive_test)

    def test_skip_resource_intensive_wins(self):
        dtest_config = DTestConfigMock()
        dtest_config.set("force_execution_of_resource_intensive_tests")
        dtest_config.set("only_resource_intensive_tests")
        dtest_config.set("skip_resource_intensive_tests")
        assert SkipConditions(dtest_config, True).is_skippable(self.resource_intensive_test)

    @pytest.mark.parametrize("item",
                             [regular_test, resource_intensive_test, vnodes_test,
                              no_offheap_memtables_test])
    def test_if_config_vnodes(self, item):
        dtest_config = DTestConfigMock()
        dtest_config.set("use_vnodes")
        assert not SkipConditions(dtest_config, True).is_skippable(item)

    def test_skip_no_offheap_memtables(self):
        dtest_config = DTestConfigMock()
        dtest_config.set("use_off_heap_memtables")
        assert SkipConditions(dtest_config, True).is_skippable(self.no_offheap_memtables_test)

    @pytest.mark.parametrize("config", ["", "execute_upgrade_tests", "execute_upgrade_tests_only",
                                        "force_execution_of_resource_intensive_tests",
                                        "only_resource_intensive_tests",
                                        "skip_resource_intensive_tests", "use_vnodes",
                                        "use_off_heap_memtables"])
    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_skip_depends_driver_always(self, config, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert SkipConditions(dtest_config, sufficient_resources).is_skippable(self.depends_driver_test)
