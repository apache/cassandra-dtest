from collections import namedtuple

from nose import plugins

# A class that defines the attributes that have to be defined for configuring
# a dtest run. namedtuple does what we want -- it's immutable and requires
# all the attributes to be passed in to be instantiated.
GlobalConfigObject = namedtuple('GlobalConfigObject', [
    'vnodes',  # disable or enable vnodes
])

_CONFIG = None


class DtestConfigPlugin(plugins.Plugin):
    """
    Pass in configuration options for the dtests.
    """
    enabled = True  # if this plugin is loaded at all, we're using it
    name = 'dtest_config'

    def __init__(self, config=None):
        """
        Instantiate this plugin with a GlobalConfigObject or, by default, None.
        Then, set  the global _CONFIG constant with the value of the plugin.

        This is a little weird, yes, but nose seems to generally be built
        around the idea that a given plugin will be instantiated only once, so
        this provides a way for test framework code to grab the value off this
        module. We want that, since the plugin itself isn't available to test
        code.

        @param config an object meeting the GlobalConfigObject spec that will
                      be used as configuration for a dtest run.
        """
        self.CONFIG = config

        global _CONFIG
        _CONFIG = self.CONFIG

    def configure(self, options, conf):
        pass
