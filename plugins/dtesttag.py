from nose import plugins
import os
import inspect


class DTestTag(plugins.Plugin):
    enabled = True  # if this plugin is loaded at all, we're using it
    name = 'dtest_tag'

    def __init__(self):
        pass

    def configure(self, options, conf):
        pass

    def nice_classname(self, obj):
        """Returns a nice name for class object or class instance.

            >>> nice_classname(Exception()) # doctest: +ELLIPSIS
            '...Exception'
            >>> nice_classname(Exception) # doctest: +ELLIPSIS
            '...Exception'

        """
        if inspect.isclass(obj):
            cls_name = obj.__name__
        else:
            cls_name = obj.__class__.__name__
        mod = inspect.getmodule(obj)
        if mod:
            name = mod.__name__
            # jython
            if name.startswith('org.python.core.'):
                name = name[len('org.python.core.'):]
            return "%s.%s" % (name, cls_name)
        else:
            return cls_name

    def describeTest(self, test):
        tag = os.getenv('TEST_TAG', '')
        if tag == '':
            tag = test.test._testMethodName
        else:
            tag = test.test._testMethodName + "-" + tag
        return "%s (%s)" % (tag, self.nice_classname(test.test))
