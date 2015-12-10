from __future__ import division

from functools import partial
import json
import os
import sys

import nose


class PrintJiraURLPlugin(nose.plugins.Plugin):
    enabled = True

    def options(self, parser, env):
        super(PrintJiraURLPlugin, self).configure(parser, env)

    def testName(self, test):
        _, test_module, test_name = test.address()
        test_method_name = test_name.split('.')[-1]
        test_method = getattr(test.test, test_method_name)

        get_attr_for_current_method = partial(
            nose.plugins.attrib.get_method_attr,
            method=test_method,
            cls=test.test,
        )

        jira_url = get_attr_for_current_method(attr_name='jira_url')
        flaky = get_attr_for_current_method(attr_name='known_flaky')
        failure_source = get_attr_for_current_method(attr_name='known_failure')

        return json.dumps({
            'module': test_module,
            'name': test_name,
            'jira_url': jira_url,
            'known_flaky': flaky,
            'failure_source': failure_source
        })


if __name__ == '__main__':
    argv = sys.argv + ['--collect-only', '-v']
    env = {}
    env.update(os.environ)
    env['CASSANDRA_VERSION'] = 'git:trunk'  # the tests need a version to run
    os.environ
    nose.main(addplugins=[PrintJiraURLPlugin()],
              argv=argv, env=env)
