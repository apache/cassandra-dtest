"""
A script that runs the tests with --collect-only, but instead of just printing
the tests' names, prints the information added by the tools.known_failure
decorator.

This is basically a wrapper around the `nosetests` command, so it takes the
same arguments, though it appends some arguments to sys.argv. In particular,
if you want to look at particular kinds of known failures, use the `-a`
parameter on this script as you would for any of the known_failures attributes.
In addition, you should call it from the same directory from which you'd call
`nosetests`.
"""

import json
import os
import sys
from functools import partial

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
        notes = get_attr_for_current_method(attr_name='failure_notes')

        return json.dumps({
            'module': test_module,
            'name': test_name,
            'jira_url': jira_url,
            'known_flaky': flaky,
            'failure_source': failure_source,
            'notes': notes
        })


if __name__ == '__main__':
    argv = sys.argv + ['--collect-only', '-v']
    env = {}
    env.update(os.environ)
    env['CASSANDRA_VERSION'] = 'git:trunk'  # the tests need a version to run
    os.environ
    nose.main(addplugins=[PrintJiraURLPlugin()],
              argv=argv, env=env)
