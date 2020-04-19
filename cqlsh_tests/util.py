from collections import namedtuple

import pytest
from ccmlib.node import ToolError


def run_cqlsh_safe(node, cmds, cqlsh_options=None, expect_error=True):
    """
    cqlsh behavior has changed to set an error code on exit. This wrapper
    makes it easier to run cqlsh commands while expecting exceptions.
    """
    try:
        ret = node.run_cqlsh(cmds=cmds, cqlsh_options=cqlsh_options)
        if expect_error:
            pytest.fail("Expected ToolError but didn't get one")
        return ret
    except ToolError as e:
        ret = namedtuple('Subprocess_Return', 'stdout stderr rc')
        return ret(stdout=e.stdout, stderr=e.stderr, rc=e.exit_status)
