#!/usr/bin/python

"""
Builds a regex that matches failing tests. Usage:

nosetests -ve `failing_tests.py`
"""

failing_tests = """
    decommission_node_schema_check_test
    upgrade_test
    simple_bootstrap_test
    decommission_node_test
    all_all_test
    all_one_test
    one_all_test
    one_one_test
    hintedhandoff_test
    quorum_available_during_failure_test
    quorum_quorum_test
    readrepair_test
    short_read_reversed_test
    short_read_test

"""

lines = (l.strip() for l in failing_tests.splitlines() if l.strip())

print "|".join(lines),

