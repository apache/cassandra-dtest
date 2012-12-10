#!/usr/bin/python

"""
Builds a regex that matches failing tests. Usage:

nosetests -ve `./failing_tests.py`
"""

failing_tests = """
    unavailable_schema_test
    upgrade_to_11_test
    decommission_node_test
    upgrade_test
    TestGlobalRowKeyCache
    rolling_upgrade_test
    incompressible_data_in_compressed_table_test
    short_read_test
"""

lines = (l.strip() for l in failing_tests.splitlines() if l.strip())

print "|".join(lines),

