#!/usr/bin/python

"""
Builds a regex that matches failing tests. Usage:

nosetests -ve `./failing_tests.py`
"""

failing_tests = """
    unavailable_schema_test
    TestUpgradeTo1_1
    decommission_node_test
    all_all_test
    TestCounters.upgrade_test
    non_local_read_test
    TestRollingUpgrade
    incompressible_data_in_compressed_table_test
    upgrade_through_versions_test
    cql3_insert_thrift_test
    upgrade_test
    putget_deflate_test
"""

lines = (l.strip() for l in failing_tests.splitlines() if l.strip())

print "|".join(lines),

