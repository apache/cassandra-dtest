from cassandra.cluster import ResultSet
from typing import List

def assert_resultset_contains(got: ResultSet, expected: List[tuple]) -> None:
    """
    So this is slow. I would hope a ResultSet has the capability of pulling data by PK or clustering,
    however I'm not finding it atm. As such, this method isn't intended for use with large datasets.
    :param got: ResultSet, expect schema of [a, b]
    :param expected: list of tuples with 2 members corresponding with a/b schema of ResultSet
    """
    # Adding a touch of sanity check so people don't mis-use this. n^2 is bad.
    assert len(expected) <= 1000, 'This is a slow comparison method. Don\'t use for > 1000 tuples.'

    # First quick check: if we have a different count, we can just die.
    assert len(got.current_rows) == len(expected)

    for t in expected:
        assert len(t) == 2, 'Got unexpected tuple len. Expected 2, got tuple: {}'.format(t)
        found = False
        for row in got.current_rows:
            if found:
                break
            if row.a == t[0] and row.b == t[1]:
                found = True
        assert found, 'Failed to find expected row: {}'.format(t)
