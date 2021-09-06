## Style

We plan to move to Python 3 in the near future. Where possible, new code should be Python 3-compatible. In particular:

- favor `format` over `%` for formatting strings.
- use the `/` on numbers in a Python 3-compatible way. In particular, if you want floor division (which is the behavior of `/` in Python 2), use `//` instead. If you want the result of integer division to be a `float` (e.g. `1 / 2 == 0.5`), add `from __future__ import division` to the top of the imports and use `/`. For more information, see [the official Python 3 porting docs](https://docs.python.org/3/howto/pyporting.html#division).
- use `absolute_import`, `division`, and `unicode_literals` in new test files.

Contributions will be evaluated by PEP8. We now strictly enforce compliance, via a linter run with Travis CI against all new pull requests. We do not enforce the default limits on line length, but have established a maximum length of 200 chars as a sanity check. You can conform to PEP8 by running `autopep8` which can be installed via `pip`.
`pip install autopep8 && autopep8 --in-place -a --ignore E501`

Another way to make sure that your code will pass compliance checks is to run **flake8** from a commit hook:

```
flake8 --install-hook
git config flake8.strict true
git config flake8.ignore E501,F811,F812,F821,F822,F823,F831,F841,N8,C9,W503
```

We do not enforce **import** sorting, but if you choose to organize imports by some convention, use the `isort` tool (`pip install isort`).

Please use `session`, and not `cursor` when naming your connection variables, to match the style preferred by the DataStax Python Driver, which is how these tests
connect to C*.

All version objects being passed from CCM are now LooseVersion objects, instead of strings. Those can still be safely compared to strings, so there is no need to do `version < LooseVersion('3.10')`.

## Documentation

Please view the following list of tags, as well as an example test. While this full list of tags is available for use, there is no need to use every tag for a given test. The **description** and **jira_ticket** fields should be included, but most others should only be used when **appropriate**. The test plan will live in a comment block below the test method declaration.


Input             | Description
------------------|------------------
Test name         | Name of the test
Description       | Brief description of the test
@param            | Description of Parameter 1 (Usually used for helper methods)
@return           | Description of expected return value (Usually used for helper methods)
@expected_errors  | What exceptions this test is expected to throw on normal behavior (should be caught and expected in the test)
@throws           | What exceptions this test would throw upon failure (if expecting a specific regression)
@jira_ticket      | Associated JIRA ticket identifier, including the project name (e.g. `CASSANDRA-42`, not `42`).
@expected_result  | Brief summary of what the expected results of this test are
@test_assumptions | Test requirements (auth, hints disabled , etc)
@note             | (future improvments, todo, etc)
@test_category    | What categories this test falls under (deprecated)


```python
def test_example(self):
"""
Demonstrates the expected syntax for a test plan.
@jira_ticket CASSANDRA-0000
@note Test should not be implemented, it is an example.
"""
    pass
```

## Modules

In some cases, we organize our test files by putting them in directories. If you do so, please export a module from that directory by placing an `__init__.py` file in the directory with the test files. This makes the modules visible to our test infrastructure scripts that divide tests into buckets for CI.

## Assertions

- When possible, you should use the assert functions from [`tools/assertions.py`](https://github.com/apache/cassandra-dtest/blob/trunk/tools/assertions.py).
- When none of these are applicable, use python's built in [unittest assertions](https://docs.python.org/2/library/unittest.html#assert-methods).

## Byteman Files

Any and all byteman (.btm) files should be saved in the cassandra-dtest/byteman/ directory.

## Summary: Review Checklist

- Correctness
    - Does the test pass? If not, is the failure expected?
    - If the test shells out to other processes,
        - does it validate the output to ensure there were no failures?
        - is it Windows-compatibile?
    - Does the test exercise a new feature? If so, is it tagged with `@since` to skip older versions?
- Style and Python 3 Compatibility:
    - Does the code use `.format()` over `%` for format strings?
    - If there are new test files, do they use the requested imports for Python 3 compatibility?
    - Have the changes caused any style regressions? In particular, did Travis find any?
    - Are `cassandra.cluster.Session` objects named `session` (and not `cursor`)?
- Documentation and Metadata:
    - Are new tests and test classes documented with docstrings?
    - Are changed tests' documentation updated?
    - Is Cassandra's desired behavior described in the documentation if it's not immediately readable in the test?
    - Does the documentation include all appropriate Doxygen annotations, in particular `@jira_ticket`?
- Readability and Reusability
    - Are any data structures built by looping that could be succinctly created in a comprehension
    - Is there repeated logic that could be factored out and given a descriptive name?
        - Does that repeated logic belong somewhere other than this particular test? Possible appropriate locations include the modules in `tools/`, or `ccm`.
    - If there is no assertion in the test, should there be? If not, is the statement that would fail under a regression commented to indicate that?
    - Is it possible for an uninitiated reader to understand what Casssandra behavior is being tested for?
        - If not, could the code be rewritten so it is?
        - If not, are there comments and documentation describing the desired behavior and how it's tested?
