We are now colocating our test plans directly with the source code. We have decided to do so in a manner compatible with Doxygen, to turn the test plans into easily navigated HTML. Please view the following list of tags, as well as an example test. The test plan will live in a comment block below the test method declaration.


Input             | Description
------------------|------------------
Test name         | Name of the test
Description       | Brief description of the test
@param            | Description of Parameter 1 (Usually used for helper methods)
@return           | Description of expected return value (Usually used for helper methods)
@expected_errors  | What exceptions this test is expected to throw on normal behavior (should be caught and expected in the test)
@throws           | What exceptions this test would throw upon failure (if expecting a specific regression)
@since            | The development version(s) of C* when this test was first introduced. NOT to be confused with the since decorator, which marks the earliest version of C* to run against.
@jira_ticket      | Associated JIRA ticket number
@expected_result  | Brief summary of what the expected results of this test are
@test_assumptions | Test requirements (auth, hints disabled , etc)
@note             | (future improvments, todo, etc)
@test_category    | What categories this test falls under (deprecated)



        def example_test(self):
        """
        Demonstrates the expected syntax for a test plan. Parsed by Doxygen.
        @jira_ticket CASSANDRA-0000
        @since 2.0.15, 2.1.5
        @note Test should not be implemented, it is an example.
        """
            pass

To run doxygen to generate HTML from these test plans, you will need to do the following

* Unzip doxygen/doxypy-0.4.2.tar.gz and install it
        cd doxygen/
        tar xvf doxypy-0.4.2.tar.gz
        cd doxypy-0.4.2
        sudo python setup.py install
* Install doxygen, via your system's package manager
* Edit the INPUT and OUTPUT_DIRECTORY fields in doxygen/Doxyfile_python. They must be absolute paths. INPUT should point to cassandra-dtest/.
* Run doxygen
        doxygen doxygen/Doxyfile_python

Feel free to submit test plans without the implemented tests. If you are submitting a new test, we would appreciate if it were annotated in this manner. If that is not possible, we will add the markup to your pull request.