
from unittest import TestCase

from cassandra import AlreadyExists, InvalidRequest, Unauthorized, Unavailable
from mock import Mock

from tools.assertions import (assert_all, assert_almost_equal, assert_exception,
                              assert_invalid, assert_length_equal, assert_none,
                              assert_one, assert_row_count, assert_stderr_clean,
                              assert_unauthorized, assert_unavailable)
import pytest

class TestAssertStderrClean(TestCase):

    def test_empty_string(self):
        err = ''
        assert_stderr_clean(err)

    def test_multiple_valid_errors(self):
        err = """objc[65696]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib. One of the two will be used. Which one is undefined.
                 WARN  15:28:24,788 JNA link failure, one or more native method will be unavailable."""
        assert_stderr_clean(err)

    def test_valid_error(self):
        err = ("objc[36358]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk"
               "/Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib."
               "One of the two will be used. Which one is undefined.")
        assert_stderr_clean(err)

    def test_valid_error_with_whitespace(self):
        err = """WARN  14:08:15,018 JNA link failure, one or more native method will be unavailable.objc[36358]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib. One of the two will be used. Which one is undefined.
                 """
        assert_stderr_clean(err)

    def test_invalid_error(self):
        err = "This string is no good and should fail."
        with pytest.raises(AssertionError):
            assert_stderr_clean(err)

    def test_valid_and_invalid_errors_same_line(self):
        err = ("This string is no good and should fail.objc[36358]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk"
               "/Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib."
               "One of the two will be used. Which one is undefined.")
        with pytest.raises(AssertionError):
            assert_stderr_clean(err)

    def test_invalid_error_after_valid_error(self):
        err = """objc[36358]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk
               /Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib.
               One of the two will be used. Which one is undefined.
               This string is no good and should fail."""
        with pytest.raises(AssertionError):
            assert_stderr_clean(err)

    def test_invalid_error_before_valid_errors(self):
        err = """This string is not good and should fail. WARN  14:08:15,018 JNA link failure, one or more native method will be unavailable.objc[36358]:
                 Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/bin/java
                 and /Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home/jre/lib/libinstrument.dylib. One of the two will be used. Which one is undefined.
                 """
        with pytest.raises(AssertionError):
            assert_stderr_clean(err)


class TestAssertionMethods(TestCase):

    def test_assertions(self):
        # assert_exception_test
        mock_session = Mock(**{'execute.side_effect': AlreadyExists("Dummy exception message.")})
        assert_exception(mock_session, "DUMMY QUERY", expected=AlreadyExists)

        # assert_unavailable_test
        mock_session = Mock(**{'execute.side_effect': Unavailable("Dummy Unavailabile message.")})
        assert_unavailable(mock_session.execute)

        # assert_invalid_test
        mock_session = Mock(**{'execute.side_effect': InvalidRequest("Dummy InvalidRequest message.")})
        assert_invalid(mock_session, "DUMMY QUERY")

        # assert_unauthorized_test
        mock_session = Mock(**{'execute.side_effect': Unauthorized("Dummy Unauthorized message.")})
        assert_unauthorized(mock_session, "DUMMY QUERY", None)

        # assert_one_test
        mock_session = Mock()
        mock_session.execute = Mock(return_value=[[1, 1]])
        assert_one(mock_session, "SELECT * FROM test", [1, 1])

        # assert_none_test
        mock_session = Mock()
        mock_session.execute = Mock(return_value=[])
        assert_none(mock_session, "SELECT * FROM test")

        # assert_all_test
        mock_session = Mock()
        mock_session.execute = Mock(return_value=[[i, i] for i in range(0, 10)])
        assert_all(mock_session, "SELECT k, v FROM test", [[i, i] for i in range(0, 10)], ignore_order=True)

        # assert_almost_equal_test
        assert_almost_equal(1, 1.1, 1.2, 1.9, error=1.0)

        # assert_row_count_test
        mock_session = Mock()
        mock_session.execute = Mock(return_value=[[1]])
        assert_row_count(mock_session, 'test', 1)

        # assert_length_equal_test
        check = [1, 2, 3, 4]
        assert_length_equal(check, 4)

    def test_almost_equal_expect_pass(self):
        assert_almost_equal(1, 1.1, 1.3, error=.31)

    def test_almost_equal_expect_failure(self):
        with pytest.raises(AssertionError):
            assert_almost_equal(1, 1.3, error=.1)
