from cassandra import AlreadyExists, Unavailable, InvalidRequest, Unauthorized
from mock import Mock
from assertions import (assert_one, assert_none, assert_exception, assert_unavailable,
                        assert_invalid, assert_unauthorized, assert_all, assert_almost_equal,
                        assert_length_equal, assert_row_count)


class TestAssertionMethods():

    def assertions_test(self):
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
