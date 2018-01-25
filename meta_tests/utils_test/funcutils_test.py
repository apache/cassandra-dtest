from time import sleep, time
from unittest import TestCase

from mock import Mock, call
from tools import funcutils


class Testget_rate_limited_function(TestCase):

    def setUp(self):
        self.mock_func, self.mock_limit = Mock(name='func'), Mock(name='limit')
        self.rate_limited_func = funcutils.get_rate_limited_function(self.mock_func, self.mock_limit)

    def _assert_initialized_correctly_with_mocks(self, rate_limited_func_arg):
        """
        Assert the rate_limited_func argument was correctly initialized with
        the mocks created in setUp as its func and limit.
        """
        self.assertIs(rate_limited_func_arg.func, self.mock_func)
        self.assertIs(rate_limited_func_arg.limit, self.mock_limit)
        assert rate_limited_func_arg.last_called == False

    def test_init_with_positional_args(self):
        """
        Rate-limited functions can be initialized with the function and limit in that order.
        """
        self._assert_initialized_correctly_with_mocks(
            funcutils.get_rate_limited_function(self.mock_func, self.mock_limit)
        )

    def test_init_with_keyword_args(self):
        """
        Rate-limited functions can be initialized with limit and func keyword arguments.
        """
        self._assert_initialized_correctly_with_mocks(
            funcutils.get_rate_limited_function(limit=self.mock_limit, func=self.mock_func)
        )

    def test_repr(self):
        """
        A rate-limited function's repr is what we expect.
        """
        self.rate_limited_func.last_called = mock_last_called = Mock()
        self.assertEqual(
            repr(self.rate_limited_func),
            ('get_rate_limited_function('
             'func=' + repr(self.mock_func) + ', '
             'limit=' + repr(self.mock_limit) + ', '
             'last_called=' + repr(mock_last_called)) + ')'
        )

    def test_calling_rate_limited_func_delegates_to_wrapped_func(self):
        """
        Calling a rate-limited function delegates to the wrapped function.
        """
        self.rate_limited_func.limit = 1
        arg, kwarg = Mock(name='arg'), Mock(name='kwarg')
        self.rate_limited_func(arg, kwarg=kwarg)
        self.mock_func.assert_called_once_with(arg, kwarg=kwarg)

    def test_calling_func_attribute_calls_wrapped_function(self):
        """
        Calling a rate-limited function's func attribute calls the wrapped function.
        """
        arg, kwarg = Mock(name='arg'), Mock(name='kwarg')
        self.rate_limited_func.func(arg, kwarg=kwarg)
        self.mock_func.assert_called_once_with(arg, kwarg=kwarg)

    def test_calling_func_attribute_not_rate_limited(self):
        """
        Calling a rate-limited function's func attribute is not affected by rate limiting.
        """
        self.rate_limited_func.limit = 5
        arg0, kwarg0, arg1, kwarg1 = (Mock(name='arg0'), Mock(name='kwarg0'),
                                      Mock(name='arg1'), Mock(name='kwarg1'))
        self.rate_limited_func.func(arg0, kwarg=kwarg0)
        self.rate_limited_func.func(arg1, kwarg=kwarg1)

        self.mock_func.assert_has_calls([call(arg0, kwarg=kwarg0),
                                         call(arg1, kwarg=kwarg1)])

    def test_rate_limit_respected(self):
        """
        If you call a rate-limited function before the time limit is up, it is called not called.

        This tests behavior with respect to last_called, rather than actually sleeping.
        """
        self.rate_limited_func.limit = 1
        self.rate_limited_func.last_called = time() - .5
        self.rate_limited_func()
        self.mock_func.assert_not_called()

    def test_can_call_again_if_last_called_older_than_limit(self):
        """
        If you call a rate-limited function a second time after the time limit is up, it is called twice.

        This tests behavior with respect to last_called, rather than actually sleeping.
        """
        self.rate_limited_func.limit = 100
        self.rate_limited_func.last_called = time() - 100
        self.rate_limited_func()
        self.mock_func.assert_has_calls([call()])

    def test_last_called_set_when_called(self):
        """
        If you call a rate-limited function, last_called is set to a new value.
        """
        self.rate_limited_func.limit = 1
        assert self.rate_limited_func.last_called == False
        self.rate_limited_func()
        assert abs(round(self.rate_limited_func.last_called, 2) - round(time(), 2)) <= 0.0

    def test_last_called_not_set_when_called_within_time_limit(self):
        """
        If you call a rate-limited function during the time limit, last_called is not set to a new value.
        """
        self.rate_limited_func.limit = 1
        assert self.rate_limited_func.last_called == False
        self.rate_limited_func()
        last_called = self.rate_limited_func.last_called
        self.rate_limited_func()
        self.assertIs(last_called, self.rate_limited_func.last_called)

    def test_end_to_end(self):
        """
        A rate-limited function works as expected.

        A rate-limited function delegates to the wrapped function, then
        prevents calls until the time limit has passed, then allows calls
        again.
        """
        self.rate_limited_func.limit = .5
        self.rate_limited_func()
        self.mock_func.assert_called_once()

        # If called before limit has elapsed, the wrapped function won't be
        # called again.
        self.rate_limited_func()
        self.mock_func.assert_called_once()

        # After limit has elapsed, the wrapped function can be called again.
        sleep(.5)
        self.rate_limited_func()
        self.mock_func.assert_has_calls([call(), call()])
