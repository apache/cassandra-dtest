import logging
import time

logger = logging.getLogger(__name__)

class RerunTestException(Exception):
    """
    This exception can be raised to signal a likely harmless test problem. If fixing a test is reasonable, that should be preferred.

    Ideally this is used in conjunction with the 'flaky' decorator, allowing the test to be automatically re-run and passed.
    When raising this exception in methods decorated with @flaky(rerun_filter=requires_rerun), do so carefully.
    Avoid overly broad try/except blocks, otherwise real (intermittent) bugs could be masked.

    example usage:

    @flaky(rerun_filter=requires_rerun)  # see requires_rerun method below in this module
    def some_flaky_test():
        # some predictable code
        # more predictable code

        try:
            # some code that occasionally fails for routine/predictable reasons (e.g. timeout)
        except SomeNarrowException:
            raise RerunTestException

    When the test raises RerunTestException, the flaky plugin will re-run the test and it will pass if the next attempt(s) succeed.
    """


def requires_rerun(err, *args):
    """
    For use in conjunction with the flaky decorator and it's rerun_filter argument. See RerunTestException above.

    Returns True if the given flaky failure data (err) is of type RerunTestException, otherwise False.
    """
    # err[0] contains the type of the error that occurred
    return err[0] == RerunTestException

def retry(fn, max_attempts=10, allowed_error=None, sleep_seconds=1):
    if max_attempts <= 0:
        raise ValueError("max_attempts must be a positive value, but given {}".format(str(max_attempts)))
    last_error = None
    for _ in range(0, max_attempts): 
        try:
            return fn()
        except Exception as e:
            last_error = e
            if allowed_error and not allowed_error(e):
                break
            logger.info("Retrying as error '{}' was seen; sleeping for {} seconds".format(str(e), str(sleep_seconds)))
            time.sleep(sleep_seconds)
    raise last_error


