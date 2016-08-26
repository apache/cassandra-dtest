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
