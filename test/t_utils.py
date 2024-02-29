from os import environ
from typing import Callable


def inhibit_CI(test: Callable) -> Callable:
    """Checks for $CI environment variable set by Github Actions
    if $CI == 'true', substitute test for a dummy test
    if $CI undefined or == 'false', test runs normally

    Args:
        test (Callable): function to wrap or @decorate
    """
    def dummy_test(*args, **kwargs):
        assert True

    if environ.get('CI', 'false') == 'true':
        return dummy_test
    else:
        return test
