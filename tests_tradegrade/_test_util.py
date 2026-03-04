"""
STRICT TEST UTIL
"""

from typing import Any, Callable, Type


def expect_raises(exc_type: Type[BaseException], fn: Callable[[], Any]) -> BaseException:
    try:
        fn()
    except exc_type as e:
        return e
    raise AssertionError(f"expected exception {exc_type.__name__} not raised")


def expect_raises_any(fn: Callable[[], Any]) -> BaseException:
    try:
        fn()
    except BaseException as e:
        return e
    raise AssertionError("expected exception not raised")