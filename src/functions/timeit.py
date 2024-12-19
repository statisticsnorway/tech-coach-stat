import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec
from typing import TypeVar


P = ParamSpec("P")  # Captures the parameters of the callable
R = TypeVar("R")  # Captures the return type of the callable


def timeit(function: Callable[P, R]) -> Callable[P, R]:
    """A decorator that measures the execution time of a function and prints it.

    Args:
        function: The function to be wrapped by the decorator.

    Returns:
        The wrapped function with added time measurement functionality.
    """

    @wraps(function)
    def timeit_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start_time = time.perf_counter()
        result = function(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {function.__name__} took {total_time:.3f} seconds")
        return result

    return timeit_wrapper
