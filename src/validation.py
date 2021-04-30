from typing import Any


def _is_positive_integer(**kwargs: Any) -> None:
    """Raise error if a value from `kwargs` is not a positive integer."""
    for key, val in kwargs.items():
        if val <= 0 or not isinstance(val, int):
            raise ValueError(f"`{key}` must be a positive integer.\nPassed in: {val}")


def _is_positive_less_than_one(**kwargs: Any) -> None:
    """Raise error if a value from `kwargs` is not a positive integer."""
    for key, val in kwargs.items():
        if not 0 < val < 1:
            raise ValueError(
                f"`{key}` must be positive but less than 1. \nPassed in {val}"
            )
