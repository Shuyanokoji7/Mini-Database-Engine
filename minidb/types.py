"""Tiny type system: INTEGER, TEXT, REAL.

Real SQL engines have dozens of types. We keep only three so the type
logic stays obvious.
"""

from .exceptions import DataError

INTEGER = "INTEGER"
TEXT = "TEXT"
REAL = "REAL"

VALID_TYPES = {INTEGER, TEXT, REAL}

# Maps our type strings to Python types. PEP 249 calls this a "type code"
# and exposes it in cursor.description.
PYTHON_TYPE = {INTEGER: int, TEXT: str, REAL: float}


def coerce(value, type_name: str):
    """Convert *value* to the Python representation of *type_name*.

    ``None`` (SQL NULL) passes through unchanged. We reject ``bool`` for
    numeric columns because Python says ``True == 1`` and that almost
    always hides a bug when it happens inside a database.
    """
    if value is None:
        return None

    if type_name not in VALID_TYPES:
        raise DataError(f"unknown type {type_name!r}")

    try:
        if type_name == INTEGER:
            if isinstance(value, bool):
                raise DataError("expected INTEGER, got bool")
            return int(value)
        if type_name == REAL:
            if isinstance(value, bool):
                raise DataError("expected REAL, got bool")
            return float(value)
        # TEXT
        if not isinstance(value, str):
            raise DataError(f"expected TEXT, got {type(value).__name__}")
        return value
    except (ValueError, TypeError) as e:
        raise DataError(f"cannot convert {value!r} to {type_name}: {e}") from e
