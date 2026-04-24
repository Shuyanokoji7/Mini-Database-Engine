"""PEP 249 exception hierarchy.

PEP 249 mandates this exact class tree so application code can catch
database errors in a driver-agnostic way. The hierarchy is::

    StandardError (Exception)
    |-- Warning
    |-- Error
        |-- InterfaceError
        |-- DatabaseError
            |-- DataError
            |-- OperationalError
            |-- IntegrityError
            |-- InternalError
            |-- ProgrammingError
            |-- NotSupportedError

Application code that wants to catch "any DB failure" catches ``Error``.
"""


class Warning(Exception):  # noqa: A001 - name mandated by PEP 249
    """Non-fatal warning."""


class Error(Exception):
    """Base class for all database errors."""


class InterfaceError(Error):
    """A problem with the database interface rather than the database."""


class DatabaseError(Error):
    """A problem with the database itself."""


class DataError(DatabaseError):
    """Bad data: type mismatch, out-of-range value, division by zero, ..."""


class OperationalError(DatabaseError):
    """Operational problem: missing file, I/O error, disconnection, ..."""


class IntegrityError(DatabaseError):
    """Integrity constraint violated (e.g. duplicate primary key)."""


class InternalError(DatabaseError):
    """Engine is in an inconsistent state. Should not normally happen."""


class ProgrammingError(DatabaseError):
    """Bad SQL, unknown table/column, wrong number of parameters, ..."""


class NotSupportedError(DatabaseError):
    """The feature is not supported by this engine."""
