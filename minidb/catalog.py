"""Catalog: the set of table schemas in a database.

The catalog lives in a single ``_catalog.json`` file inside the database
directory. It is small (just column definitions) so we load it fully
into memory at connection time.

A Schema is just a list of column descriptors::

    {"name": "id",   "type": "INTEGER", "primary_key": True,  "not_null": True}
    {"name": "name", "type": "TEXT",    "primary_key": False, "not_null": False}
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Optional

from .exceptions import ProgrammingError
from .types import VALID_TYPES

CATALOG_FILENAME = "_catalog.json"


class Schema:
    """Column definitions for one table."""

    def __init__(self, name: str, columns: List[dict]):
        self.name = name
        self.columns = columns
        self._validate()

    def _validate(self):
        seen = set()
        pk_count = 0
        for col in self.columns:
            cname = col["name"]
            if cname in seen:
                raise ProgrammingError(f"duplicate column name: {cname}")
            seen.add(cname)
            if col["type"] not in VALID_TYPES:
                raise ProgrammingError(
                    f"unknown type {col['type']!r} for column {cname!r}"
                )
            if col.get("primary_key"):
                pk_count += 1
        if pk_count > 1:
            raise ProgrammingError(
                f"table {self.name!r} has more than one PRIMARY KEY"
            )

    @property
    def column_names(self) -> List[str]:
        return [c["name"] for c in self.columns]

    def type_of(self, column: str) -> str:
        for c in self.columns:
            if c["name"] == column:
                return c["type"]
        raise ProgrammingError(f"no such column: {column}")

    def has_column(self, column: str) -> bool:
        return any(c["name"] == column for c in self.columns)

    def is_not_null(self, column: str) -> bool:
        for c in self.columns:
            if c["name"] == column:
                return bool(c.get("not_null", False))
        raise ProgrammingError(f"no such column: {column}")

    @property
    def primary_key(self) -> Optional[str]:
        """Name of the primary-key column, or None."""
        for c in self.columns:
            if c.get("primary_key"):
                return c["name"]
        return None

    def to_dict(self) -> dict:
        return {"name": self.name, "columns": self.columns}

    @classmethod
    def from_dict(cls, data: dict) -> "Schema":
        return cls(data["name"], data["columns"])


class Catalog:
    """All table schemas in a database, loaded and saved as one JSON file."""

    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        self.tables: Dict[str, Schema] = {}
        self._load()

    @property
    def _path(self) -> str:
        return os.path.join(self.db_dir, CATALOG_FILENAME)

    def _load(self):
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as f:
            data = json.load(f)
        for t in data.get("tables", []):
            s = Schema.from_dict(t)
            self.tables[s.name] = s

    def save(self):
        """Atomically write the catalog to disk."""
        payload = {"tables": [s.to_dict() for s in self.tables.values()]}
        tmp = self._path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, self._path)

    # --- mutations --------------------------------------------------
    def add(self, schema: Schema):
        if schema.name in self.tables:
            raise ProgrammingError(f"table already exists: {schema.name}")
        self.tables[schema.name] = schema

    def drop(self, name: str):
        if name not in self.tables:
            raise ProgrammingError(f"no such table: {name}")
        del self.tables[name]

    def get(self, name: str) -> Schema:
        if name not in self.tables:
            raise ProgrammingError(f"no such table: {name}")
        return self.tables[name]

    def __contains__(self, name: str) -> bool:
        return name in self.tables
