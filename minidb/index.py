"""Hash index.

The only index type we implement: a Python ``dict`` mapping a primary
key value to a row id (an integer position inside a Table).

Why hash and not B-tree? Hashes are the simplest fast lookup structure
anyone has seen and the core idea is "a dict is a hash table". A B-tree
would give us ordered range scans but nearly double the code.

What the index buys us: when a query is ``WHERE pk = something`` we can
answer it with one dict lookup instead of scanning the whole table. The
engine checks for this pattern in ``engine.py``; everything else falls
back to a full scan.
"""

from __future__ import annotations

from typing import Dict, Optional

from .exceptions import IntegrityError


class HashIndex:
    """Single-column unique hash index over a primary key."""

    def __init__(self, column: str):
        self.column = column
        self._map: Dict[object, int] = {}  # pk value -> row id

    # --- lookups ----------------------------------------------------
    def find(self, key) -> Optional[int]:
        """Return the row id for *key*, or None if missing."""
        return self._map.get(key)

    def contains(self, key) -> bool:
        return key in self._map

    # --- maintenance ------------------------------------------------
    def insert(self, key, row_id: int):
        if key in self._map:
            raise IntegrityError(
                f"duplicate value for primary key {self.column!r}: {key!r}"
            )
        self._map[key] = row_id

    def remove(self, key):
        self._map.pop(key, None)

    def rebuild(self, rows):
        """Drop everything and re-scan *rows*.

        ``rows`` is an iterable of (row_id, row_dict). Called after
        operations that could leave the index stale (e.g. rollback).
        """
        self._map.clear()
        for rid, row in rows:
            val = row.get(self.column)
            if val is None:
                continue
            if val in self._map:
                raise IntegrityError(
                    f"duplicate value for primary key {self.column!r}: {val!r}"
                )
            self._map[val] = rid

    def __len__(self) -> int:
        return len(self._map)
