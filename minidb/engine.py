"""Query execution engine.

The engine takes an AST (from ``parser``) plus a parameters tuple and
carries out the operation against the catalog, storage, and indexes.

Transaction model
-----------------
DML statements (INSERT/UPDATE/DELETE) take effect *immediately* in
memory — after running the statement, a SELECT in the same transaction
sees the change. (This is what every real database does, and it's
necessary for "read-your-own-writes".)

But the same statement also appends two records to the connection:

* an **undo** record (used by ``rollback``)
* a **redo** record (written to the WAL on ``commit``)

So:

* ``commit()``  → write redo log to WAL, fsync, flush data files,
                  truncate WAL, drop the in-memory undo log.
* ``rollback()`` → walk the undo log in reverse and restore prior state.

DDL (CREATE / DROP TABLE) is auto-committed: it changes the catalog and
files immediately and is not transactional. This matches sqlite3's
default behaviour.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from . import parser as p
from .catalog import Catalog, Schema
from .exceptions import (
    DataError,
    IntegrityError,
    NotSupportedError,
    ProgrammingError,
)
from .index import HashIndex
from .storage import Table
from .types import INTEGER, PYTHON_TYPE, coerce


# ============================================================
# Database: the top-level container
# ============================================================

class Database:
    """Holds the catalog, all tables, and all indexes for one DB directory."""

    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        self.catalog = Catalog(db_dir)
        self.storage: Dict[str, Table] = {}
        self.indexes: Dict[str, HashIndex] = {}

        # Load each existing table + build its PK index.
        for name, schema in self.catalog.tables.items():
            self._open_table(name, schema)

    def _open_table(self, name: str, schema: Schema):
        table = Table(self.db_dir, name)
        self.storage[name] = table
        pk = schema.primary_key
        if pk is not None:
            idx = HashIndex(pk)
            idx.rebuild(table.scan())
            self.indexes[name] = idx

    # --- transaction boundary --------------------------------------
    def commit(self):
        """Write every dirty table's rows out to disk."""
        self.catalog.save()
        for t in self.storage.values():
            t.flush()

    def reload_from_disk(self):
        """Throw away in-memory state, re-read everything from disk.

        Used as a heavy-weight reset (e.g. when the WAL is being applied
        on recovery and we want a clean baseline first).
        """
        self.catalog = Catalog(self.db_dir)
        self.storage.clear()
        self.indexes.clear()
        for name, schema in self.catalog.tables.items():
            self._open_table(name, schema)


# ============================================================
# Engine: one per cursor; talks to the connection it belongs to.
# ============================================================

class Engine:
    """Executes parsed statements against the database.

    The engine is given a back-reference to the owning ``Connection`` so
    it can append undo/redo records as it runs DML.
    """

    def __init__(self, db: Database, connection):
        self.db = db
        self.conn = connection

    # --- entry point -----------------------------------------------
    def execute(self, ast, params: Tuple[Any, ...]):
        """Execute *ast* with *params*.

        Returns ``(rows, description, rowcount)``:

        * ``rows``        — list of tuples for SELECT, else ``[]``
        * ``description`` — PEP 249 description list for SELECT, else ``None``
        * ``rowcount``    — affected rows, or ``-1`` for DDL
        """
        if isinstance(ast, p.CreateTable):
            return self._create_table(ast)
        if isinstance(ast, p.DropTable):
            return self._drop_table(ast)
        if isinstance(ast, p.Insert):
            return self._insert(ast, params)
        if isinstance(ast, p.Select):
            return self._select(ast, params)
        if isinstance(ast, p.Update):
            return self._update(ast, params)
        if isinstance(ast, p.Delete):
            return self._delete(ast, params)
        if isinstance(ast, p.Begin):
            # Implicit transactions are always running, so BEGIN is a
            # no-op — we accept it for compatibility.
            return [], None, -1
        if isinstance(ast, p.Commit):
            self.conn.commit()
            return [], None, -1
        if isinstance(ast, p.Rollback):
            self.conn.rollback()
            return [], None, -1
        raise NotSupportedError(f"unsupported statement: {type(ast).__name__}")

    # --- DDL --------------------------------------------------------
    def _create_table(self, ast: p.CreateTable):
        if ast.if_not_exists and ast.table in self.db.catalog:
            return [], None, -1
        cols = [
            {
                "name": c.name,
                "type": c.type,
                "primary_key": c.primary_key,
                "not_null": c.not_null,
            }
            for c in ast.columns
        ]
        schema = Schema(name=ast.table, columns=cols)
        self.db.catalog.add(schema)
        # Create an empty backing file so ``cat`` shows the table exists.
        table = Table(self.db.db_dir, ast.table)
        self.db.storage[ast.table] = table
        pk = schema.primary_key
        if pk:
            self.db.indexes[ast.table] = HashIndex(pk)

        # DDL auto-commits: persist immediately so the schema survives.
        self.db.commit()
        # Any in-flight DML is now stale, so clear the txn state to
        # avoid accidentally rolling back this DDL.
        self.conn._discard_pending_txn_logs()
        return [], None, -1

    def _drop_table(self, ast: p.DropTable):
        if ast.if_exists and ast.table not in self.db.catalog:
            return [], None, -1
        self.db.catalog.drop(ast.table)
        t = self.db.storage.pop(ast.table, None)
        if t is not None:
            t.remove_file()
        self.db.indexes.pop(ast.table, None)
        self.db.commit()
        self.conn._discard_pending_txn_logs()
        return [], None, -1

    # --- DML --------------------------------------------------------
    def _insert(self, ast: p.Insert, params):
        schema = self.db.catalog.get(ast.table)
        table = self.db.storage[ast.table]

        target_cols = ast.columns if ast.columns is not None else schema.column_names
        if len(ast.values) != len(target_cols):
            raise ProgrammingError(
                f"INSERT: got {len(ast.values)} values for {len(target_cols)} columns"
            )

        # Validate that every named column actually exists.
        for c in target_cols:
            if not schema.has_column(c):
                raise ProgrammingError(f"no such column: {c}")

        # Bind placeholders + coerce to column types.
        row: Dict[str, Any] = {}
        for col_name, raw in zip(target_cols, ast.values):
            val = self._resolve(raw, params)
            row[col_name] = coerce(val, schema.type_of(col_name))

        # Any un-mentioned columns become NULL.
        for c in schema.column_names:
            row.setdefault(c, None)

        # Integrity: NOT NULL columns must have a value.
        for c in schema.column_names:
            if schema.is_not_null(c) and row[c] is None:
                raise IntegrityError(f"NOT NULL constraint failed: {ast.table}.{c}")

        # Integrity check on primary key.
        pk = schema.primary_key
        if pk is not None:
            if row[pk] is None:
                raise IntegrityError(f"primary key {pk!r} cannot be NULL")
            idx = self.db.indexes[ast.table]
            if idx.contains(row[pk]):
                raise IntegrityError(
                    f"duplicate value for primary key {pk!r}: {row[pk]!r}"
                )

        row_id = table.insert(row)
        if pk is not None:
            self.db.indexes[ast.table].insert(row[pk], row_id)

        # Log undo + redo for this change.
        self.conn._record_change(
            undo={"op": "INSERT_UNDO", "table": ast.table, "row_id": row_id, "pk_value": row.get(pk) if pk else None},
            redo={"op": "INSERT", "table": ast.table, "row": row},
        )
        return [], None, 1

    def _select(self, ast: p.Select, params):
        schema = self.db.catalog.get(ast.table)
        table = self.db.storage[ast.table]

        # Aggregate path: SELECT COUNT(*) FROM t [WHERE ...]
        if ast.aggregate == "COUNT":
            n = 0
            for _ in self._iter_matching_rows(ast.table, ast.where, params):
                n += 1
            description = [("count", PYTHON_TYPE[INTEGER], None, None, None, None, None)]
            return [(n,)], description, 1

        # Which columns to return?
        if ast.columns == ["*"]:
            out_cols = schema.column_names
        else:
            for c in ast.columns:
                if not schema.has_column(c):
                    raise ProgrammingError(f"no such column: {c}")
            out_cols = ast.columns

        # Collect matching rows (we need the dicts to ORDER BY by any
        # column, including ones not in the SELECT list).
        matched: List[Dict[str, Any]] = []
        for _, row in self._iter_matching_rows(ast.table, ast.where, params):
            matched.append(row)

        # ORDER BY: stable multi-key sort, last key has highest priority
        # in a chain of `sorted()` calls (Python's sort is stable).
        if ast.order_by:
            for term in reversed(ast.order_by):
                if not schema.has_column(term.column):
                    raise ProgrammingError(f"no such column: {term.column}")
                col = term.column
                matched.sort(
                    key=lambda r, c=col: _sort_key(r.get(c)),
                    reverse=term.descending,
                )

        # Project to the requested columns.
        rows = [tuple(row.get(c) for c in out_cols) for row in matched]

        # DISTINCT: dedupe while preserving order.
        if ast.distinct:
            seen = set()
            unique = []
            for r in rows:
                if r not in seen:
                    seen.add(r)
                    unique.append(r)
            rows = unique

        # LIMIT
        if ast.limit is not None:
            rows = rows[: ast.limit]

        description = [
            (col, PYTHON_TYPE[schema.type_of(col)], None, None, None, None, None)
            for col in out_cols
        ]
        return rows, description, len(rows)

    def _update(self, ast: p.Update, params):
        schema = self.db.catalog.get(ast.table)
        table = self.db.storage[ast.table]
        pk = schema.primary_key
        idx = self.db.indexes.get(ast.table)

        # Validate and pre-resolve the SET list.
        prepared: List[Tuple[str, Any]] = []
        for col, raw in ast.assignments:
            if not schema.has_column(col):
                raise ProgrammingError(f"no such column: {col}")
            val = self._resolve(raw, params)
            prepared.append((col, coerce(val, schema.type_of(col))))

        changing_pk = pk is not None and any(c == pk for c, _ in prepared)

        matches = list(self._iter_matching_rows(ast.table, ast.where, params))

        # If the update touches the PK and affects >1 row, we'd create
        # duplicate keys. Reject that up front.
        if changing_pk and len(matches) > 1:
            raise IntegrityError(
                f"UPDATE would set primary key {pk!r} on {len(matches)} rows"
            )

        # Pre-check: a PK change must not collide with another existing row.
        if changing_pk and len(matches) == 1:
            new_pk = dict(prepared)[pk]
            if new_pk is None:
                raise IntegrityError(f"primary key {pk!r} cannot be NULL")
            existing = idx.find(new_pk)
            if existing is not None and existing != matches[0][0]:
                raise IntegrityError(
                    f"duplicate value for primary key {pk!r}: {new_pk!r}"
                )

        # NOT NULL pre-check for the SET list.
        prepared_dict = dict(prepared)
        for col, new_val in prepared_dict.items():
            if schema.is_not_null(col) and new_val is None:
                raise IntegrityError(f"NOT NULL constraint failed: {ast.table}.{col}")

        count = 0
        for row_id, row in matches:
            new_row = dict(row)
            for col, val in prepared:
                new_row[col] = val
            # Keep the index in sync with the new state of the row.
            if pk is not None and idx is not None:
                if row[pk] != new_row[pk]:
                    idx.remove(row[pk])
                    idx.insert(new_row[pk], row_id)
            old_row_copy = dict(row)
            table.update(row_id, new_row)
            count += 1

            self.conn._record_change(
                undo={
                    "op": "UPDATE_UNDO",
                    "table": ast.table,
                    "row_id": row_id,
                    "old_row": old_row_copy,
                    "new_pk_value": new_row.get(pk) if pk else None,
                },
                redo={
                    "op": "UPDATE",
                    "table": ast.table,
                    "pk": old_row_copy.get(pk) if pk else None,
                    "old_row": old_row_copy,
                    "new_row": new_row,
                },
            )
        return [], None, count

    def _delete(self, ast: p.Delete, params):
        schema = self.db.catalog.get(ast.table)
        table = self.db.storage[ast.table]
        pk = schema.primary_key
        idx = self.db.indexes.get(ast.table)

        matches = list(self._iter_matching_rows(ast.table, ast.where, params))
        for row_id, row in matches:
            if pk is not None and idx is not None:
                idx.remove(row[pk])
            row_copy = dict(row)
            table.delete(row_id)

            self.conn._record_change(
                undo={
                    "op": "DELETE_UNDO",
                    "table": ast.table,
                    "row_id": row_id,
                    "row": row_copy,
                },
                redo={
                    "op": "DELETE",
                    "table": ast.table,
                    "pk": row_copy.get(pk) if pk else None,
                    "row": row_copy,
                },
            )
        return [], None, len(matches)

    # ---------------------------------------------------------------
    # Row iteration: either an index lookup or a full scan, plus WHERE
    # filtering. Everything converges on this one helper.
    # ---------------------------------------------------------------
    def _iter_matching_rows(self, table_name: str, where, params):
        schema = self.db.catalog.get(table_name)
        table = self.db.storage[table_name]
        idx = self.db.indexes.get(table_name)
        pk = schema.primary_key

        # Fast path: "WHERE pk = value" -> one dict lookup.
        fast_row_id = None
        if where is not None and idx is not None and pk is not None:
            fast_row_id = self._pk_equality_lookup(where, pk, idx, params)

        if fast_row_id is not None:
            row = table.get(fast_row_id)
            if row is not None and self._matches(row, where, params):
                yield fast_row_id, row
            return

        # Slow path: full scan, filter with WHERE.
        for row_id, row in table.scan():
            if self._matches(row, where, params):
                yield row_id, row

    def _pk_equality_lookup(
        self,
        where,
        pk: str,
        idx: HashIndex,
        params,
    ) -> Optional[int]:
        """If *where* is ``pk = val`` (possibly AND'd with more), return row id."""
        cond = where
        # Descend into the left side of AND nodes -- "pk = v AND x > y"
        # lets us use the index and still filter on x > y afterwards.
        while isinstance(cond, p.BoolOp) and cond.op == "AND":
            cond = cond.left
        if not isinstance(cond, p.Comparison):
            return None
        if cond.column != pk or cond.op != "=":
            return None
        key = self._resolve(cond.value, params)
        return idx.find(key)

    # --- condition evaluation --------------------------------------
    def _matches(self, row, where, params) -> bool:
        if where is None:
            return True
        if isinstance(where, p.BoolOp):
            if where.op == "AND":
                return self._matches(row, where.left, params) and self._matches(
                    row, where.right, params
                )
            if where.op == "OR":
                return self._matches(row, where.left, params) or self._matches(
                    row, where.right, params
                )
            raise ProgrammingError(f"unknown boolean op: {where.op}")

        if isinstance(where, p.Comparison):
            if where.column not in row:
                raise ProgrammingError(f"no such column: {where.column}")
            left = row[where.column]
            right = self._resolve(where.value, params)
            return _apply_op(left, where.op, right)

        raise ProgrammingError(f"unknown condition node: {where!r}")

    # --- parameter binding -----------------------------------------
    def _resolve(self, value, params):
        """Replace a Placeholder with the matching parameter; pass others through."""
        if isinstance(value, p.Placeholder):
            if value.index >= len(params):
                raise ProgrammingError(
                    f"not enough parameters: needed index {value.index}, got {len(params)}"
                )
            return params[value.index]
        return value


def _sort_key(v):
    """Sort key that handles NULLs: NULLs sort first under ASC.

    SQL specifies NULLS FIRST/NULLS LAST as configurable; we pick a
    consistent default: NULLs go to the start in ASC order, which means
    the end under DESC. Implemented by always returning a tuple
    ``(is_not_null, value)`` so None-tuples sort before anything else.
    """
    if v is None:
        return (0, 0)
    return (1, v)


def _like_match(text: str, pattern: str) -> bool:
    """Translate a SQL LIKE pattern to a regex and match.

    ``%`` -> any sequence of characters (including empty).
    ``_`` -> exactly one character.
    Everything else is matched literally.
    """
    # Escape regex specials, then reintroduce SQL wildcards.
    out = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.fullmatch("".join(out), text) is not None


def _apply_op(left, op, right) -> bool:
    """Evaluate a comparison, treating NULL as 'not equal to anything'."""
    if left is None or right is None:
        # SQL's three-valued logic is simplified here: any comparison
        # with NULL is False. That keeps this project tractable.
        return False
    try:
        if op == "=":  return left == right
        if op == "!=": return left != right
        if op == "<":  return left < right
        if op == ">":  return left > right
        if op == "<=": return left <= right
        if op == ">=": return left >= right
        if op == "LIKE":
            if not isinstance(left, str) or not isinstance(right, str):
                raise DataError("LIKE requires TEXT operands")
            return _like_match(left, right)
    except TypeError as e:
        raise DataError(f"cannot compare {left!r} {op} {right!r}: {e}") from e
    raise ProgrammingError(f"unknown operator: {op}")
