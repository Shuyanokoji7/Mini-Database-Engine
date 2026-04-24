"""SQL tokenizer + recursive-descent parser.

This module turns text like::

    SELECT id, name FROM users WHERE age > ?

into an abstract syntax tree (AST). The AST is a set of small
dataclasses defined in ``ast_nodes``. The engine walks these nodes to
execute the query.

Supported SQL (a tiny, tidy subset):
    CREATE TABLE t (col TYPE [PRIMARY KEY], ...)
    DROP TABLE t
    INSERT INTO t [(cols...)] VALUES (vals...)
    SELECT * | col, col FROM t [WHERE cond]
    UPDATE t SET col = val [, col = val ...] [WHERE cond]
    DELETE FROM t [WHERE cond]

Conditions: col OP value [AND|OR col OP value ...]
Operators:  =  !=  <  >  <=  >=
Values:     integer | real | 'string' | NULL | ?  (parameter)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

from .exceptions import ProgrammingError

# ============================================================
# AST nodes
# ============================================================

@dataclass
class ColumnDef:
    name: str
    type: str
    primary_key: bool = False


@dataclass
class CreateTable:
    table: str
    columns: List[ColumnDef]


@dataclass
class DropTable:
    table: str


@dataclass
class Insert:
    table: str
    columns: Optional[List[str]]  # None = "all columns, in schema order"
    values: List[Any]             # literals or Placeholder


@dataclass
class Select:
    table: str
    columns: List[str]            # ["*"] or list of column names
    where: Optional["Condition"] = None


@dataclass
class Update:
    table: str
    assignments: List[Tuple[str, Any]]  # (col, value)
    where: Optional["Condition"] = None


@dataclass
class Delete:
    table: str
    where: Optional["Condition"] = None


@dataclass
class Placeholder:
    """A ``?`` in the SQL, to be filled in from the parameters tuple."""
    index: int  # 0-based position in the parameters tuple


@dataclass
class Comparison:
    """Leaf condition: column OP value."""
    column: str
    op: str            # "=", "!=", "<", ">", "<=", ">="
    value: Any         # literal or Placeholder


@dataclass
class BoolOp:
    """AND / OR of two conditions."""
    op: str            # "AND" or "OR"
    left: "Condition"
    right: "Condition"


Condition = Any  # Comparison | BoolOp


# ============================================================
# Tokenizer
# ============================================================

KEYWORDS = {
    "CREATE", "TABLE", "DROP", "INSERT", "INTO", "VALUES", "SELECT",
    "FROM", "WHERE", "AND", "OR", "UPDATE", "SET", "DELETE",
    "PRIMARY", "KEY", "NULL",
    # types
    "INTEGER", "TEXT", "REAL",
}

# token kinds
T_KEYWORD = "KEYWORD"
T_IDENT = "IDENT"
T_INT = "INT"
T_REAL = "REAL"
T_STR = "STR"
T_OP = "OP"
T_LPAREN = "LPAREN"
T_RPAREN = "RPAREN"
T_COMMA = "COMMA"
T_STAR = "STAR"
T_SEMI = "SEMI"
T_PARAM = "PARAM"
T_EOF = "EOF"


def tokenize(sql: str) -> List[Tuple[str, Any]]:
    """Break *sql* into a list of (kind, value) tokens."""
    tokens: List[Tuple[str, Any]] = []
    i, n = 0, len(sql)

    while i < n:
        c = sql[i]

        # whitespace
        if c.isspace():
            i += 1
            continue

        # line comment: -- ...
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            while i < n and sql[i] != "\n":
                i += 1
            continue

        # punctuation
        if c == "(":
            tokens.append((T_LPAREN, "(")); i += 1; continue
        if c == ")":
            tokens.append((T_RPAREN, ")")); i += 1; continue
        if c == ",":
            tokens.append((T_COMMA, ",")); i += 1; continue
        if c == ";":
            tokens.append((T_SEMI, ";")); i += 1; continue
        if c == "*":
            tokens.append((T_STAR, "*")); i += 1; continue
        if c == "?":
            tokens.append((T_PARAM, "?")); i += 1; continue

        # operators
        if c in "=<>!":
            if i + 1 < n and sql[i + 1] == "=":
                op = sql[i : i + 2]
                i += 2
                tokens.append((T_OP, op))
                continue
            if c == "!":
                raise ProgrammingError("expected '!=' after '!'")
            tokens.append((T_OP, c))
            i += 1
            continue

        # string literal: 'text' with '' as literal '
        if c == "'":
            i += 1
            buf = []
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        buf.append("'")
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(sql[i])
                i += 1
            else:
                raise ProgrammingError("unterminated string literal")
            tokens.append((T_STR, "".join(buf)))
            continue

        # numeric literal (integer or real)
        if c.isdigit() or (c == "-" and i + 1 < n and sql[i + 1].isdigit()):
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            is_real = False
            if j < n and sql[j] == ".":
                is_real = True
                j += 1
                while j < n and sql[j].isdigit():
                    j += 1
            lexeme = sql[i:j]
            tokens.append((T_REAL, float(lexeme)) if is_real else (T_INT, int(lexeme)))
            i = j
            continue

        # identifier or keyword
        if c.isalpha() or c == "_":
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            word = sql[i:j]
            up = word.upper()
            if up in KEYWORDS:
                tokens.append((T_KEYWORD, up))
            else:
                tokens.append((T_IDENT, word))
            i = j
            continue

        raise ProgrammingError(f"unexpected character {c!r} at position {i}")

    tokens.append((T_EOF, None))
    return tokens


# ============================================================
# Parser
# ============================================================

class Parser:
    """Recursive-descent parser.

    The entry point is ``parse``. Each ``_parse_xxx`` method consumes
    tokens and returns a piece of AST. ``_peek``, ``_eat``, and
    ``_expect`` are the helpers they share.
    """

    def __init__(self, tokens: List[Tuple[str, Any]]):
        self.tokens = tokens
        self.pos = 0
        self._param_count = 0  # numbered as we encounter '?'

    # --- token helpers ---------------------------------------------
    def _peek(self, offset: int = 0) -> Tuple[str, Any]:
        return self.tokens[self.pos + offset]

    def _eat(self) -> Tuple[str, Any]:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def _expect(self, kind: str, value: Any = None) -> Tuple[str, Any]:
        tok = self._eat()
        if tok[0] != kind or (value is not None and tok[1] != value):
            want = f"{kind}({value})" if value is not None else kind
            raise ProgrammingError(f"expected {want}, got {tok[0]}({tok[1]!r})")
        return tok

    def _match_keyword(self, *keywords: str) -> bool:
        tok = self._peek()
        return tok[0] == T_KEYWORD and tok[1] in keywords

    # --- entry point -----------------------------------------------
    def parse(self):
        if not self._match_keyword(
            "CREATE", "DROP", "INSERT", "SELECT", "UPDATE", "DELETE"
        ):
            tok = self._peek()
            raise ProgrammingError(f"unexpected start of statement: {tok[1]!r}")

        kw = self._peek()[1]
        if kw == "CREATE":
            node = self._parse_create_table()
        elif kw == "DROP":
            node = self._parse_drop_table()
        elif kw == "INSERT":
            node = self._parse_insert()
        elif kw == "SELECT":
            node = self._parse_select()
        elif kw == "UPDATE":
            node = self._parse_update()
        elif kw == "DELETE":
            node = self._parse_delete()

        # Optional trailing semicolon
        if self._peek()[0] == T_SEMI:
            self._eat()
        if self._peek()[0] != T_EOF:
            tok = self._peek()
            raise ProgrammingError(f"unexpected trailing token: {tok[1]!r}")
        return node

    # --- statements ------------------------------------------------
    def _parse_create_table(self) -> CreateTable:
        self._expect(T_KEYWORD, "CREATE")
        self._expect(T_KEYWORD, "TABLE")
        table = self._expect(T_IDENT)[1]
        self._expect(T_LPAREN)
        columns = [self._parse_column_def()]
        while self._peek()[0] == T_COMMA:
            self._eat()
            columns.append(self._parse_column_def())
        self._expect(T_RPAREN)
        return CreateTable(table=table, columns=columns)

    def _parse_column_def(self) -> ColumnDef:
        name = self._expect(T_IDENT)[1]
        type_tok = self._eat()
        if type_tok[0] != T_KEYWORD or type_tok[1] not in ("INTEGER", "TEXT", "REAL"):
            raise ProgrammingError(
                f"expected column type, got {type_tok[1]!r}"
            )
        pk = False
        if self._match_keyword("PRIMARY"):
            self._eat()
            self._expect(T_KEYWORD, "KEY")
            pk = True
        return ColumnDef(name=name, type=type_tok[1], primary_key=pk)

    def _parse_drop_table(self) -> DropTable:
        self._expect(T_KEYWORD, "DROP")
        self._expect(T_KEYWORD, "TABLE")
        table = self._expect(T_IDENT)[1]
        return DropTable(table=table)

    def _parse_insert(self) -> Insert:
        self._expect(T_KEYWORD, "INSERT")
        self._expect(T_KEYWORD, "INTO")
        table = self._expect(T_IDENT)[1]

        columns: Optional[List[str]] = None
        if self._peek()[0] == T_LPAREN:
            self._eat()
            columns = [self._expect(T_IDENT)[1]]
            while self._peek()[0] == T_COMMA:
                self._eat()
                columns.append(self._expect(T_IDENT)[1])
            self._expect(T_RPAREN)

        self._expect(T_KEYWORD, "VALUES")
        self._expect(T_LPAREN)
        values = [self._parse_value()]
        while self._peek()[0] == T_COMMA:
            self._eat()
            values.append(self._parse_value())
        self._expect(T_RPAREN)
        return Insert(table=table, columns=columns, values=values)

    def _parse_select(self) -> Select:
        self._expect(T_KEYWORD, "SELECT")
        columns: List[str]
        if self._peek()[0] == T_STAR:
            self._eat()
            columns = ["*"]
        else:
            columns = [self._expect(T_IDENT)[1]]
            while self._peek()[0] == T_COMMA:
                self._eat()
                columns.append(self._expect(T_IDENT)[1])
        self._expect(T_KEYWORD, "FROM")
        table = self._expect(T_IDENT)[1]
        where = self._parse_optional_where()
        return Select(table=table, columns=columns, where=where)

    def _parse_update(self) -> Update:
        self._expect(T_KEYWORD, "UPDATE")
        table = self._expect(T_IDENT)[1]
        self._expect(T_KEYWORD, "SET")
        assignments = [self._parse_assignment()]
        while self._peek()[0] == T_COMMA:
            self._eat()
            assignments.append(self._parse_assignment())
        where = self._parse_optional_where()
        return Update(table=table, assignments=assignments, where=where)

    def _parse_assignment(self) -> Tuple[str, Any]:
        col = self._expect(T_IDENT)[1]
        self._expect(T_OP, "=")
        val = self._parse_value()
        return (col, val)

    def _parse_delete(self) -> Delete:
        self._expect(T_KEYWORD, "DELETE")
        self._expect(T_KEYWORD, "FROM")
        table = self._expect(T_IDENT)[1]
        where = self._parse_optional_where()
        return Delete(table=table, where=where)

    # --- WHERE clause ----------------------------------------------
    def _parse_optional_where(self) -> Optional[Condition]:
        if not self._match_keyword("WHERE"):
            return None
        self._eat()
        return self._parse_or()

    def _parse_or(self) -> Condition:
        left = self._parse_and()
        while self._match_keyword("OR"):
            self._eat()
            right = self._parse_and()
            left = BoolOp(op="OR", left=left, right=right)
        return left

    def _parse_and(self) -> Condition:
        left = self._parse_comparison()
        while self._match_keyword("AND"):
            self._eat()
            right = self._parse_comparison()
            left = BoolOp(op="AND", left=left, right=right)
        return left

    def _parse_comparison(self) -> Comparison:
        col = self._expect(T_IDENT)[1]
        op_tok = self._eat()
        if op_tok[0] != T_OP or op_tok[1] not in ("=", "!=", "<", ">", "<=", ">="):
            raise ProgrammingError(f"expected comparison operator, got {op_tok[1]!r}")
        val = self._parse_value()
        return Comparison(column=col, op=op_tok[1], value=val)

    # --- values ----------------------------------------------------
    def _parse_value(self) -> Any:
        tok = self._eat()
        kind, val = tok
        if kind == T_INT or kind == T_REAL or kind == T_STR:
            return val
        if kind == T_KEYWORD and val == "NULL":
            return None
        if kind == T_PARAM:
            idx = self._param_count
            self._param_count += 1
            return Placeholder(index=idx)
        raise ProgrammingError(f"expected value, got {val!r}")


def parse(sql: str):
    """Parse *sql* into an AST node."""
    return Parser(tokenize(sql)).parse()
