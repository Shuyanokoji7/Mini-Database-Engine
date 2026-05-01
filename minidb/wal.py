"""Write-Ahead Log.

The WAL provides durability and crash recovery. The discipline is the
classic one used by every real database: *write the log to disk before
you write the data to disk*. If we crash after a commit reaches the WAL
but before the data files are flushed, recovery on the next start
replays the WAL and we lose nothing.

File format
-----------
One JSON object per line. A transaction looks like::

    {"txid": 7, "op": "INSERT", "table": "users", "row": {...}}
    {"txid": 7, "op": "UPDATE", "table": "users", "pk": 1, "old_row": {...}, "new_row": {...}}
    {"txid": 7, "op": "COMMIT"}

The COMMIT marker tells recovery this transaction is final and should
be replayed. Records without a matching COMMIT are uncommitted (the
process died mid-transaction) and are discarded on recovery.

When ``commit()`` succeeds and the data files are durably flushed, the
WAL is truncated — the records it held are now redundant.
"""

from __future__ import annotations

import json
import os
from typing import List


class WAL:
    """Append-only log file inside the database directory."""

    FILENAME = "wal.log"

    def __init__(self, db_path: str):
        os.makedirs(db_path, exist_ok=True)
        self.path = os.path.join(db_path, self.FILENAME)
        # 'a+' so we can both append new records and read existing ones.
        # buffering=1 = line-buffered; combined with explicit flush+fsync
        # in flush() this gives us durability when we ask for it.
        self.file = open(self.path, "a+", buffering=1)

    def log(self, record: dict):
        """Append one record. Not durable until flush()."""
        self.file.write(json.dumps(record))
        self.file.write("\n")

    def flush(self):
        """Make every log() call so far durable on disk (fsync)."""
        self.file.flush()
        try:
            os.fsync(self.file.fileno())
        except OSError:
            # Some test/temp filesystems don't support fsync; ignore.
            pass

    def read_all(self) -> List[dict]:
        """Return every record currently in the log."""
        self.file.flush()
        records: List[dict] = []
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # A torn final line from a crash — stop here.
                    break
        return records

    def clear(self):
        """Truncate the WAL. Call after data has been durably flushed."""
        try:
            self.file.close()
        except Exception:
            pass
        # Truncate the underlying file and reopen.
        open(self.path, "w").close()
        self.file = open(self.path, "a+", buffering=1)

    def close(self):
        try:
            if not self.file.closed:
                self.file.close()
        except Exception:
            pass
