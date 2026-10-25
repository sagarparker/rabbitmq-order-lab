"""A shared, locked JSONL journal; one row is the lab's business action.

This local-file design supports multiple processes on one Unix host. The action
and the deduplication record are the SAME durable row, avoiding a separate
side-effect/marker crash window for this simulated action.
"""

import fcntl
import json
import os
from pathlib import Path

from common import log


def complete_order(path, order, worker):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as journal:
        fcntl.flock(journal, fcntl.LOCK_EX)
        journal.seek(0)
        records = [json.loads(line) for line in journal if line.strip()]
        if any(record["event_id"] == order["event_id"] for record in records):
            log("DUPLICATE", worker=worker, event_id=order["event_id"],
                detail="business action skipped")
            return False
        log("PROCESSING", worker=worker, **order)
        journal.seek(0, os.SEEK_END)
        journal.write(json.dumps({**order, "processed_by": worker}) + "\n")
        journal.flush()
        os.fsync(journal.fileno())
        log("PROCESSED", worker=worker, **order)
        return True
