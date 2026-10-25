"""Verify schema constraints and cross-process deduplication without a broker."""

from concurrent.futures import ProcessPoolExecutor
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from common import validate_order
from event_store import complete_order

ORDER = {"event_id": "same-event", "order_id": "order-1", "item_id": "item-1", "quantity": 2}


def attempt(path):
    with contextlib.redirect_stdout(io.StringIO()):
        return complete_order(path, ORDER, "test-worker")


class EventStoreTests(unittest.TestCase):
    def test_concurrent_workers_write_one_business_action(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "processed.jsonl")
            with ProcessPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(attempt, [path] * 12))
            self.assertEqual(sum(results), 1)
            rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_id"], ORDER["event_id"])
            # A later process/restart must still skip the persisted event.
            self.assertFalse(attempt(path))

    def test_invalid_orders_are_rejected(self):
        for value in (None, [], {}, {**ORDER, "quantity": True},
                      {**ORDER, "quantity": 0}, {**ORDER, "event_id": " "}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_order(value)
        self.assertEqual(validate_order(ORDER), ORDER)


if __name__ == "__main__":
    unittest.main()
