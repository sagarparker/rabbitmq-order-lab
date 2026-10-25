"""Run real subprocesses against RabbitMQ and assert the lab requirements."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from common import connect

ROOT = Path(__file__).resolve().parent


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


class Demo:
    def __init__(self, output):
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.workers = []
        self.connection = connect()
        self.channel = self.connection.channel()
        self.queues = []
        self.summary = (self.output / "summary.txt").open("w")

    def note(self, text):
        print(text, flush=True)
        self.summary.write(text + "\n")
        self.summary.flush()

    def queue(self, phase):
        queue = f"orders-demo-{phase}-{uuid.uuid4().hex[:8]}"
        self.channel.queue_declare(queue=queue, durable=True)
        self.queues.append(queue)
        return queue

    def events(self, name):
        path = self.output / f"{name}.log"
        if not path.exists():
            return []
        # Ignore a not-yet-complete final line while the subprocess is writing.
        lines = path.read_text().splitlines(keepends=True)
        return [json.loads(line) for line in lines if line.endswith("\n") and line.startswith("{")]

    def has(self, name, action, event_id=None):
        return any(row["action"] == action and
                   (event_id is None or row.get("event_id") == event_id)
                   for row in self.events(name))

    def start(self, name, queue, store, crash=None):
        command = [sys.executable, str(ROOT / "worker.py"), "--name", name,
                   "--queue", queue, "--store", str(store),
                   "--processing-seconds", "3" if crash else "0.2"]
        if crash:
            command += ["--crash-at", crash]
        stream = (self.output / f"{name}.log").open("w")
        process = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
        self.workers.append((process, stream))
        wait_for(lambda: self.has(name, "READY"), f"{name} ready")
        return process

    def publish(self, queue, event_id=None, count=1):
        command = [sys.executable, str(ROOT / "producer.py"), "--queue", queue,
                   "--count", str(count)]
        if event_id:
            command += ["--event-id", event_id]
        with (self.output / "producer.log").open("a") as stream:
            subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                           check=True, timeout=30)

    def two_consumers(self, queue):
        status = self.channel.queue_declare(queue=queue, passive=True).method
        assert status.consumer_count == 2, status.consumer_count
        self.note(f"BROKER queue={queue} consumers=2")

    def stop_workers(self):
        for process, stream in self.workers:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            stream.close()
        self.workers.clear()

    def competition(self):
        queue = self.queue("competition")
        store = self.output / "competition-orders.jsonl"
        self.start("worker-A", queue, store)
        self.start("worker-B", queue, store)
        self.two_consumers(queue)
        self.publish(queue, count=6)
        names = ("worker-A", "worker-B")

        def actions(action):
            return [row for name in names for row in self.events(name)
                    if row["action"] == action]

        wait_for(lambda: len(actions("ACK_SENT")) == 6, "six completed orders")
        received = actions("RECEIVED")
        assert len(received) == 6
        assert len({row["event_id"] for row in received}) == 6
        assert {row["worker"] for row in received} == set(names)
        assert len(actions("PROCESSED")) == 6
        counts = Counter(row["worker"] for row in received)
        self.note(f"PASS competing consumers: 6 unique deliveries; {dict(sorted(counts.items()))}")
        for row in sorted(received, key=lambda row: row["order_id"]):
            self.note(f"  {row['order_id']} event_id={row['event_id']} -> {row['worker']} (once)")

        event = "duplicate-demo"
        self.publish(queue, event)
        wait_for(lambda: any(self.has(name, "ACK_SENT", event) for name in names), "first copy ACK")
        self.publish(queue, event)
        wait_for(lambda: len([r for r in actions("ACK_SENT") if r["event_id"] == event]) == 2,
                 "duplicate ACK")
        copies = [row for row in actions("RECEIVED") if row["event_id"] == event]
        assert len(copies) == 2 and {row["worker"] for row in copies} == set(names)
        assert len([row for row in actions("PROCESSED") if row["event_id"] == event]) == 1
        assert len([row for row in actions("DUPLICATE") if row["event_id"] == event]) == 1
        rows = [json.loads(line) for line in store.read_text().splitlines()]
        assert len(rows) == 7 and sum(row["event_id"] == event for row in rows) == 1
        self.note("PASS duplicate publication across workers: 2 deliveries, 1 business action, 1 DUPLICATE, 2 ACKs")
        self.stop_workers()

    def failure(self, stage):
        queue = self.queue(stage)
        store = self.output / f"{stage}-orders.jsonl"
        event = f"failure-{stage}"
        doomed_name, survivor_name = f"doomed-{stage}", f"survivor-{stage}"
        doomed = self.start(doomed_name, queue, store, crash=stage)
        self.publish(queue, event)
        wait_for(lambda: self.has(doomed_name, "RECEIVED", event), "doomed worker delivery")
        survivor = self.start(survivor_name, queue, store)
        self.two_consumers(queue)
        wait_for(lambda: doomed.poll() is not None, "worker crash")
        assert doomed.returncode == 42
        assert self.has(doomed_name, "CRASH_BEFORE_ACK", event)
        assert not self.has(doomed_name, "ACK_SENT", event)
        wait_for(lambda: self.has(survivor_name, "ACK_SENT", event), "redelivery ACK")
        received = [row for row in self.events(survivor_name)
                    if row["action"] == "RECEIVED" and row["event_id"] == event]
        assert len(received) == 1 and received[0]["redelivered"] is True
        expected_action = "DUPLICATE" if stage == "after-processing" else "PROCESSED"
        assert self.has(survivor_name, expected_action, event)
        rows = [json.loads(line) for line in store.read_text().splitlines()]
        assert len(rows) == 1 and rows[0]["event_id"] == event
        self.note(f"PASS {stage}: first worker RECEIVED -> exit 42 without ACK -> survivor RECEIVED redelivered=true -> {expected_action} -> ACK_SENT")
        followup = f"followup-{stage}"
        self.publish(queue, followup)
        wait_for(lambda: self.has(survivor_name, "ACK_SENT", followup), "follow-up ACK")
        assert survivor.poll() is None
        status = self.channel.queue_declare(queue=queue, passive=True).method
        assert status.consumer_count == 1 and status.message_count == 0
        self.note(f"PASS system remains running: survivor processed {followup}; broker consumers=1 ready=0")
        self.stop_workers()

    def close(self):
        self.stop_workers()
        if self.connection.is_open:
            for queue in self.queues:
                self.channel.queue_delete(queue=queue)
            self.connection.close()
        self.summary.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(
        "data/demo-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")))
    args = parser.parse_args()
    demo = Demo(args.output)
    try:
        demo.note(f"Run: {datetime.now(timezone.utc).isoformat()}")
        demo.note(f"Python: {sys.version.split()[0]}")
        demo.note(f"RabbitMQ: {demo.connection._impl.server_properties['version']}")
        demo.competition()
        demo.failure("before-processing")
        demo.failure("after-processing")
        demo.note("ALL CHECKS PASSED; demo workers stopped and isolated demo queues removed; RabbitMQ remains running.")
    finally:
        demo.close()
    print(f"Evidence: {demo.output}")


if __name__ == "__main__":
    main()
