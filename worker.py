"""Consume orders with manual ACKs, shared deduplication, and crash injection."""

import argparse
import json
import os

from common import connect, log, validate_order
from event_store import complete_order


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--queue", default="orders")
    parser.add_argument("--store", default="data/processed.jsonl")
    parser.add_argument("--processing-seconds", type=float, default=1)
    parser.add_argument("--crash-at", choices=("before-processing", "after-processing"),
                        help="Exit abruptly on the first valid message, before ACK")
    args = parser.parse_args()
    if not 0 <= args.processing_seconds <= 60:
        parser.error("processing-seconds must be between 0 and 60")
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=args.queue, durable=True)
    channel.basic_qos(prefetch_count=1)

    def crash(order):
        log("CRASH_BEFORE_ACK", worker=args.name, event_id=order["event_id"],
            stage=args.crash_at, exit_code=42)
        # No graceful close or ACK: the broker must recover this delivery.
        os._exit(42)

    def consume(ch, method, properties, body):
        try:
            order = validate_order(json.loads(body))
        except (ValueError, UnicodeDecodeError) as exc:
            log("REJECTED", worker=args.name, error=str(exc))
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        log("RECEIVED", worker=args.name, redelivered=method.redelivered, **order)
        # Connection.sleep services heartbeats while simulating preparation.
        connection.sleep(args.processing_seconds)
        if args.crash_at == "before-processing":
            crash(order)
        complete_order(args.store, order, args.name)
        if args.crash_at == "after-processing":
            crash(order)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log("ACK_SENT", worker=args.name, event_id=order["event_id"])

    channel.basic_consume(queue=args.queue, on_message_callback=consume, auto_ack=False)
    log("READY", worker=args.name, queue=args.queue, pid=os.getpid(), prefetch=1)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
