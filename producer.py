"""Publish persistent orders and wait for broker confirmation."""

import argparse
import json
import uuid

import pika

from common import connect, log, validate_order


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="orders")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--event-id", help="Fixed ID for one order; use again to replay it")
    parser.add_argument("--order-id", default="order-001")
    parser.add_argument("--item-id", default="item-001")
    parser.add_argument("--quantity", type=int, default=1)
    args = parser.parse_args()
    if args.count < 1 or args.quantity < 1:
        parser.error("count and quantity must be positive")
    if args.event_id is not None and args.count != 1:
        parser.error("--event-id requires --count 1")
    orders = [validate_order({
        "event_id": args.event_id if args.event_id is not None else str(uuid.uuid4()),
        "order_id": args.order_id if args.count == 1 else f"order-{i + 1:03d}",
        "item_id": args.item_id, "quantity": args.quantity,
    }) for i in range(args.count)]
    connection = connect()
    try:
        channel = connection.channel()
        channel.queue_declare(queue=args.queue, durable=True)
        channel.confirm_delivery()
        for order in orders:
            channel.basic_publish(
                exchange="", routing_key=args.queue,
                body=json.dumps(order).encode(), mandatory=True,
                properties=pika.BasicProperties(
                    content_type="application/json", delivery_mode=2,
                    message_id=order["event_id"],
                ),
            )
            log("PUBLISHED", queue=args.queue, **order)
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
