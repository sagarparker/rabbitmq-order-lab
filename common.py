"""Shared AMQP configuration, order validation, and structured logging."""

import json
import os
from datetime import datetime, timezone

import pika


def log(action, **fields):
    print(json.dumps({"time": datetime.now(timezone.utc).isoformat(),
                      "action": action, **fields}, sort_keys=True), flush=True)


def connect():
    params = pika.URLParameters(os.getenv(
        "RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F"))
    params.heartbeat = 30
    params.blocked_connection_timeout = 30
    params.socket_timeout = 10
    params.connection_attempts = 5
    params.retry_delay = 2
    return pika.BlockingConnection(params)


def validate_order(order):
    if not isinstance(order, dict):
        raise ValueError("order must be a JSON object")
    for key in ("event_id", "order_id", "item_id"):
        if not isinstance(order.get(key), str) or not order[key].strip():
            raise ValueError(f"{key} must be a nonempty string")
    if type(order.get("quantity")) is not int or order["quantity"] < 1:
        raise ValueError("quantity must be a positive integer")
    return order
