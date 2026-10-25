# RabbitMQ Order Messaging Lab

A Python producer publishes orders to RabbitMQ. Independent worker processes compete for messages, acknowledge completed work, recover unacknowledged deliveries, and skip previously processed event IDs. The lab's business action is saving a completed order to a shared JSONL journal; it does not call a payment or inventory service.

## Architecture

```text
                        RabbitMQ (independent broker)
Order Producer ──JSON──> durable queue: orders
                              │       │
                 one delivery │       │ one delivery
                              ▼       ▼
                          Worker A  Worker B
                              │       │
                              └───┬───┘
                                  ▼
                      shared data/processed.jsonl
                    (completed orders + event IDs)

Worker ──ACK after durable completion / duplicate check──> RabbitMQ
Worker exits before ACK ──> RabbitMQ requeues ──> surviving worker
```

The producer, each worker, and the broker run independently. Messages use the default exchange and the queue name as their routing key. Workers use manual acknowledgments (`auto_ack=False`) and `prefetch_count=1`, allowing one unacknowledged order per worker. RabbitMQ assigns each normal delivery to one consumer of this queue; it can deliver the same event again after failure.

An order contains these fields:

```json
{
  "event_id": "evt-001",
  "order_id": "order-001",
  "item_id": "item-001",
  "quantity": 2
}
```

## Run RabbitMQ, producer, and two workers

Prerequisites: Python 3.10+ on macOS or Linux, and Docker with the Compose plugin. On Windows, run the Python processes in WSL because the shared journal uses Unix `fcntl.flock`. Start Docker Desktop or your Docker daemon first. Ports 5672 and 15672 must be available.

Clone this repository, enter its directory, and set up the environment:

```sh
git clone https://github.com/sagarparker/rabbitmq-order-lab.git
cd rabbitmq-order-lab
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up -d --wait
```

Compose starts RabbitMQ 4.1.8 with persistent storage and a health check. AMQP listens at `localhost:5672`. The optional management UI is at <http://localhost:15672>, with username `guest` and password `guest`. Docker publishes both ports only on localhost.

In **terminal 1**, from the repository root:

```sh
source .venv/bin/activate
python worker.py --name worker-A
```

In **terminal 2**, from the same repository root:

```sh
source .venv/bin/activate
python worker.py --name worker-B
```

Wait for both workers to log `READY`. In **terminal 3**:

```sh
source .venv/bin/activate
python producer.py --count 6
```

Each published message gets a new UUID `event_id`. Workers log `RECEIVED`, `PROCESSING`, `PROCESSED`, and `ACK_SENT`. `PUBLISHED` means the broker confirmed publication, while `ACK_SENT` means the worker sent its delivery acknowledgment after completing the action. Both workers must use the same `--queue` and the same `--store` path; the defaults are `orders` and `data/processed.jsonl`. Keep this file between worker restarts to retain duplicate protection.

To publish and then replay an identical event, run this command twice:

```sh
python producer.py --count 1 --event-id evt-001 --order-id order-001 --item-id item-001 --quantity 2
```

One delivery records the business action; the next logs `DUPLICATE` and sends an ACK without appending another completed order. A republished duplicate can have `redelivered=false`: RabbitMQ's flag describes broker redelivery, while application duplicate detection uses `event_id`.

## Reproduce every demonstration automatically

With RabbitMQ running and the virtual environment active:

```sh
python -m unittest discover -s tests -v
python demo.py
```

The demo launches actual `producer.py` and `worker.py` subprocesses against the running RabbitMQ broker. It uses fresh, isolated queue names and journal files, so existing `orders` messages and `data/processed.jsonl` are untouched. The output directory defaults to a new `data/demo-<UTC timestamp>` directory; `--output some/new/directory` selects another **new** directory. The demo stops its workers and deletes only its own queues at the end; RabbitMQ stays running.

The demo asserts that:

1. The broker reports two consumers, and six unique orders are received once each across both workers.
2. Publishing the same event twice delivers it to different workers, produces one journal row, logs one duplicate, and acknowledges both copies.
3. A worker exits with code 42 before processing and without ACK; a second worker receives the same event with `redelivered=true`, completes it, and acknowledges it.
4. A worker exits after saving the business action but before ACK; a second worker detects the redelivered event as a duplicate and acknowledges it without repeating the action.
5. After each crash, the surviving worker stays alive and successfully processes another order while the broker remains available.

The unit tests independently race 12 attempts across four processes to verify that the shared file records only one business action. They also verify persistence across later attempts and reject invalid order fields.

## Captured results and evidence

The committed evidence comes from a real local run using Python 3.13.14, Pika 1.3.2, and RabbitMQ 4.1.8. Log timestamps are UTC. Run command:

```sh
python demo.py --output evidence/demo
```

The directory already exists in this repository; choose a new output path when rerunning. The complete [demo summary](evidence/demo/summary.txt), [producer output](evidence/demo/producer.log), [unit test output](evidence/unit-tests.txt), and [broker health check after the demo](evidence/broker-status.txt) are committed. The excerpts below are selected raw JSON log lines, in timestamp order; omitted lines remain in the linked files.

### Successful processing

From [worker A's log](evidence/demo/worker-A.log), the order is received, recorded, and then acknowledged:

```json
{"action": "RECEIVED", "event_id": "68876eb3-6f58-4367-b794-84339cca5333", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": false, "time": "2026-10-25T05:00:27.401884+00:00", "worker": "worker-A"}
{"action": "PROCESSING", "event_id": "68876eb3-6f58-4367-b794-84339cca5333", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:27.603449+00:00", "worker": "worker-A"}
{"action": "PROCESSED", "event_id": "68876eb3-6f58-4367-b794-84339cca5333", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:27.603681+00:00", "worker": "worker-A"}
{"action": "ACK_SENT", "event_id": "68876eb3-6f58-4367-b794-84339cca5333", "time": "2026-10-25T05:00:27.603900+00:00", "worker": "worker-A"}
```

The corresponding durable row is in [the completed-order journal](evidence/demo/competition-orders.jsonl).

### Two competing consumers

From the [demo summary](evidence/demo/summary.txt), the broker reported two registered consumers before publication. Each of the six unique event IDs appears in exactly one worker's `RECEIVED` logs:

```text
BROKER queue=orders-demo-competition-249edf56 consumers=2
PASS competing consumers: 6 unique deliveries; {'worker-A': 3, 'worker-B': 3}
  order-001 event_id=68876eb3-6f58-4367-b794-84339cca5333 -> worker-A (once)
  order-002 event_id=3f85b4e3-90b9-4f37-b499-3ae1f776fac4 -> worker-B (once)
  order-003 event_id=2704a7ae-6b0f-44c3-94c1-53dd92817a6a -> worker-A (once)
  order-004 event_id=842b88aa-b846-423c-a9dd-c629324ce405 -> worker-B (once)
  order-005 event_id=3bf37a8a-35db-47f1-bc7b-fe454872e93c -> worker-B (once)
  order-006 event_id=18a1b677-03bc-4643-852c-f23195dcdbe4 -> worker-A (once)
```

See [worker A](evidence/demo/worker-A.log) and [worker B](evidence/demo/worker-B.log) for the independent process IDs, received messages, and ACKs. This run split the six orders 3/3; equal allocation and global processing order are not guaranteed.

### Failure before processing and redelivery

From [the first worker](evidence/demo/doomed-before-processing.log) and [the survivor](evidence/demo/survivor-before-processing.log):

```json
{"action": "RECEIVED", "event_id": "failure-before-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": false, "time": "2026-10-25T05:00:28.882166+00:00", "worker": "doomed-before-processing"}
{"action": "CRASH_BEFORE_ACK", "event_id": "failure-before-processing", "exit_code": 42, "stage": "before-processing", "time": "2026-10-25T05:00:31.883356+00:00", "worker": "doomed-before-processing"}
{"action": "RECEIVED", "event_id": "failure-before-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": true, "time": "2026-10-25T05:00:31.888029+00:00", "worker": "survivor-before-processing"}
{"action": "PROCESSING", "event_id": "failure-before-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:32.089696+00:00", "worker": "survivor-before-processing"}
{"action": "PROCESSED", "event_id": "failure-before-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:32.089897+00:00", "worker": "survivor-before-processing"}
{"action": "ACK_SENT", "event_id": "failure-before-processing", "time": "2026-10-25T05:00:32.090080+00:00", "worker": "survivor-before-processing"}
```

The first process deliberately calls `os._exit(42)` without closing its connection gracefully or acknowledging the message. The broker detects the closed connection and requeues the delivery. The survivor was already registered before the crash, receives the same `event_id` with `redelivered=true`, and sends the ACK after processing. It then completes `followup-before-processing`, proving that the system continues running.

For a manual reproduction, start this worker in one terminal:

```sh
python worker.py --name doomed --queue failure-manual --store data/failure-manual.jsonl --processing-seconds 10 --crash-at before-processing
```

In another terminal, publish to it and immediately start the survivor during the ten-second preparation delay:

```sh
python producer.py --queue failure-manual --count 1 --event-id manual-failure-001
python worker.py --name survivor --queue failure-manual --store data/failure-manual.jsonl
```

Use a new event ID for each new manual experiment. The doomed worker exits on its first valid order. The survivor keeps consuming until Ctrl+C.

### Duplicate handling across workers

This event was deliberately published twice. The [first worker](evidence/demo/worker-A.log) completed it, and [the other worker](evidence/demo/worker-B.log) skipped the second copy:

```json
{"action": "RECEIVED", "event_id": "duplicate-demo", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": false, "time": "2026-10-25T05:00:28.117289+00:00", "worker": "worker-A"}
{"action": "PROCESSING", "event_id": "duplicate-demo", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:28.318539+00:00", "worker": "worker-A"}
{"action": "PROCESSED", "event_id": "duplicate-demo", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:28.318649+00:00", "worker": "worker-A"}
{"action": "ACK_SENT", "event_id": "duplicate-demo", "time": "2026-10-25T05:00:28.318749+00:00", "worker": "worker-A"}
{"action": "RECEIVED", "event_id": "duplicate-demo", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": false, "time": "2026-10-25T05:00:28.417868+00:00", "worker": "worker-B"}
{"action": "DUPLICATE", "detail": "business action skipped", "event_id": "duplicate-demo", "time": "2026-10-25T05:00:28.618721+00:00", "worker": "worker-B"}
{"action": "ACK_SENT", "event_id": "duplicate-demo", "time": "2026-10-25T05:00:28.618971+00:00", "worker": "worker-B"}
```

The [journal](evidence/demo/competition-orders.jsonl) contains exactly one row with `event_id=duplicate-demo`, and both deliveries were acknowledged.

### Crash after processing but before ACK

This additional case tests the critical interval after saving the business action. From [the crashing worker](evidence/demo/doomed-after-processing.log) and [the survivor](evidence/demo/survivor-after-processing.log):

```json
{"action": "RECEIVED", "event_id": "failure-after-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": false, "time": "2026-10-25T05:00:32.643907+00:00", "worker": "doomed-after-processing"}
{"action": "PROCESSING", "event_id": "failure-after-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:35.646546+00:00", "worker": "doomed-after-processing"}
{"action": "PROCESSED", "event_id": "failure-after-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "time": "2026-10-25T05:00:35.647256+00:00", "worker": "doomed-after-processing"}
{"action": "CRASH_BEFORE_ACK", "event_id": "failure-after-processing", "exit_code": 42, "stage": "after-processing", "time": "2026-10-25T05:00:35.647498+00:00", "worker": "doomed-after-processing"}
{"action": "RECEIVED", "event_id": "failure-after-processing", "item_id": "item-001", "order_id": "order-001", "quantity": 1, "redelivered": true, "time": "2026-10-25T05:00:35.654557+00:00", "worker": "survivor-after-processing"}
{"action": "DUPLICATE", "detail": "business action skipped", "event_id": "failure-after-processing", "time": "2026-10-25T05:00:35.856756+00:00", "worker": "survivor-after-processing"}
{"action": "ACK_SENT", "event_id": "failure-after-processing", "time": "2026-10-25T05:00:35.857356+00:00", "worker": "survivor-after-processing"}
```

The [journal](evidence/demo/after-processing-orders.jsonl) has exactly one row for `failure-after-processing`. The survivor also processed and acknowledged `followup-after-processing`. The summary ends with:

```text
ALL CHECKS PASSED; demo workers stopped and isolated demo queues removed; RabbitMQ remains running.
```

## Duplicate tracking and limits

`event_store.py` acquires an exclusive file lock, reads previously completed event IDs, and appends a row only if the event is new. It flushes and `fsync`s the row before releasing the lock. The business action and the processed marker are the same journal row, so the demonstrated crash after completion cannot leave a separate marker unwritten. The worker sends ACK only after this function returns; duplicates are ACKed as well so they leave the queue.

This is a small lab on a single Unix host using a local shared file. Journal scans grow linearly with the number of completed orders, and the file is not designed for multiple hosts, network filesystems, or recovery from a partial write or disk corruption. If replacing the journal action with a real external action, such as charging a card, that action also needs an idempotency key or a transactional design. A separate file marker by itself would not make an external side effect atomic. Messages with malformed JSON or invalid required fields are logged as `REJECTED` and rejected without requeue; this lab has no dead-letter queue. Workers fail on unexpected storage or connection errors; the broker can then requeue their unacknowledged messages for a remaining worker.

The producer uses persistent messages, a durable queue, and publisher confirms. These are separate from consumer ACKs: publication confirmation does not mean an order has been processed. See RabbitMQ's [work queue tutorial](https://www.rabbitmq.com/tutorials/tutorial-two-python) and [acknowledgments and publisher confirms](https://www.rabbitmq.com/docs/confirms) for the underlying delivery behavior.

## Reflection

A message can be delivered more than once when a worker loses its connection or stops before RabbitMQ receives its acknowledgment. The worker may already have completed the business action, so the broker cannot infer from a missing ACK whether the action happened. A producer retry can also publish the same logical event again. Idempotent handling uses a stable `event_id` to recognize completed events, skip repeated actions, and still acknowledge duplicate deliveries so useful work can continue.

## Configuration and cleanup

`RABBITMQ_URL` overrides the default `amqp://guest:guest@localhost:5672/%2F`. Both programs accept `--queue`; workers additionally accept `--name`, `--store`, `--processing-seconds`, and the explicit demo-only `--crash-at` option. Run either program with `--help` for details. Broker credentials belong in the environment rather than source files.

Stop workers with Ctrl+C. Stop the broker while keeping its data volume:

```sh
docker compose down
```

`docker compose down -v` additionally removes this Compose project's RabbitMQ volume and its queued messages. Retain `data/processed.jsonl` while replaying events for which you want duplicate protection.

Source files: [producer](producer.py), [worker](worker.py), [shared helpers](common.py), [event journal](event_store.py), [automated demo](demo.py), and [unit tests](tests/test_event_store.py).
