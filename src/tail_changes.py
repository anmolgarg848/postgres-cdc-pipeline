"""Print Debezium change events as they land, in a form a human can read.

Debezium envelopes are verbose. This flattens each one to a single line so the
shape of the stream is visible: which table, which operation, and what actually
changed on an update.
"""

from __future__ import annotations

import json
import logging
import os
import sys

from kafka import KafkaConsumer

log = logging.getLogger("tail")

OP_NAMES = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def changed_fields(before: dict | None, after: dict | None) -> str:
    """Which columns actually moved. Empty unless REPLICA IDENTITY is FULL."""
    if not before or not after:
        return ""
    diffs = [
        f"{k}: {before.get(k)!r} -> {after[k]!r}"
        for k in after
        if before.get(k) != after[k]
    ]
    return "; ".join(diffs)


def render(topic: str, event: dict) -> str:
    op = OP_NAMES.get(event.get("op"), event.get("op", "?"))
    before, after = event.get("before"), event.get("after")
    row = after or before or {}
    row_id = row.get("id", "?")

    line = f"{topic:24} {op:8} id={row_id}"
    if op == "UPDATE":
        delta = changed_fields(before, after)
        line += f"  {delta}" if delta else "  (no before-image — REPLICA IDENTITY not FULL?)"
    elif op in ("INSERT", "SNAPSHOT"):
        line += f"  amount={row.get('amount')} status={row.get('status')}"
    return line


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
        group_id=os.getenv("CONSUMER_GROUP", "cdc-tail"),
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b) if b else None,
        consumer_timeout_ms=int(os.getenv("IDLE_TIMEOUT_MS", "0")) or None,
    )
    pattern = os.getenv("TOPIC_PATTERN", "^shop\\..*")
    consumer.subscribe(pattern=pattern)
    log.info("subscribed to %s", pattern)

    try:
        for msg in consumer:
            if msg.value is None:  # tombstone
                log.info("%s TOMBSTONE key=%s", msg.topic, msg.key)
                continue
            log.info(render(msg.topic, msg.value))
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
