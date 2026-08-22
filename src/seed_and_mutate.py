"""Generate change, so there is something for CDC to capture.

Seeds a pool of customers, then loops forever doing a mix of inserts, updates
and deletes. The mix matters: a generator that only inserts makes CDC look
easy, because inserts are the one operation where the before-image is always
null and REPLICA IDENTITY never bites you.
"""

from __future__ import annotations

import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass

import psycopg
from faker import Faker

log = logging.getLogger("seed")
fake = Faker()

CHANNELS = ("web", "mobile", "pos", "api")
STATUSES = ("pending", "settled", "refunded", "failed")


@dataclass(frozen=True)
class Settings:
    dsn: str
    customers: int
    interval: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            dsn=os.getenv(
                "DATABASE_URL", "postgresql://cdc:cdc@localhost:5432/shop"
            ),
            customers=int(os.getenv("SEED_CUSTOMERS", "50")),
            interval=float(os.getenv("EVENT_INTERVAL", "1.0")),
        )


_running = True


def _stop(signum, _frame) -> None:
    global _running
    log.info("signal %s — finishing current statement and stopping", signum)
    _running = False


def seed_customers(conn: psycopg.Connection, n: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM customers")
        existing = cur.fetchone()[0]
        if existing >= n:
            cur.execute("SELECT id FROM customers")
            return [r[0] for r in cur.fetchall()]

        rows = [
            (fake.name(), fake.unique.email(), fake.country_code())
            for _ in range(n - existing)
        ]
        cur.executemany(
            "INSERT INTO customers (full_name, email, country) VALUES (%s, %s, %s)",
            rows,
        )
        conn.commit()
        log.info("seeded %d customers", len(rows))
        cur.execute("SELECT id FROM customers")
        return [r[0] for r in cur.fetchall()]


def insert_transaction(cur: psycopg.Cursor, customer_ids: list[int]) -> None:
    cur.execute(
        """INSERT INTO transactions (customer_id, amount, currency, channel, status)
           VALUES (%s, %s, %s, %s, 'pending')""",
        (
            random.choice(customer_ids),
            round(random.uniform(1, 5000), 2),
            random.choice(("USD", "EUR", "GBP", "INR")),
            random.choice(CHANNELS),
        ),
    )


def update_transaction(cur: psycopg.Cursor) -> None:
    """Move a pending row to a terminal state — the interesting CDC event.

    This is where REPLICA IDENTITY FULL earns its keep: without it the change
    event arrives with a before-image of nulls and consumers cannot tell what
    actually changed.
    """
    cur.execute(
        """UPDATE transactions
              SET status = %s, updated_at = now()
            WHERE id = (
                SELECT id FROM transactions
                 WHERE status = 'pending'
                 ORDER BY random() LIMIT 1
            )""",
        (random.choice(STATUSES[1:]),),
    )


def delete_transaction(cur: psycopg.Cursor) -> None:
    cur.execute(
        """DELETE FROM transactions
            WHERE id = (
                SELECT id FROM transactions
                 WHERE status = 'failed'
                 ORDER BY created_at LIMIT 1
            )"""
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s"
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    cfg = Settings.from_env()
    log.info("connecting to %s", cfg.dsn.rsplit("@", 1)[-1])

    with psycopg.connect(cfg.dsn, autocommit=False) as conn:
        customer_ids = seed_customers(conn, cfg.customers)

        # Weighted so most events are inserts and updates; deletes stay rare,
        # which is roughly how an operational table behaves.
        actions = (
            [insert_transaction] * 6
            + [lambda cur, _ids=None: update_transaction(cur)] * 3
            + [lambda cur, _ids=None: delete_transaction(cur)]
        )

        n = 0
        while _running:
            action = random.choice(actions)
            try:
                with conn.cursor() as cur:
                    if action is insert_transaction:
                        action(cur, customer_ids)
                    else:
                        action(cur)
                conn.commit()
                n += 1
                if n % 25 == 0:
                    log.info("%d statements committed", n)
            except psycopg.Error:
                conn.rollback()
                log.exception("statement failed, rolled back")
            time.sleep(cfg.interval)

    log.info("stopped after %d statements", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
