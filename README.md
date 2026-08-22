# postgres-cdc-pipeline

Change Data Capture from PostgreSQL into Kafka with Debezium, as a local stack
you can bring up in one command and watch row changes stream past.

Every row written to Postgres already goes into the write-ahead log, because
that is how the database survives a crash. Change Data Capture is the idea of
reading that log and publishing it as a stream, so other systems learn about
changes without polling the database or having the application dual-write.

![CDC architecture: applications, PostgreSQL and users as sources; Debezium capturing changes from PostgreSQL; Kafka as the event streaming platform with ZooKeeper and a control centre for monitoring; Spark, Flink, Storm and ksqlDB as stream processing options; Superset, Elasticsearch, Slack and Telegram as destinations](architecture.png)

## Stack

| Stage | Component |
|---|---|
| Operational database | PostgreSQL 16, `wal_level=logical` |
| CDC connector | Debezium 2.7 (Postgres connector, `pgoutput`) |
| Transport | Kafka + ZooKeeper |
| Ops UI | Kafka UI (topics, consumer groups, connector status) |

Downstream, the change topics are ordinary Kafka topics — a stream processor
(Spark, Flink, Kafka Streams) or a sink connector into Elasticsearch, a
warehouse or a BI tool reads them like any other. This repo stops at the point
where the changes are on the broker, which is the part worth understanding.

## Run it

```bash
make up          # start postgres, kafka, zookeeper, debezium, kafka-ui
make register    # register the source connector
make status      # confirm the connector and its task are RUNNING
```

Then, in two terminals:

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
make seed        # generate inserts, updates and deletes
make tail        # print change events as they land
```

Kafka UI is at http://localhost:8080.

## What the code does

- **`sql/01_schema.sql`** — two tables, and the two settings that make CDC
  behave: `REPLICA IDENTITY FULL` and an explicit publication.
- **`connectors/postgres-source.json`** — the Debezium source connector.
- **`src/seed_and_mutate.py`** — generates a weighted mix of inserts, updates
  and deletes. Not inserts only: inserts are the easy case, because the
  before-image is always null and `REPLICA IDENTITY` never bites you.
- **`src/tail_changes.py`** — flattens Debezium envelopes to one line each and,
  on updates, prints which columns actually changed.

## Three things that are easy to get wrong

**`wal_level` must be `logical`.** The default is `replica`, which is enough
for streaming replication but does not carry the row detail logical decoding
needs. Debezium cannot create a slot without it, and the error does not say so
plainly.

**`REPLICA IDENTITY` decides whether updates are useful.** By default Postgres
logs only the primary key for an UPDATE or DELETE, so the change event's
`before` is almost all nulls and consumers cannot compute a diff. Setting it to
`FULL` logs every column — at the cost of more WAL. `tail_changes.py` prints a
warning when a before-image is missing, because it is a silent failure
otherwise.

**An inactive replication slot will fill the disk.** A slot holds WAL until the
consumer confirms it. Stop Debezium and leave the slot behind, and Postgres
retains WAL segments indefinitely, waiting for a consumer that never returns.
On a laptop that is an annoyance; in production it is an outage. `make slot`
shows how much WAL each slot is pinning:

```
 slot_name | active | retained_wal
-----------+--------+--------------
 cdc_slot  | t      | 16 MB
```

`make reset` tears the volumes down, which is the clean way to drop the slot.

## Licence

MIT — see [LICENSE](LICENSE).
