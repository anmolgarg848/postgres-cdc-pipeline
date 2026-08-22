.PHONY: up down register status tail seed logs reset slot

up:            ## start the stack
	docker compose up -d

down:          ## stop the stack, keep data
	docker compose down

reset:         ## stop and destroy data + replication slot
	docker compose down -v

register:      ## register the Debezium source connector
	curl -sS -X PUT http://localhost:8083/connectors/postgres-source/config \
		-H 'Content-Type: application/json' \
		-d @<(python3 -c "import json;print(json.dumps(json.load(open('connectors/postgres-source.json'))['config']))") \
		| python3 -m json.tool

status:        ## connector + task state
	@curl -sS http://localhost:8083/connectors/postgres-source/status | python3 -m json.tool

slot:          ## replication slot lag — watch this, it pins WAL
	@docker compose exec -T postgres psql -U cdc -d shop -c \
		"SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots;"

seed:          ## generate change against Postgres
	python3 src/seed_and_mutate.py

tail:          ## print change events as they arrive
	python3 src/tail_changes.py

logs:
	docker compose logs -f connect
