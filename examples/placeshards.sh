#!/usr/bin/env bash
# Place mydb.accounts and mydb.notes on the two groups in compose.sharded.yaml
# and load the example rows. Ids below 3 stay on g1–g3. Ids from 3 up stay on
# g4–g6. A later run leaves an existing placement and those rows in place.
#
#	docker compose -f examples/compose.sharded.yaml up --build
#	examples/placeshards.sh

set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
password=${HARDHATDB_BOOTSTRAP_PASSWORD:-dev-only-change-me}
container=$(docker ps -qf name=hardhatdb-sharded-g1-)
if [[ -z "$container" ]]; then
	echo "g1 is not running. Start examples/compose.sharded.yaml first." >&2
	exit 1
fi
network=$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$container")
if [[ -z "$network" ]]; then
	echo "g1 has no network" >&2
	exit 1
fi

sql() {
	docker run --rm --network "$network" \
		-v "$dir/certs/ca.crt:/ca.crt:ro" \
		mysql:8.4 \
		mysql --host=g1 --port=3306 --user=root --password="$password" \
		--ssl-mode=REQUIRED --ssl-ca=/ca.crt --batch --skip-column-names \
		-e "$1"
}

deadline=$((SECONDS + 90))
until sql "SELECT 1" >/dev/null 2>&1; do
	if ((SECONDS >= deadline)); then
		echo "g1 did not accept a connection" >&2
		exit 1
	fi
	sleep 2
done

left=$(sql "SELECT email FROM mydb.accounts WHERE id = 1" 2>/dev/null || true)
right=$(sql "SELECT email FROM mydb.accounts WHERE id = 4" 2>/dev/null || true)
if [[ "$left" == "ada@example.com" && "$right" == "katherine@example.com" ]]; then
	exit 0
fi

if ! sql "SELECT 1 FROM mydb.accounts LIMIT 1" >/dev/null 2>&1; then
	sql "CREATE DATABASE IF NOT EXISTS mydb"
	sql "CREATE TABLE mydb.accounts (
  id BIGINT NOT NULL AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  status TINYINT NOT NULL,
  tags JSON NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY accounts_email (email),
  KEY accounts_status_created (status, created_at)
)"
	sql "CREATE TABLE mydb.notes (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id BIGINT NOT NULL,
  body VARCHAR(255) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  KEY notes_account (account_id),
  CONSTRAINT notes_account_fk FOREIGN KEY (account_id) REFERENCES mydb.accounts (id)
)"
fi

left="GROUP 33333333-3333-3333-3333-333333333331 PEER g1 10.118.0.2:7101 10.118.0.2:7102 PEER g2 10.118.0.3:7101 10.118.0.3:7102 PEER g3 10.118.0.4:7101 10.118.0.4:7102"
right="GROUP 44444444-4444-4444-4444-444444444441 PEER g4 10.118.0.5:7101 10.118.0.5:7102 PEER g5 10.118.0.6:7101 10.118.0.6:7102 PEER g6 10.118.0.7:7101 10.118.0.7:7102"
sql "SHARD TABLE mydb.accounts BY id CHECK email RANGE END 3 $left"
sql "SHARD TABLE mydb.accounts BY id CHECK email RANGE START 3 $right"
sql "SHARD TABLE mydb.notes BY account_id RANGE END 3 $left"
sql "SHARD TABLE mydb.notes BY account_id RANGE START 3 $right"

created="2022-11-01 12:00:00.000001"
sql "INSERT INTO mydb.accounts (id, email, name, status, tags, created_at) VALUES
(1, 'ada@example.com', 'Ada Lovelace', 1, '[\"demo\"]', '$created'),
(2, 'grace@example.com', 'Grace Hopper', 1, '[\"demo\",\"compiler\"]', '$created')"
sql "INSERT INTO mydb.accounts (id, email, name, status, tags, created_at) VALUES
(3, 'alan@example.com', 'Alan Turing', 0, '[]', '$created'),
(4, 'katherine@example.com', 'Katherine Johnson', 1, '[\"orbit\"]', '$created')"
sql "INSERT INTO mydb.notes (id, account_id, body, created_at) VALUES
(1, 1, 'Wrote the first program', '$created'),
(2, 1, 'Cluster demo', '$created'),
(3, 2, 'A compiler is a program', '$created'),
(4, 2, 'Second note', '$created')"
sql "INSERT INTO mydb.notes (id, account_id, body, created_at) VALUES
(5, 3, 'Can machines think', '$created'),
(6, 3, 'Second note', '$created'),
(7, 4, 'Calculated the trajectory', '$created'),
(8, 4, 'Second note', '$created')"
