# Example web app

The cluster page for `mydb.accounts` and `mydb.notes`. Open it to see each node's `SHOW RAFT STATUS`, choose which address receives the next write, and compare the row on that connection with a read on another node.

`MYSQL_ADDRS` lists every MySQL address. One request stays on one node. A broken connection tries the next address. Creating an account inserts the account and its first note in one transaction, then reads that row back on the same connection. Another connection can still see an older copy. Leave `MYSQL_ADDRS` unset to use `MYSQL_HOST`:`MYSQL_PORT`.

`accounts.id` is the shard column. The ranged stress target places that key on more than one Raft group: `go/performance/stress/run.sh ranged`.

## Run

Run `examples/gencerts.sh` from the repo root first. A volume created for the previous example still has `mydb.mytable`. Remove it before the first start on this schema.

From the repo root, this starts the cluster and all three apps:

```bash
docker compose -f examples/compose.yaml up --build
```

From this directory, this starts the cluster and this app:

```bash
docker compose up --build
```

Open http://localhost:8080.

```bash
curl -s localhost:8080/cluster
curl -s localhost:8080/accounts?size=2
curl -s -X POST localhost:8080/accounts \
  -H 'content-type: application/json' \
  -d '{"name":"Lin","email":"lin@example.com","status":1,"tags":["demo"],"note":"hello","node":"n2:3306"}'
```

This stack publishes MySQL on 3306, 3307, and 3308, the same ports as the repo-root cluster. Stop the other stack before `up`.

To run the app on the host against a cluster that is already up:

```bash
go run .
```

The default CA is `../certs/ca.crt`.

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |
| `HTTP_ADDR` | `:8080` |

The tables already exist. This app does not create them.

Handler tests use an in-memory store and do not need MySQL:

```bash
go test .
```
