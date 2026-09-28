# HardhatDB

A MySQL server backed by one Badger directory. Rows are still there after the process exits.

The documentation site is in [`docs/`](docs/). Pushes to `main` publish it to GitHub Pages.

Run the server from the `go` directory. The example clients take `MYSQL_ADDRS` and send reads and writes to any of those nodes.

## One process

Cluster mode stays off unless `HARDHATDB_RAFT_ADDR` is set.

```bash
HARDHATDB_SEED_EXAMPLE=1 HARDHATDB_BOOTSTRAP_PASSWORD=secret go run ./cmd/hardhatdb sql-server
mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM accounts;"
```

`HARDHATDB_SEED_EXAMPLE=1` creates `mydb.accounts` and `mydb.notes` when those tables are missing. `accounts.id` is the shard column. Leave it unset for an empty data directory. A volume created for the previous example still has `mydb.mytable`. Remove it before the first start on this schema: `docker compose down -v`.

The first boot creates one `mysql_native_password` account. `HARDHATDB_BOOTSTRAP_PASSWORD` is required then and is not saved for later boots; the account lives in the data directory. The default user is `root` and the default host is `%`. A follower that has not received the account yet rejects every login.

The server listens on `localhost:3306`. Data is stored in `data/hardhatdb`. Set `HARDHATDB_DATA` to use another directory. Leave `HARDHATDB_TLS_CERT` and `HARDHATDB_TLS_KEY` unset to stay on plaintext. Set both to require TLS.

Building needs ICU headers. On this machine:

```bash
CGO_CPPFLAGS="-I/opt/homebrew/opt/icu4c/include" \
CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c/lib" \
go run ./cmd/hardhatdb sql-server
```

## One container

```bash
docker compose build
docker run --rm -p 3306:3306 -v hardhatdb-data:/data \
  -e HARDHATDB_MYSQL_HOST=0.0.0.0 \
  -e HARDHATDB_BOOTSTRAP_PASSWORD=secret \
  hardhatdb
```

`HARDHATDB_MYSQL_HOST=0.0.0.0` is required for the published port to accept connections. Data inside the container is `/data/hardhatdb`.

`docker compose up n1` still starts `n2` and `n3`, because `n1` depends on them and every service sets `HARDHATDB_RAFT_ADDR`.

## Local testing

Create the test CA and server certificate before the first `up`. They are not committed. `examples/gencerts.sh` writes them to `examples/certs`. Compose mounts that directory at `/certs`, and Raft reads `ca.crt`, `server.crt`, and `server.key` from there. Without `ca.crt` each node logs `open /certs/ca.crt: no such file or directory` and exits, and Compose restarts it. The server certificate is signed by that CA, carries `serverAuth` and `clientAuth`, and names the addresses clients dial: `127.0.0.1` from the host, the node names, and the private addresses Compose assigns. A stack that is already restarting picks the files up after `docker compose restart`.

```bash
examples/gencerts.sh
docker compose up --build
```

| Node | Host port |
|------|-----------|
| n1   | 3306      |
| n2   | 3307      |
| n3   | 3308      |

Check the leader, then a follower:

```bash
mysql --host=127.0.0.1 --port=3306 --user=root --password=dev-only-change-me \
  --ssl-mode=REQUIRED --ssl-ca=examples/certs/ca.crt \
  mydb --execute="SELECT name, email FROM accounts;"
mysql --host=127.0.0.1 --port=3307 --user=root --password=dev-only-change-me \
  --ssl-mode=REQUIRED --ssl-ca=examples/certs/ca.crt \
  mydb --execute="SELECT name, email FROM accounts;"
```

## Three nodes

Compose sets `HARDHATDB_BOOTSTRAP_PASSWORD` to `dev-only-change-me` unless you override it. That value is used only when a node has no accounts yet. TLS is required on the published MySQL ports. Each node binds Raft and MySQL to its address on the `vpc` network: `10.116.0.2`, `10.116.0.3`, and `10.116.0.4`. Raft on port 7001 and the write-forward port 7002 stay on that network and require mutual TLS. The certificate above is the client certificate as well as the server certificate.

A volume created before those addresses still has the old Raft peer list (`n1:7001` and so on). Remove it before the first start on this plan: `docker compose down -v`.

The same addresses, with host networking and a Cloud Firewall, are what a DigitalOcean deployment uses. See [droplets/README.md](droplets/README.md).

`n1` bootstraps the Raft group. `n2` and `n3` join it. Writes succeed on the leader. A follower forwards a write, or a whole explicit transaction, to the current leader, then waits until that commit is applied locally before the next read on the same connection. Other connections can still see an older copy. `FLUSH BINARY LOGS` on the leader rolls every node's binlog together. Any node can stream that binlog; the GTID stream is the same after a promotion.

Raft stays on `10.116.0.0/24` and is not published to the host. Each node keeps `/data` in its own volume.

Each example app lives in its own directory and has a compose file that starts that app with this cluster. Run `examples/gencerts.sh` first. These stacks publish 3306–3308, so stop the root stack before starting one. One file starts the cluster and all three apps:

```bash
docker compose -f examples/compose.yaml up --build
```

Or start one language:

```bash
docker compose -f examples/webapp/compose.yaml up --build
docker compose -f examples/rails/compose.yaml up --build
docker compose -f examples/fastapi/compose.yaml up --build
```

| App | URL |
|-----|-----|
| [examples/webapp](examples/webapp) | http://localhost:8080 |
| [examples/rails](examples/rails) | http://localhost:3000 |
| [examples/fastapi](examples/fastapi) | http://localhost:8000/docs |

The page on port 8080 is the cluster view: each node's `SHOW RAFT STATUS`, a choice of which address receives the next write, and whether another node can see that row yet.

Inside those stacks the apps dial `n1`, `n2`, and `n3` on port 3306 and trust `/certs/ca.crt`. From the host, the same clients take every published address. A broken connection tries the next one:

```bash
MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308
```

The example clients trust `examples/certs/ca.crt` unless `MYSQL_TLS_CA` is set. `MYSQL_TLS_CA=off` connects without TLS. Leave `MYSQL_ADDRS` unset to use only `localhost:3306`.

A second stack places the same tables on two Raft groups. `g1`–`g3` hold `accounts.id` below 3. `g4`–`g6` hold ids from 3 up. `g1`–`g3` also vote in the meta group that stores those ranges. MySQL is on host ports 3416–3421. The apps are on 8081, 8001, and 3001, so this stack can run beside the single group. A new account has to include `id`.

```bash
docker compose -f examples/compose.sharded.yaml up --build
examples/placeshards.sh
```

## Stress test

`go/performance/stress/run.sh` starts one target in Docker, loads the same workload, and writes `go/performance/stress/summary.md`. Run it from this repo. Docker has to be running. The first start creates `go/performance/stress/certs` with `openssl`. These ports stay off 3306–3308, so the stress stack can run beside the example cluster.

```bash
go/performance/stress/run.sh single
go/performance/stress/run.sh cluster
go/performance/stress/run.sh mysql
go/performance/stress/run.sh tidb
go/performance/stress/run.sh ranged
go/performance/stress/run.sh compare
go/performance/stress/run.sh failover
```

`failover` is not part of `compare`. It runs the cluster for 60 seconds with writes aimed at a follower, kills the leader, waits for a new leader, and starts the killed node again. The report splits errors during that election from errors after the new leader is serving. A row inserted before the kill must be readable on every node afterward.

`compare` runs single, cluster, MySQL, TiDB, and the ranged cluster one after another so they do not share the CPU. Each run also prints a text report and stores JSON, CPU samples, and disk usage under `go/performance/stress/results/`.

Flags after `--` go to the client:

```bash
go/performance/stress/run.sh cluster -- -duration 60s -concurrency 32 -seed 20000
```

| Flag | Default | Role |
|------|---------|------|
| `-duration` | `20s` | Measured run length. |
| `-concurrency` | `8` | Simultaneous clients. |
| `-seed` | `5000` | Accounts inserted before the run. Each account gets two notes. |
| `-read-pct` | `80` | Percent of operations that are reads. |
| `-warmup` | `2s` | Unmeasured run before the clock starts. |
| `-batch` | `100` | Rows per seed `INSERT`. |

The default workload is 8 clients for 20 seconds, 80% reads, after seeding 5000 accounts and 10000 notes. Cluster writes go to the leader and reads go to the followers. MySQL flushes the redo log and the binlog on commit. The single node fsyncs each commit. The cluster also waits for a Raft quorum. TiDB reads and writes go through one SQL server to three TiKV nodes, and a commit waits for two Raft quorums. The ranged target keeps the catalog on a meta group replicated to every node and places each account, with its notes, by key range. A commit waits for that group's quorum.

| Target | Address |
|--------|---------|
| single | `127.0.0.1:3316` |
| cluster | `127.0.0.1:3326`, `3327`, `3328`. Writes go to the current leader. |
| mysql | `127.0.0.1:3336` |
| tidb | `127.0.0.1:3346` |
| ranged | `127.0.0.1:3376`–`3381`. Two data groups. Writes for an account go to the group that owns its id. |

The account is `root` / `stress`, database `stress`. Every target requires TLS. The script creates the CA and server certificate on first use.

`KEEP=1` leaves the containers up after the run:

```bash
KEEP=1 go/performance/stress/run.sh cluster
```

Wipe stored data with:

```bash
docker compose -f go/performance/stress/compose.yaml -p hardhatdb-stress down -v
```

## Operations

`SIGTERM` and `SIGINT` stop the MySQL listener, wait up to `HARDHATDB_SHUTDOWN_TIMEOUT` (default 15s) for sessions to finish, then shut Raft down and sync Badger.

`SHOW RAFT STATUS` returns one row: `role`, `leader`, `commit_index`, `applied_index`, `lag`, and `suffrage`. A standalone process reports `standalone` and an empty `suffrage`. On a follower the statement runs locally, so `lag` is how far that node is behind the commit index. A read on another connection can still see an older copy. `suffrage` is `voter`, `nonvoter`, or `staging`.

Membership changes are leader statements. A follower forwards them:

```sql
RAFT ADD VOTER 'n4' '10.116.0.5:7001';
RAFT ADD NONVOTER 'n4' '10.116.0.5:7001';
RAFT REMOVE SERVER 'n4';
```

A nonvoter copies the log and serves reads. It does not vote, so quorum stays the voters. A write sent to it is forwarded to the leader. `RAFT REMOVE SERVER` drops a voter or a nonvoter. More write throughput comes from a faster machine for the voters. A table whose writes do not fit that leader is placed with `SHARD TABLE` on a second set of peers. A size-based split stays on the current hosts and does not add a machine.

`HARDHATDB_RAFT_BOOTSTRAP=1` bootstraps only when the Raft directory has no state. A later start with the flag still set logs that bootstrap is ignored, then catches up as a follower instead of waiting to become leader. A wiped volume with the flag left on still creates a second group. Restored data is the exception: an empty Raft directory and a data directory that already has a Raft index start the group from that index.

`BACKUP TO '/var/backups/1'` writes that directory on the leader, or on a standalone process. It holds `state.bin`, `meta`, and, in a cluster, `binlog/`. The directory is refused when it already holds files. With the server stopped, `hardhatdb restore --from /var/backups/1 --data /data/hardhatdb` loads `state.bin` into an empty data directory and refuses one that already exists. Start that process with a fresh Raft directory. On the new leader, `RESTORE BINLOG FROM '/var/backups/1/binlog' AFTER n` applies binlog events whose sequence is above the index in `meta`. Each process backs up the directory it stores.

`SET GLOBAL max_connections` does not resize the listener. The cap is `HARDHATDB_MAX_CONNECTIONS`, read at start. The same is true of the net timeouts.

Set `HARDHATDB_METRICS_ADDR` (for example `127.0.0.1:9090`) to serve `GET /healthz`, `GET /readyz`, and `GET /metrics`. Leave it unset and that port stays closed.

## Environment

| Variable | Role |
|----------|------|
| `HARDHATDB_DATA` | Badger directory. Default `data/hardhatdb`. |
| `HARDHATDB_MYSQL_HOST` | MySQL bind address. Default `localhost`. Use `0.0.0.0` for one published container. The three-node compose file binds each node's private address. |
| `HARDHATDB_MYSQL_PORT` | MySQL port. Default `3306`. |
| `HARDHATDB_RAFT_ADDR` | Turns cluster mode on and sets the Raft bind address, such as `10.116.0.2:7001`. `0.0.0.0` listens on every interface. A non-loopback address requires `HARDHATDB_RAFT_TLS_CERT`, `HARDHATDB_RAFT_TLS_KEY`, and `HARDHATDB_RAFT_TLS_CA`. |
| `HARDHATDB_RAFT_TLS_CERT` | PEM certificate for Raft and the write-forward port. It needs `serverAuth` and `clientAuth`. |
| `HARDHATDB_RAFT_TLS_KEY` | PEM private key for that certificate. |
| `HARDHATDB_RAFT_TLS_CA` | PEM CA that signed the certificate. Peers present the certificate and verify it with this CA. |
| `HARDHATDB_FORWARD_ADDR` | Write-forward listen address. The default is port 7002 on the Raft advertise host. |
| `HARDHATDB_RAFT_ADVERTISE` | Address other nodes dial. Defaults to `HARDHATDB_RAFT_ADDR`. |
| `HARDHATDB_NODE_ID` | Raft server id. Defaults to the bind address. |
| `HARDHATDB_RAFT_PEERS` | `id=host:port` list, comma-separated. |
| `HARDHATDB_RAFT_BOOTSTRAP` | `1` on exactly one node, the first time the group starts. A restart that still has Raft state ignores the flag, logs that, and joins as a follower. |
| `HARDHATDB_RAFT_DIR` | Raft log, snapshots, server UUID, and binlog. Default is beside the data directory. |
| `HARDHATDB_SERVER_UUID` | Shared by every node. It is the GTID server id in the binlog. |
| `HARDHATDB_BINLOG_MAX_SIZE` | Rolls the binlog after a transaction crosses this many bytes. Default is 1 GiB. `FLUSH BINARY LOGS` rolls it immediately. |
| `HARDHATDB_SOURCE_HOST` | Upstream MySQL host. When set, the current primary replicates from it. Set the same value on every node. |
| `HARDHATDB_SOURCE_PORT` | Upstream MySQL port. Default `3306`. |
| `HARDHATDB_SOURCE_USER` | Upstream MySQL user. |
| `HARDHATDB_SOURCE_PASSWORD` | Upstream MySQL password. It stays in a local file beside the Raft directory. A promoted node can dial only if it already has this password. |
| `HARDHATDB_BOOTSTRAP_PASSWORD` | Password for the first account. Required on the first boot of a standalone process or the Raft leader. Ignored once accounts are stored. |
| `HARDHATDB_BOOTSTRAP_USER` | First account name. Default `root`. |
| `HARDHATDB_BOOTSTRAP_HOST` | Host pattern for that account. Default `%`, so published Docker ports and other containers can connect. |
| `HARDHATDB_TLS_CERT` | PEM certificate for the MySQL listener. Set together with `HARDHATDB_TLS_KEY` to require TLS. |
| `HARDHATDB_TLS_KEY` | PEM private key for the MySQL listener. |
| `HARDHATDB_MAX_CONNECTIONS` | Listener connection cap. Default `151`. |
| `HARDHATDB_NET_READ_TIMEOUT` | Connection read timeout. Unset means no socket deadline. A bare number is seconds. |
| `HARDHATDB_NET_WRITE_TIMEOUT` | Connection write timeout. Unset means no socket deadline. A bare number is seconds. |
| `HARDHATDB_MAX_EXECUTION_TIME` | Per-statement deadline in milliseconds. Default `0`, which sets no deadline. |
| `HARDHATDB_SHUTDOWN_TIMEOUT` | How long `SIGTERM` waits for sessions before closing the store. Default `15s`. |
| `HARDHATDB_METRICS_ADDR` | Optional `host:port` for `/healthz`, `/readyz`, and `/metrics`. Unset means those routes are not served. |
| `HARDHATDB_SEED_EXAMPLE` | `1` creates `mydb.accounts` and `mydb.notes`, with the example rows, when `accounts` is missing. Default is off. |
| `HARDHATDB_META_ADDR` | Turns the meta catalog on. Raft address of this node's meta group. The `HARDHATDB_RAFT_*` variables are the data group this process stores. |
| `HARDHATDB_META_PEERS` | Meta voters, `id=host:port`, comma-separated. |
| `HARDHATDB_META_NONVOTERS` | Meta replicas that copy the catalog and do not vote. |
| `HARDHATDB_META_BOOTSTRAP` | `1` on one meta voter, the first time the group starts. |
| `HARDHATDB_META_DATA` | Badger directory for the catalog. Default `data/meta`. |
| `HARDHATDB_META_DIR` | Raft directory for the catalog. Default `data/meta-raft`. |
| `HARDHATDB_META_FORWARD` | Meta write-forward address. Required when the shard forward port would otherwise collide with it. |
| `HARDHATDB_SHARD_FORWARD` | `index=host:port` for one member of each data group. Schema statements are sent to every address. The index is a label. |

`SHARD TABLE db.table BY column [CHECK column] RANGE ...` records a key range in meta. A sharded insert includes the shard column. One statement stays on one range. A read that does not name the shard column is sent to every range that holds the table.

`CHANGE REPLICATION SOURCE TO`, `START REPLICA`, and `STOP REPLICA` configure that upstream job. The primary is the only node that connects. After a failover the new primary continues from the GTID stored with the applied rows. Replication filters are unsupported.
