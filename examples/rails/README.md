# Example Rails app

A browser for `mydb.accounts` and `mydb.notes` on the HardhatDB cluster. The layout shows `SHOW RAFT STATUS` for every address. A browser session stays on one node, so the page after a write sees that write.

`MYSQL_ADDRS` lists every MySQL address. Reads and writes use the session's node. A broken connection moves the session to the next address. Creating an account inserts the account and its first note in one transaction. A write whose connection breaks before a result comes back is sent again. Another connection can still see an older copy.

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

Open http://localhost:3000.

This stack publishes MySQL on 3306, 3307, and 3308, the same ports as the repo-root cluster. Stop the other stack before `up`.

To run the app on the host against a cluster that is already up:

```bash
bundle install
bin/rails server
```

The default CA is `../certs/ca.crt`.

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `127.0.0.1` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |

The tables already exist. This app does not create or migrate them.
