# Example Rails app

A small CRUD app for `mydb.mytable` on the HardhatDB cluster.

`MYSQL_ADDRS` lists every MySQL address. Reads and writes use any of them. A browser session stays on one node, so the page after a write sees that write. A broken connection moves the session to the next address. A write whose connection breaks before a result comes back is sent again. Another connection can still see an older copy.

## Run

Create the certificates at the repo root first. See the repo README. Then, from this directory:

```bash
docker compose up --build
```

That starts the three-node cluster and this app. Open http://localhost:3000.

This stack publishes MySQL on 3306, 3307, and 3308, the same ports as the repo-root cluster. Stop the other stack before `up`.

To run the app on the host against a cluster that is already up:

```bash
bundle install
bin/rails server
```

The default CA is `../../certs/ca.crt`.

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `127.0.0.1` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../../certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |

The table already exists. This app does not create or migrate it.
