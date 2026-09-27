# Example FastAPI app

A JSON API for `mydb.accounts` and `mydb.notes` on the HardhatDB cluster. Same routes as `../webapp`. `GET /cluster` returns `SHOW RAFT STATUS` for every address.

`MYSQL_ADDRS` lists every MySQL address. One request stays on one node. A broken connection tries the next address. Creating an account inserts the account and its first note in one transaction, then reads that row back on the same connection. Another connection can still see an older copy. Leave `MYSQL_ADDRS` unset to use `MYSQL_HOST`:`MYSQL_PORT`.

## Run

Run `examples/gencerts.sh` from the repo root first. A volume created for the previous example still has `mydb.mytable`. Remove it before the first start on this schema.

From the repo root, this starts the cluster and all three apps:

```bash
docker compose -f examples/compose.yaml up --build
```

From this directory, this starts the cluster and this API:

```bash
docker compose up --build
```

Open http://localhost:8000/docs.

```bash
curl -s localhost:8000/cluster
curl -s localhost:8000/accounts?size=2
curl -s -X POST localhost:8000/accounts \
  -H 'content-type: application/json' \
  -d '{"name":"Lin","email":"lin@example.com","status":1,"tags":["demo"],"note":"hello"}'
```

This stack publishes MySQL on 3306, 3307, and 3308, the same ports as the repo-root cluster. Stop the other stack before `up`.

To run the API on the host against a cluster that is already up:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python main.py
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
| `HTTP_ADDR` | `:8000` |

The tables already exist. This app does not create them.

Handler tests use an in-memory store and do not need MySQL:

```bash
pip install pytest httpx
pytest
```
