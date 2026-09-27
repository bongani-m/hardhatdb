# Example web app

A JSON CRUD API for `mydb.mytable` on the HardhatDB cluster. Same routes as `../fastapi`.

`MYSQL_ADDRS` lists every MySQL address. Reads and writes use any of them. A broken connection tries the next address. Another connection can still see an older copy. Leave `MYSQL_ADDRS` unset to use `MYSQL_HOST`:`MYSQL_PORT`.

## Run

Create the certificates at the repo root first. See the repo README. Then, from this directory:

```bash
docker compose up --build
```

That starts the three-node cluster and this API. Open http://localhost:8080.

```bash
curl -s localhost:8080/people?size=2
curl -s 'localhost:8080/people?name=Jane'
curl -s -X POST localhost:8080/people \
  -H 'content-type: application/json' \
  -d '{"name":"Ada Lovelace","email":"ada@example.com","phone_numbers":["111-222-333"]}'
```

This stack publishes MySQL on 3306, 3307, and 3308, the same ports as the repo-root cluster. Stop the other stack before `up`.

To run the API on the host against a cluster that is already up:

```bash
go run .
```

The default CA is `../../certs/ca.crt`.

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../../certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |
| `HTTP_ADDR` | `:8080` |

The table already exists. This app does not create it.

Handler tests use an in-memory store and do not need MySQL:

```bash
go test .
```
